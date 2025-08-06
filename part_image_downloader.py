#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
優化的 Part Number 影像下載器
基於現有的 download.py 進行模組化重構和功能增強
"""

import asyncio
import aiofiles
import argparse
import json
import logging
import os
import sys
import pandas as pd
import zipfile
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

import requests
import aiohttp
from tqdm.asyncio import tqdm
from sshtunnel import SSHTunnelForwarder

# 設置編碼環境
if sys.platform.startswith('win'):
    import locale
    import codecs
    # 設置標準輸出編碼為 UTF-8
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# 假設現有的資料庫模型和會話管理
from database.amr_info import AmrRawData
from database.sessions import create_session


@dataclass
class DownloadConfig:
    """下載設定資料類"""
    site: str
    start_date: str
    end_date: str
    images_per_part: int = 20
    download_dir: str = "downloads"
    max_concurrent: int = 5
    extract_zip: bool = True  # 是否解壓縮 ZIP 檔案
    keep_zip: bool = False    # 是否保留原始 ZIP 檔案
    top_n: Optional[int] = None  # 下載前 N 個 part number
    interactive: bool = False    # 是否啟用互動式選擇


class Logger:
    """優化的日誌管理器"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._setup_logger()
        return cls._instance
    
    @classmethod
    def _setup_logger(cls):
        """設置日誌器"""
        cls.logger = logging.getLogger('part_downloader')
        cls.logger.setLevel(logging.INFO)
        
        if not cls.logger.handlers:
            # 檔案處理器 - 明確指定 UTF-8 編碼
            fh = logging.FileHandler(
                f'downloads_{datetime.now().strftime("%Y%m%d")}.log',
                encoding='utf-8'
            )
            fh.setLevel(logging.INFO)
            
            # 控制台處理器 - 處理編碼問題
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.INFO)
            
            # 格式化器
            formatter = logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            
            cls.logger.addHandler(fh)
            cls.logger.addHandler(ch)
    
    @classmethod
    def info(cls, message: str):
        cls().logger.info(message)
    
    @classmethod
    def error(cls, message: str):
        cls().logger.error(message, exc_info=True)
    
    @classmethod
    def warning(cls, message: str):
        cls().logger.warning(message)


class ConfigManager:
    """設定檔管理器"""
    
    @staticmethod
    def load_config(config_path: str = 'configs.json') -> dict:
        """載入設定檔"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            Logger.error(f"Config file {config_path} not found")
            raise
        except json.JSONDecodeError:
            Logger.error(f"Config file {config_path} format error")
            raise


class PartNumberAnalyzer:
    """Part Number 分析器"""
    
    def __init__(self, config: DownloadConfig):
        self.config = config
        self.app_config = ConfigManager.load_config()
    
    def get_part_statistics(self, filters: dict) -> pd.DataFrame:
        """取得 Part Number 統計資料，按數量降序排列"""
        Logger.info(f"Starting Part Number analysis for site: {self.config.site}")
        
        ssh_tunnel = self.app_config[self.config.site]['SSHTUNNEL']
        database = self.app_config[self.config.site]['database']
        
        try:
            with create_session(ssh_tunnel, database) as session:
                # 建立基礎查詢
                query = session.query(
                    AmrRawData.part_number,
                    AmrRawData.image_path,
                    AmrRawData.line_id,
                    AmrRawData.create_time
                ).filter(
                    AmrRawData.create_time.between(
                        self.config.start_date, 
                        self.config.end_date
                    )
                ).filter(
                    AmrRawData.part_number.isnot(None)
                )
                
                # 應用額外篩選條件
                for key, value in filters.items():
                    if value and hasattr(AmrRawData, key):
                        query = query.filter(getattr(AmrRawData, key) == value)
                
                # 執行查詢
                results = query.all()
                
                if not results:
                    Logger.warning("No data found matching the criteria")
                    return pd.DataFrame()
                
                # 轉換為 DataFrame 並進行統計
                df = pd.DataFrame([
                    {
                        'part_number': r.part_number,
                        'image_path': r.image_path,
                        'line_id': r.line_id,
                        'create_time': r.create_time
                    }
                    for r in results
                ])
                
                # 按 part_number 分組統計
                part_stats = df.groupby('part_number').agg({
                    'image_path': 'count',
                    'line_id': 'first'
                }).rename(columns={'image_path': 'image_count'}).reset_index()
                
                # 按數量降序排列
                part_stats = part_stats.sort_values('image_count', ascending=False)
                
                Logger.info(f"Found {len(part_stats)} Part Numbers, total images: {df.shape[0]}")
                
                # 合併回原始資料以便後續下載
                result_df = df.merge(part_stats[['part_number']], on='part_number')
                
                return result_df
                
        except Exception as e:
            Logger.error(f"Error querying Part Number statistics: {e}")
            raise


class AsyncImageDownloader:
    """非同步影像下載器"""
    
    def __init__(self, config: DownloadConfig):
        self.config = config
        self.app_config = ConfigManager.load_config()
        self.download_dir = Path(config.download_dir)
        self.download_dir.mkdir(exist_ok=True)
    
    def _get_download_url(self, line_id: str) -> str:
        """取得下載URL"""
        ssh_tunnel = self.app_config[self.config.site]['SSHTUNNEL']
        image_pools = self.app_config[self.config.site]['image_pool']
        
        if (ssh_tunnel['ssh_address_or_host'] and 
            ssh_tunnel['ssh_username'] and 
            ssh_tunnel['ssh_password']):
            
            # 使用 SSH 隧道
            server = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_tunnel['ssh_address_or_host'], 22),
                ssh_username=ssh_tunnel['ssh_username'],
                ssh_password=ssh_tunnel['ssh_password'],
                remote_bind_address=(
                    image_pools[line_id]['ip'], 
                    image_pools[line_id]['port']
                )
            )
            server.start()
            local_host = '127.0.0.1'
            local_port = server.local_bind_port
        else:
            local_host = image_pools[line_id]['ip']
            local_port = image_pools[line_id]['port']
        
        download_url_prefix = image_pools[line_id]['donwload_url_prefix']
        return f'http://{local_host}:{local_port}/{download_url_prefix}'
    
    async def download_part_images(
        self, 
        part_number: str, 
        image_data: pd.DataFrame
    ) -> bool:
        """下載指定 Part Number 的影像"""
        
        # 建立 Part Number 專用資料夾
        part_dir = self.download_dir / part_number
        part_dir.mkdir(exist_ok=True)
        
        # 限制下載數量
        limited_data = image_data.head(self.config.images_per_part)
        
        Logger.info(f"Starting download for {part_number}: {len(limited_data)} images")
        
        # 按 line_id 分組下載
        for line_id, group in limited_data.groupby('line_id'):
            try:
                success = await self._download_group_images(
                    part_number, line_id, group, part_dir
                )
                if not success:
                    Logger.error(f"Failed to download images for {part_number} on line {line_id}")
                    return False
            except Exception as e:
                Logger.error(f"Error downloading {part_number}: {e}")
                return False
        
        Logger.info(f"Completed download for {part_number}")
        return True
    
    def _extract_zip_file(self, zip_path: Path, extract_to: Path) -> bool:
        """解壓縮 ZIP 檔案到指定目錄"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # 檢查 ZIP 檔案是否有效
                zip_ref.testzip()
                
                # 解壓縮到指定目錄
                zip_ref.extractall(extract_to)
                
                # 獲取解壓縮的檔案列表
                extracted_files = zip_ref.namelist()
                Logger.info(f"Extracted {len(extracted_files)} files from {zip_path.name}")
                
                return True
                
        except zipfile.BadZipFile:
            Logger.error(f"Invalid ZIP file: {zip_path}")
            return False
        except Exception as e:
            Logger.error(f"Error extracting ZIP file {zip_path}: {e}")
            return False

    async def _download_group_images(
        self, 
        part_number: str, 
        line_id: str, 
        group_data: pd.DataFrame,
        part_dir: Path
    ) -> bool:
        """下載同一 line_id 的影像群組"""
        
        try:
            url = self._get_download_url(line_id)
            
            # 準備影像路徑列表
            if self.config.site == 'NK':
                image_list = [
                    f'images/{self.app_config[self.config.site]["image_pool"][line_id]["line_ip"]}/{path}'
                    for path in group_data['image_path']
                ]
            elif self.config.site == 'ZJ':
                image_list = [
                    f'{line_id}/{path}' 
                    for path in group_data['image_path']
                ]
            else:
                image_list = group_data['image_path'].tolist()
            
            # 發送下載請求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, 
                    json={"paths": image_list},
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    
                    if response.status == 200:
                        zip_filename = part_dir / f"{part_number}_{line_id}.zip"
                        
                        # 非同步寫入 ZIP 檔案
                        async with aiofiles.open(zip_filename, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                await f.write(chunk)
                        
                        Logger.info(f"Downloaded ZIP: {zip_filename}")
                        
                        # 如果設定為解壓縮
                        if self.config.extract_zip:
                            # 建立解壓縮目錄（以 line_id 命名）
                            extract_dir = part_dir / f"line_{line_id}"
                            extract_dir.mkdir(exist_ok=True)
                            
                            # 解壓縮檔案
                            if self._extract_zip_file(zip_filename, extract_dir):
                                Logger.info(f"Extracted to: {extract_dir}")
                                
                                # 如果不保留原始 ZIP 檔案，則刪除它
                                if not self.config.keep_zip:
                                    zip_filename.unlink()
                                    Logger.info(f"Removed ZIP file: {zip_filename}")
                            else:
                                Logger.error(f"Failed to extract ZIP file: {zip_filename}")
                                return False
                        
                        return True
                    else:
                        Logger.error(f"Download failed, HTTP status: {response.status}")
                        return False
        
        except Exception as e:
            Logger.error(f"Error downloading group images: {e}")
            return False
    
    async def batch_download(self, part_data: pd.DataFrame) -> None:
        """批次下載所有 Part Number 的影像"""
        
        # 按 part_number 分組
        part_groups = part_data.groupby('part_number')
        
        Logger.info(f"Starting batch download for {len(part_groups)} Part Numbers")
        
        # 控制並行下載數量
        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        async def download_with_semaphore(part_number, group_data):
            async with semaphore:
                return await self.download_part_images(part_number, group_data)
        
        # 建立下載任務
        tasks = [
            download_with_semaphore(part_number, group_data)
            for part_number, group_data in part_groups
        ]
        
        # 執行並行下載
        results = await tqdm.gather(*tasks, desc="Download Progress")
        
        # 統計結果
        success_count = sum(results)
        Logger.info(f"Download completed: {success_count}/{len(results)} Part Numbers successful")


class PartImageDownloadManager:
    """主要管理器"""
    
    def __init__(self, config: DownloadConfig):
        self.config = config
        self.analyzer = PartNumberAnalyzer(config)
        self.downloader = AsyncImageDownloader(config)
    
    def _display_part_statistics(self, part_stats: pd.Series) -> None:
        """顯示 Part Number 統計資訊"""
        Logger.info("=" * 60)
        Logger.info("PART NUMBER STATISTICS (Sorted by image count DESC)")
        Logger.info("=" * 60)
        Logger.info(f"{'Rank':<6} {'Part Number':<20} {'Image Count':<12} {'Percentage':<10}")
        Logger.info("-" * 60)
        
        total_images = part_stats.sum()
        for i, (part_num, count) in enumerate(part_stats.items(), 1):
            percentage = (count / total_images) * 100
            Logger.info(f"{i:<6} {part_num:<20} {count:<12} {percentage:.1f}%")
        
        Logger.info("-" * 60)
        Logger.info(f"Total: {len(part_stats)} Part Numbers, {total_images} images")
        Logger.info("=" * 60)
    
    def _get_user_selection(self, part_stats: pd.Series) -> List[str]:
        """取得用戶選擇的 Part Number"""
        if self.config.top_n:
            # 自動選擇前 N 個
            selected_parts = part_stats.head(self.config.top_n).index.tolist()
            Logger.info(f"Auto-selected top {self.config.top_n} Part Numbers:")
            for i, part in enumerate(selected_parts, 1):
                Logger.info(f"  {i}. {part} ({part_stats[part]} images)")
            return selected_parts
        
        elif self.config.interactive:
            # 互動式選擇
            return self._interactive_selection(part_stats)
        
        else:
            # 預設行為：下載全部
            Logger.info("Downloading ALL Part Numbers (use --top-n or --interactive for selection)")
            return part_stats.index.tolist()
    
    def _interactive_selection(self, part_stats: pd.Series) -> List[str]:
        """互動式選擇 Part Number"""
        while True:
            try:
                print("\n" + "=" * 50)
                print("INTERACTIVE SELECTION")
                print("=" * 50)
                print("Options:")
                print("1. Download top N part numbers")
                print("2. Download specific part numbers by rank")
                print("3. Download specific part numbers by name")
                print("4. Download all part numbers")
                print("5. Exit without downloading")
                
                choice = input("\nPlease select an option (1-5): ").strip()
                
                if choice == '1':
                    return self._select_top_n(part_stats)
                elif choice == '2':
                    return self._select_by_rank(part_stats)
                elif choice == '3':
                    return self._select_by_name(part_stats)
                elif choice == '4':
                    return part_stats.index.tolist()
                elif choice == '5':
                    Logger.info("User cancelled download")
                    return []
                else:
                    print("Invalid option. Please try again.")
                    
            except KeyboardInterrupt:
                Logger.info("User cancelled selection")
                return []
            except Exception as e:
                Logger.error(f"Error in interactive selection: {e}")
                return []
    
    def _select_top_n(self, part_stats: pd.Series) -> List[str]:
        """選擇前 N 個 Part Number"""
        while True:
            try:
                n = input(f"\nEnter number of top part numbers to download (1-{len(part_stats)}): ").strip()
                n = int(n)
                
                if 1 <= n <= len(part_stats):
                    selected = part_stats.head(n).index.tolist()
                    print(f"\nSelected top {n} part numbers:")
                    for i, part in enumerate(selected, 1):
                        print(f"  {i}. {part} ({part_stats[part]} images)")
                    
                    confirm = input("\nConfirm selection? (y/n): ").strip().lower()
                    if confirm == 'y':
                        return selected
                    else:
                        continue
                else:
                    print(f"Please enter a number between 1 and {len(part_stats)}")
                    
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                return []
    
    def _select_by_rank(self, part_stats: pd.Series) -> List[str]:
        """按排名選擇 Part Number"""
        while True:
            try:
                ranks = input("\nEnter ranks separated by commas (e.g., 1,3,5-10): ").strip()
                selected_ranks = self._parse_rank_input(ranks, len(part_stats))
                
                if selected_ranks:
                    selected_parts = [part_stats.index[i-1] for i in selected_ranks]
                    print(f"\nSelected {len(selected_parts)} part numbers:")
                    for rank in sorted(selected_ranks):
                        part = part_stats.index[rank-1]
                        print(f"  {rank}. {part} ({part_stats[part]} images)")
                    
                    confirm = input("\nConfirm selection? (y/n): ").strip().lower()
                    if confirm == 'y':
                        return selected_parts
                    else:
                        continue
                else:
                    print("No valid ranks selected")
                    
            except KeyboardInterrupt:
                return []
            except Exception as e:
                print(f"Error parsing ranks: {e}")
    
    def _select_by_name(self, part_stats: pd.Series) -> List[str]:
        """按名稱選擇 Part Number"""
        while True:
            try:
                names = input("\nEnter part numbers separated by commas: ").strip()
                part_names = [name.strip() for name in names.split(',') if name.strip()]
                
                valid_parts = []
                invalid_parts = []
                
                for part in part_names:
                    if part in part_stats.index:
                        valid_parts.append(part)
                    else:
                        invalid_parts.append(part)
                
                if invalid_parts:
                    print(f"Invalid part numbers: {', '.join(invalid_parts)}")
                
                if valid_parts:
                    print(f"\nSelected {len(valid_parts)} part numbers:")
                    for part in valid_parts:
                        rank = part_stats.index.tolist().index(part) + 1
                        print(f"  {rank}. {part} ({part_stats[part]} images)")
                    
                    confirm = input("\nConfirm selection? (y/n): ").strip().lower()
                    if confirm == 'y':
                        return valid_parts
                    else:
                        continue
                else:
                    print("No valid part numbers selected")
                    
            except KeyboardInterrupt:
                return []
    
    def _parse_rank_input(self, ranks_str: str, max_rank: int) -> List[int]:
        """解析排名輸入字串"""
        ranks = set()
        
        for part in ranks_str.split(','):
            part = part.strip()
            if '-' in part:
                # 處理範圍，如 "5-10"
                start, end = part.split('-', 1)
                start, end = int(start.strip()), int(end.strip())
                if 1 <= start <= max_rank and 1 <= end <= max_rank:
                    ranks.update(range(start, end + 1))
            else:
                # 處理單個數字
                rank = int(part)
                if 1 <= rank <= max_rank:
                    ranks.add(rank)
        
        return sorted(list(ranks))

    async def run(self, filters: dict = None) -> None:
        """執行完整的下載流程"""
        try:
            Logger.info("=== Part Number Image Downloader Started ===")
            Logger.info(f"Site: {self.config.site}")
            Logger.info(f"Date Range: {self.config.start_date} ~ {self.config.end_date}")
            Logger.info(f"Images per Part Number: {self.config.images_per_part}")
            
            # 1. 分析 Part Number 統計
            Logger.info("Step 1: Analyzing Part Number statistics")
            part_data = self.analyzer.get_part_statistics(filters or {})
            
            if part_data.empty:
                Logger.warning("No data found matching the criteria")
                return
            
            # 計算統計資訊並顯示
            part_stats = part_data.groupby('part_number').size().sort_values(ascending=False)
            self._display_part_statistics(part_stats)
            
            # 2. 取得用戶選擇
            Logger.info("Step 2: Getting user selection")
            selected_parts = self._get_user_selection(part_stats)
            
            if not selected_parts:
                Logger.info("No part numbers selected for download")
                return
            
            # 3. 篩選要下載的資料
            selected_data = part_data[part_data['part_number'].isin(selected_parts)]
            Logger.info(f"Selected {len(selected_parts)} Part Numbers for download")
            Logger.info(f"Total images to download: {len(selected_data)}")
            
            # 4. 執行批次下載
            Logger.info("Step 3: Starting batch download")
            await self.downloader.batch_download(selected_data)
            
            Logger.info("=== Download Process Completed ===")
            
        except Exception as e:
            Logger.error(f"Error in download process: {e}")
            raise


def parse_arguments():
    """解析命令列參數"""
    parser = argparse.ArgumentParser(description='Part Number 影像批次下載器')
    
    # 必要參數
    parser.add_argument('--site', required=True, 
                       help='站點名稱 (例如：JQ, ZJ, NK, HZ)')
    parser.add_argument('--start-date', required=True,
                       help='開始日期 (格式：YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True,
                       help='結束日期 (格式：YYYY-MM-DD)')
    
    # 可選參數
    parser.add_argument('--images-per-part', type=int, default=20,
                       help='每個 Part Number 下載的影像數量 (預設：20)')
    parser.add_argument('--download-dir', default='downloads',
                       help='下載目錄 (預設：downloads)')
    parser.add_argument('--max-concurrent', type=int, default=5,
                       help='最大並行下載數 (預設：5)')
    
    # Part Number 選擇選項
    selection_group = parser.add_mutually_exclusive_group()
    selection_group.add_argument('--top-n', type=int, metavar='N',
                               help='自動下載前 N 個 Part Number（按影像數量排序）')
    selection_group.add_argument('--interactive', action='store_true',
                               help='啟用互動式選擇模式')
    
    # ZIP 檔案處理選項
    parser.add_argument('--no-extract', action='store_true',
                       help='不解壓縮 ZIP 檔案（預設會自動解壓縮）')
    parser.add_argument('--keep-zip', action='store_true',
                       help='保留原始 ZIP 檔案（預設解壓縮後會刪除）')
    
    # 篩選條件
    parser.add_argument('--product-name', help='產品名稱')
    parser.add_argument('--line-id', help='產線ID')
    parser.add_argument('--station-id', help='站點ID')
    parser.add_argument('--factory', help='工廠名稱')
    parser.add_argument('--aoi-id', help='AOI ID')
    parser.add_argument('--group-type', help='群組類型')
    
    return parser.parse_args()


async def main():
    """主函數"""
    try:
        # 解析參數
        args = parse_arguments()
        
        # 建立設定
        config = DownloadConfig(
            site=args.site,
            start_date=args.start_date,
            end_date=args.end_date,
            images_per_part=args.images_per_part,
            download_dir=args.download_dir,
            max_concurrent=args.max_concurrent,
            extract_zip=not args.no_extract,  # 預設解壓縮，除非指定 --no-extract
            keep_zip=args.keep_zip,  # 預設不保留 ZIP，除非指定 --keep-zip
            top_n=args.top_n,  # 前 N 個 Part Number
            interactive=args.interactive  # 互動式選擇
        )
        
        # 準備篩選條件
        filters = {
            key: value for key, value in vars(args).items()
            if value is not None and key not in [
                'site', 'start_date', 'end_date', 'images_per_part', 
                'download_dir', 'max_concurrent', 'no_extract', 'keep_zip',
                'top_n', 'interactive'
            ]
        }
        
        # 顯示設定資訊
        Logger.info(f"Extract ZIP files: {config.extract_zip}")
        Logger.info(f"Keep original ZIP files: {config.keep_zip}")
        
        if config.top_n:
            Logger.info(f"Auto-select mode: Top {config.top_n} Part Numbers")
        elif config.interactive:
            Logger.info("Interactive selection mode enabled")
        else:
            Logger.info("Download all Part Numbers (default mode)")
        
        # 執行下載
        manager = PartImageDownloadManager(config)
        await manager.run(filters)
        
    except KeyboardInterrupt:
        Logger.info("User interrupted execution")
    except Exception as e:
        Logger.error(f"Program execution failed: {e}")
        raise


if __name__ == '__main__':
    # 執行非同步主函數
    asyncio.run(main())
