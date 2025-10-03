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
                    if hasattr(AmrRawData, key):
                        # 特別處理布林值條件
                        if key == 'is_covered':
                            if value is True:
                                query = query.filter(getattr(AmrRawData, key) == True)
                                Logger.info(f"Applied filter: {key} = True")
                            elif value is False:
                                query = query.filter(getattr(AmrRawData, key) == False)
                                Logger.info(f"Applied filter: {key} = False")
                        # 處理其他條件
                        elif value is not None:
                            query = query.filter(getattr(AmrRawData, key) == value)
                            Logger.info(f"Applied filter: {key} = {value}")
                
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
    
    def _display_part_statistics(self, part_stats: pd.Series, filters: dict = None) -> None:
        """顯示 Part Number 統計資訊"""
        Logger.info("=" * 70)
        Logger.info("PART NUMBER STATISTICS (Sorted by image count DESC)")
        
        # 顯示應用的篩選條件
        if filters:
            Logger.info("Applied Filters:")
            for key, value in filters.items():
                if isinstance(value, bool):
                    status = "Yes" if value else "No"
                    Logger.info(f"  - {key}: {status}")
                elif value is None:
                    Logger.info(f"  - {key}: NULL")
                else:
                    Logger.info(f"  - {key}: {value}")
            Logger.info("-" * 70)
        
        Logger.info("=" * 70)
        Logger.info(f"{'Rank':<6} {'Part Number':<25} {'Image Count':<12} {'Percentage':<10}")
        Logger.info("-" * 70)
        
        total_images = part_stats.sum()
        for i, (part_num, count) in enumerate(part_stats.items(), 1):
            percentage = (count / total_images) * 100
            Logger.info(f"{i:<6} {part_num:<25} {count:<12} {percentage:.1f}%")
        
        Logger.info("-" * 70)
        Logger.info(f"Total: {len(part_stats)} Part Numbers, {total_images} images")
        Logger.info("=" * 70)
    
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
            self._display_part_statistics(part_stats, filters)
            
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
    parser = argparse.ArgumentParser(
        description='Part Number 影像批次下載器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
篩選條件範例:
  --filter aoi_defect=NG                     # 篩選特定 AOI 缺陷
  --filter is_covered=true                   # 篩選被覆蓋的元件
  --filter station_id=ST01                   # 篩選特定站點
  --filter product_name=ProductA             # 篩選特定產品
  --filter line_id=L001 --filter factory=F1 # 多個篩選條件

支援的值類型:
  字串: product_name=ProductA
  數字: station_id=123
  布林: is_covered=true/false
  空值: comp_name=null

查看可用欄位:
  --list-fields                              # 顯示所有可用的資料庫欄位
        """
    )
    
    # 便利功能
    parser.add_argument('--list-fields', action='store_true',
                       help='顯示所有可用的資料庫欄位並退出')
    
    # 必要參數
    parser.add_argument('--site', 
                       help='站點名稱 (例如：JQ, ZJ, NK, HZ)')
    parser.add_argument('--start-date',
                       help='開始日期 (格式：YYYY-MM-DD)')
    parser.add_argument('--end-date',
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
    
    # 通用篩選條件參數
    parser.add_argument('--filter', action='append', metavar='KEY=VALUE',
                       help='資料庫篩選條件 (格式: 欄位名=值)，可重複使用')
    
    # 常用的快捷篩選選項（向後相容）
    parser.add_argument('--is-covered', action='store_true',
                       help='快捷選項：只下載被覆蓋的影像 (等同於 --filter is_covered=true)')
    parser.add_argument('--not-covered', action='store_true',
                       help='快捷選項：只下載未被覆蓋的影像 (等同於 --filter is_covered=false)')
    
    return parser.parse_args()


def show_available_fields():
    """顯示所有可用的資料庫欄位"""
    try:
        # 獲取 AmrRawData 模型的所有欄位
        columns = AmrRawData.__table__.columns.keys()
        
        print("=" * 60)
        print("AVAILABLE DATABASE FIELDS FOR FILTERING")
        print("=" * 60)
        print(f"Total: {len(columns)} fields available")
        print("-" * 60)
        
        # 按字母順序排序並分欄顯示
        sorted_columns = sorted(columns)
        for i, column in enumerate(sorted_columns, 1):
            print(f"{i:2d}. {column}")
        
        print("-" * 60)
        print("Usage examples:")
        print("  --filter product_name=ProductA")
        print("  --filter is_covered=true")
        print("  --filter station_id=123")
        print("  --filter aoi_defect=NG")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error retrieving database fields: {e}")
        print("Make sure the database models are properly imported.")


def parse_filter_value(key: str, value: str):
    """解析篩選條件的值，自動轉換型別"""
    # 處理布林值
    if value.lower() in ['true', 'false']:
        return value.lower() == 'true'
    
    # 處理數值
    if value.isdigit():
        return int(value)
    
    # 嘗試轉換為浮點數
    try:
        if '.' in value:
            return float(value)
    except ValueError:
        pass
    
    # 處理 None 值
    if value.lower() in ['none', 'null']:
        return None
    
    # 預設為字串
    return value


def validate_filter_key(key: str) -> bool:
    """驗證篩選條件的欄位是否存在於資料庫模型中"""
    return hasattr(AmrRawData, key)


def parse_filters(args) -> dict:
    """解析篩選條件參數"""
    filters = {}
    
    # 處理通用 --filter 參數
    if args.filter:
        for filter_str in args.filter:
            try:
                if '=' not in filter_str:
                    Logger.warning(f"Invalid filter format: {filter_str} (expected KEY=VALUE)")
                    continue
                
                key, value = filter_str.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                if not key:
                    Logger.warning(f"Empty filter key in: {filter_str}")
                    continue
                
                # 驗證欄位是否存在
                if not validate_filter_key(key):
                    Logger.warning(f"Unknown filter field: {key} (skipped)")
                    continue
                
                # 解析值
                parsed_value = parse_filter_value(key, value)
                filters[key] = parsed_value
                
                Logger.info(f"Parsed filter: {key} = {parsed_value} (type: {type(parsed_value).__name__})")
                
            except ValueError as e:
                Logger.warning(f"Error parsing filter '{filter_str}': {e}")
                continue
    
    # 處理快捷選項（向後相容）
    if args.is_covered and args.not_covered:
        Logger.error("Cannot use both --is-covered and --not-covered")
        sys.exit(1)
    elif args.is_covered:
        filters['is_covered'] = True
        Logger.info("Applied shortcut: is_covered = True")
    elif args.not_covered:
        filters['is_covered'] = False
        Logger.info("Applied shortcut: is_covered = False")
    
    return filters


async def main():
    """主函數"""
    try:
        # 解析參數
        args = parse_arguments()
        
        # 如果要求顯示欄位列表，直接顯示並退出
        if args.list_fields:
            show_available_fields()
            return
        
        # 檢查必要參數
        if not args.site:
            Logger.error("Missing required argument: --site")
            print("Use --help for usage information")
            sys.exit(1)
        if not args.start_date:
            Logger.error("Missing required argument: --start-date")
            print("Use --help for usage information")
            sys.exit(1)
        if not args.end_date:
            Logger.error("Missing required argument: --end-date")
            print("Use --help for usage information")
            sys.exit(1)
        
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
        
        # 解析篩選條件
        filters = parse_filters(args)
        
        # 顯示設定資訊
        Logger.info(f"Extract ZIP files: {config.extract_zip}")
        Logger.info(f"Keep original ZIP files: {config.keep_zip}")
        
        if config.top_n:
            Logger.info(f"Auto-select mode: Top {config.top_n} Part Numbers")
        elif config.interactive:
            Logger.info("Interactive selection mode enabled")
        else:
            Logger.info("Download all Part Numbers (default mode)")
        
        # 顯示篩選條件摘要
        if filters:
            Logger.info("Applied filters:")
            for key, value in filters.items():
                Logger.info(f"  {key}: {value}")
        else:
            Logger.info("No filters applied")
        
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
