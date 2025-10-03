#!/usr/bin/env python3
"""
依照product_name與元件名稱分類影像檔案
 
將來源資料夾中符合副檔名的影像檔案，
1. 根據檔名開頭14位時間戳，拼出相對路徑: YYYY-MM-DD/timestamp/filename
2. 以timestamp(對應資料庫中image_path)查詢AmrRawData，取得product_name
3. 以正則解析檔名中@後的元件名稱
4. 從product_name中提取公司代號和產品號（忽略迭代號碼）
5. 將影像搬移或複製至 dest_root/[company_code]_[product_code]_[component]_[light] 下
 
使用方式:
    python classify_images.py --config configs.json --project HPH <source_dir> <dest_root> [-c]
 
依賴: sessions.py, amr_info.py 同目錄下
"""
import os
import re
import json
import shutil
import argparse
from tqdm import tqdm
from pathlib import Path
from database.sessions import create_session
from database.amr_info import AmrRawData
 
 
def parse_args():
    parser = argparse.ArgumentParser(
        description='依照product_name與元件名稱分類影像檔案'
    )
    parser.add_argument(
        '--config',
        default='configs.json',
        help='Config 檔案路徑 (預設: configs.json)'
    )
    parser.add_argument(
        '--project',
        default='HPH',
        help='Config 中的專案 key (如 HPH, JQ, ZJ 等)'
    )
    parser.add_argument(
        'source_dir',
        help='來源資料夾路徑'
    )
    parser.add_argument(
        'dest_root',
        help='分類後的根目錄'
    )
    parser.add_argument(
        '-p', '--pattern',
        default=r'@(.+?)_',
        help='解析元件名稱的正則 (預設: @(.+?)_)'
    )
    parser.add_argument(
        '-e', '--extensions',
        nargs='+',
        default=['jpg', 'jpeg', 'png'],
        help='處理的影像副檔名 (預設: jpg jpeg png)'
    )
    parser.add_argument(
        '-c', '--copy',
        action='store_true',
        help='使用複製檔案，否則搬移'
    )
    return parser.parse_args()


def parse_product_name(product_name):
    """
    解析產品名稱，提取公司代號和產品號
    例: 1005-700700-00B -> 公司代號: 1005, 產品號: 700700
    例: 1005-251700-01B -> 公司代號: 1005, 產品號: 251700
    """
    # 使用正則表達式解析 company_code-product_number-iteration
    pattern = r'^(\d+)-(\d+)-\w+$'
    match = re.match(pattern, product_name)
    
    if match:
        company_code = match.group(1)
        product_code = match.group(2)
        return f"{company_code}_{product_code}"
    else:
        print(f"警告: 無法解析產品名稱格式 -> {product_name}")
        # 如果解析失敗，返回原始產品名稱
        return product_name.replace('-', '_')
 
 
def main():
    args = parse_args()
 
    # 讀取 config
    with open(args.config, 'r', encoding='utf-8') as f:
        config = json.load(f)
 
    if args.project not in config:
        print(f"Error: 找不到專案設定 '{args.project}'")
        return
 
    proj_conf = config[args.project]
    session = create_session(proj_conf['SSHTUNNEL'], proj_conf['database'])
 
    # 元件名稱正則
    comp_re = re.compile(args.pattern)
 
    # 遍歷來源資料夾
    for f in tqdm(sorted(Path(args.source_dir).rglob('*.jp*'))):
        fname = f.name
        # 解析時間戳
        m_ts = re.match(r'^(\d{14})', fname)
        if not m_ts:
            print(f"警告: 檔名無法解析時間戳 -> {fname}")
            continue
        ts = m_ts.group(1)
        date_folder = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
        relative_path = f"{date_folder}/{ts}/{fname}"
        
        # 解析元件名稱
        m_comp = comp_re.search(fname)
        if not m_comp:
            print(f"警告: 檔名無法解析元件名稱 -> {fname}")
            continue
        comp = m_comp.group(1)

        # 查 DB 取得 product_name
        rec = session.query(AmrRawData).filter(
            AmrRawData.create_time >= date_folder,
            AmrRawData.line_id == 'V31',
            AmrRawData.image_path == relative_path
        ).first()
        if not rec or not rec.product_name:
            print(f"警告: 找不到 product_name (image_path={ts}) -> {relative_path}")
            continue
        
        # 解析產品名稱，提取公司代號和產品號
        product_code = rec.product_name
        
        # 提取光源資訊
        light = f.stem.rsplit('_', 1)[-1]

        # 構造分類資料夾：company_product_component_light
        dest_dir = Path(args.dest_root) / f"{product_code}_{comp}_{light}"
        dest_dir.mkdir(parents=True, exist_ok=True)

        # 移動或複製
        src_path = f.parent / f.name
        dst_path = dest_dir / f.name
        if args.copy:
            shutil.copy2(src_path, dst_path)
            action = '複製'
        else:
            shutil.move(src_path, dst_path)
            action = '搬移'
        print(f"{action}: {src_path} → {dst_path}")
 
    print('✅ 分類完成')


if __name__ == '__main__':
    main()