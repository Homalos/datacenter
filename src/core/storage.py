#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos_data_center
@FileName   : storage.py
@Date       : 2025/10/14 15:40
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据存储与加载封装（支持多合约、日期文件夹）
"""
import os
import duckdb
import pandas as pd
from config import settings
from pathlib import Path
from datetime import datetime
from typing import Optional

os.makedirs(settings.TICK_PATH, exist_ok=True)
os.makedirs(settings.KLINE_PATH, exist_ok=True)


class DataStorage:
    """
    数据存储类 - 支持多合约、按日期文件夹存储
    
    文件结构：
    - data/ticks/{date}/{symbol}.parquet
    - data/klines/{date}/{symbol}.parquet
    """

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.conn = duckdb.connect(database=':memory:')

    def _get_file_path(self, symbol: str, date: Optional[str] = None) -> Path:
        """
        获取文件路径
        
        Args:
            symbol: 合约代码
            date: 日期字符串，格式YYYYMMDD，如果为None则使用今天
        
        Returns:
            完整的文件路径
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        
        # data/ticks/{date}/{symbol}.parquet
        date_dir = Path(self.base_path) / date
        date_dir.mkdir(parents=True, exist_ok=True)
        return date_dir / f"{symbol}.parquet"

    def save_ticks(self, symbol: str, df: pd.DataFrame, date: Optional[str] = None):
        """
        保存 Tick 数据（按日期文件夹存储）
        
        Args:
            symbol: 合约代码
            df: 数据DataFrame
            date: 日期（YYYYMMDD），默认今天
        """
        file_path = self._get_file_path(symbol, date)
        
        # 检查文件是否存在且不为空
        if file_path.exists() and file_path.stat().st_size > 0:
            try:
                # 读取已有数据并合并
                existing = pd.read_parquet(file_path)
                df = pd.concat([existing, df], ignore_index=True)
                # 去重（如果datetime相同）
                if 'datetime' in df.columns:
                    df = df.drop_duplicates(subset=['datetime'], keep='last')
                    df = df.sort_values('datetime').reset_index(drop=True)
            except Exception as e:
                print(f"[Storage] 读取已有tick数据失败: {e}，将覆盖写入")
        
        df.to_parquet(file_path, engine="pyarrow", index=False)

    def save_kline(self, symbol: str, df: pd.DataFrame, date: Optional[str] = None):
        """
        保存 K线 数据（按日期文件夹存储）
        
        Args:
            symbol: 合约代码
            df: 数据DataFrame
            date: 日期（YYYYMMDD），默认今天
        """
        file_path = self._get_file_path(symbol, date)
        
        # 检查文件是否存在且不为空
        if file_path.exists() and file_path.stat().st_size > 0:
            try:
                # 读取已有数据并合并
                existing = pd.read_parquet(file_path)
                df = pd.concat([existing, df], ignore_index=True)
                # 去重（按datetime保留最新）
                if 'datetime' in df.columns:
                    df = df.drop_duplicates(subset=['datetime'], keep='last')
                    df = df.sort_values('datetime').reset_index(drop=True)
            except Exception as e:
                print(f"[Storage] 读取已有kline数据失败: {e}，将覆盖写入")
        
        df.to_parquet(file_path, engine="pyarrow", index=False)

    def load_ticks(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        加载 Tick 数据（从日期文件夹）
        
        Args:
            symbol: 合约代码
            start_date: 开始日期（YYYYMMDD），默认今天
            end_date: 结束日期（YYYYMMDD），默认今天
        
        Returns:
            合并后的DataFrame
        """
        base_path = Path(settings.TICK_PATH)
        
        if not base_path.exists():
            return pd.DataFrame()
        
        # 如果未指定日期，使用今天
        if start_date is None and end_date is None:
            start_date = end_date = datetime.now().strftime("%Y%m%d")
        elif start_date is None:
            start_date = end_date
        elif end_date is None:
            end_date = start_date
        
        # 收集所有日期文件夹中的该symbol数据
        dfs = []
        for date_folder in sorted(base_path.iterdir()):
            if not date_folder.is_dir():
                continue
            
            date_str = date_folder.name
            if start_date <= date_str <= end_date:
                # data/ticks/{date}/{symbol}.parquet
                symbol_file = date_folder / f"{symbol}.parquet"
                if symbol_file.exists() and symbol_file.stat().st_size > 0:
                    try:
                        df = pd.read_parquet(symbol_file)
                        dfs.append(df)
                    except Exception as e:
                        print(f"[Storage] 读取{symbol_file}失败: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        # 合并所有数据
        combined = pd.concat(dfs, ignore_index=True)
        combined['datetime'] = pd.to_datetime(combined['datetime'])
        combined = combined.sort_values('datetime').reset_index(drop=True)
        
        return combined

    def load_kline(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        加载 K线 数据（从日期文件夹）
        
        Args:
            symbol: 合约代码
            start_date: 开始日期（YYYYMMDD），默认今天
            end_date: 结束日期（YYYYMMDD），默认今天
        
        Returns:
            合并后的DataFrame
        """
        base_path = Path(settings.KLINE_PATH)
        
        if not base_path.exists():
            return pd.DataFrame()
        
        # 如果未指定日期，使用今天
        if start_date is None and end_date is None:
            start_date = end_date = datetime.now().strftime("%Y%m%d")
        elif start_date is None:
            start_date = end_date
        elif end_date is None:
            end_date = start_date
        
        # 收集所有日期文件夹中的该symbol数据
        dfs = []
        for date_folder in sorted(base_path.iterdir()):
            if not date_folder.is_dir():
                continue
            
            date_str = date_folder.name
            if start_date <= date_str <= end_date:
                # data/klines/{date}/{symbol}.parquet
                symbol_file = date_folder / f"{symbol}.parquet"
                if symbol_file.exists() and symbol_file.stat().st_size > 0:
                    try:
                        df = pd.read_parquet(symbol_file)
                        dfs.append(df)
                    except Exception as e:
                        print(f"[Storage] 读取{symbol_file}失败: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        # 合并所有数据
        combined = pd.concat(dfs, ignore_index=True)
        combined['datetime'] = pd.to_datetime(combined['datetime'])
        combined = combined.sort_values('datetime').reset_index(drop=True)
        
        return combined

    def query_kline(self, symbol: str, start_time: str, end_time: str) -> pd.DataFrame:
        """
        查询指定时间区间K线（兼容性接口）
        
        Args:
            symbol: 合约代码
            start_time: 开始时间（可以是日期或完整时间）
            end_time: 结束时间
        
        Returns:
            查询结果DataFrame
        """
        # 从时间字符串提取日期
        start_date = start_time[:10].replace('-', '')  # YYYY-MM-DD -> YYYYMMDD
        end_date = end_time[:10].replace('-', '')
        
        # 加载数据
        df = self.load_kline(symbol, start_date, end_date)
        
        if df.empty:
            return df
        
        # 时间范围过滤
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)]
        
        return df
    
    def query_ticks(self, symbol: str, start_time: str, end_time: str) -> pd.DataFrame:
        """
        查询指定时间区间Tick（兼容性接口）
        
        Args:
            symbol: 合约代码
            start_time: 开始时间（可以是日期或完整时间）
            end_time: 结束时间
        
        Returns:
            查询结果DataFrame
        """
        # 从时间字符串提取日期
        start_date = start_time[:10].replace('-', '')  # YYYY-MM-DD -> YYYYMMDD
        end_date = end_time[:10].replace('-', '')
        
        # 加载数据
        df = self.load_ticks(symbol, start_date, end_date)
        
        if df.empty:
            return df
        
        # 时间范围过滤
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)]
        
        return df
