#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : hybrid_storage.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 混合存储 - 智能路由SQLite（热数据）和Parquet（冷数据）
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from src.core.sqlite_storage import SQLiteStorage
from src.core.storage import DataStorage
from src.utils.log import get_logger


class HybridStorage:
    """
    混合存储 - 智能路由SQLite和Parquet
    
    存储策略：
    1. 热数据（近7天）：写入SQLite + Parquet
    2. 冷数据（7天前）：只存储在Parquet
    
    查询策略：
    1. 查询近7天数据：从SQLite查询（快速）
    2. 查询历史数据：从Parquet查询（稍慢）
    3. 跨越时间段：合并SQLite和Parquet结果
    """
    
    def __init__(self,
                 sqlite_db_path: str = "data/db",
                 parquet_base_path: str = "data",
                 retention_days: int = 7):
        """
        初始化混合存储
        
        Args:
            sqlite_db_path: SQLite数据库路径
            parquet_base_path: Parquet文件基础路径
            retention_days: SQLite数据保留天数
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # SQLite存储层（热数据）
        self.sqlite_storage = SQLiteStorage(
            db_path=sqlite_db_path,
            retention_days=retention_days
        )
        
        # Parquet存储层（冷数据）
        self.parquet_storage = DataStorage(base_path=parquet_base_path)
        
        self.retention_days = retention_days
        
        self.logger.info(
            f"混合存储初始化完成，SQLite保留{retention_days}天，"
            f"历史数据存储在Parquet"
        )
    
    def save_ticks(self, df: pd.DataFrame) -> None:
        """
        保存Tick数据（同时保存到SQLite和Parquet）
        
        Args:
            df: Tick数据DataFrame
        """
        if df.empty:
            return
        
        try:
            # 1. 保存到SQLite（热数据，快速查询）
            self.sqlite_storage.save_ticks(df)
            
            # 2. 保存到Parquet（冷数据归档）
            # 按日期和合约保存
            if "instrument_id" in df.columns and "datetime" in df.columns:
                for instrument_id, group in df.groupby("instrument_id"):
                    # 确保datetime列是datetime类型
                    if not pd.api.types.is_datetime64_any_dtype(group["datetime"]):
                        group["datetime"] = pd.to_datetime(group["datetime"])
                    
                    # 按日期保存
                    for date, date_group in group.groupby(group["datetime"].dt.date):
                        date_str = date.strftime("%Y%m%d")
                        self.parquet_storage.save_tick(
                            symbol=str(instrument_id),
                            df=date_group,
                            date=date_str
                        )
        
        except Exception as e:
            self.logger.error(f"保存Tick数据失败: {e}", exc_info=True)
    
    def save_klines(self, df: pd.DataFrame) -> None:
        """
        保存K线数据（同时保存到SQLite和Parquet）
        
        Args:
            df: K线数据DataFrame
        """
        if df.empty:
            return
        
        try:
            # 1. 保存到SQLite（热数据，快速查询）
            self.sqlite_storage.save_klines(df)
            
            # 2. 保存到Parquet（冷数据归档）
            # 按日期、合约和周期保存
            if all(col in df.columns for col in ["instrument_id", "interval", "datetime"]):
                for (instrument_id, interval), group in df.groupby(["instrument_id", "interval"]):
                    # 确保datetime列是datetime类型
                    if not pd.api.types.is_datetime64_any_dtype(group["datetime"]):
                        group["datetime"] = pd.to_datetime(group["datetime"])
                    
                    # 按日期保存
                    for date, date_group in group.groupby(group["datetime"].dt.date):
                        date_str = date.strftime("%Y%m%d")
                        symbol_with_interval = f"{instrument_id}_{interval}"
                        self.parquet_storage.save_kline(
                            symbol=symbol_with_interval,
                            df=date_group,
                            date=date_str
                        )
        
        except Exception as e:
            self.logger.error(f"保存K线数据失败: {e}", exc_info=True)
    
    def query_ticks(self,
                    instrument_id: str,
                    start_time: str,
                    end_time: str) -> pd.DataFrame:
        """
        查询Tick数据（智能路由）
        
        Args:
            instrument_id: 合约代码
            start_time: 开始时间（ISO格式）
            end_time: 结束时间（ISO格式）
        
        Returns:
            Tick数据DataFrame
        """
        try:
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
            cutoff_dt = datetime.now() - timedelta(days=self.retention_days)
            
            results = []
            
            # 1. 查询历史数据（Parquet）
            if start_dt < cutoff_dt:
                parquet_end = min(end_dt, cutoff_dt)
                df_parquet = self._query_ticks_from_parquet(
                    instrument_id,
                    start_dt,
                    parquet_end
                )
                if not df_parquet.empty:
                    results.append(df_parquet)
                
                self.logger.debug(
                    f"从Parquet查询到 {len(df_parquet)} 条历史Tick数据"
                )
            
            # 2. 查询近期数据（SQLite）
            if end_dt >= cutoff_dt:
                sqlite_start = max(start_dt, cutoff_dt)
                df_sqlite = self.sqlite_storage.query_ticks(
                    instrument_id,
                    sqlite_start.isoformat(),
                    end_dt.isoformat()
                )
                if not df_sqlite.empty:
                    results.append(df_sqlite)
                
                self.logger.debug(
                    f"从SQLite查询到 {len(df_sqlite)} 条近期Tick数据"
                )
            
            # 3. 合并结果
            if results:
                df = pd.concat(results, ignore_index=True)
                df = df.sort_values("datetime")
                return df
            
            return pd.DataFrame()
        
        except Exception as e:
            self.logger.error(f"查询Tick数据失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def query_klines(self,
                     instrument_id: str,
                     interval: str,
                     start_time: str,
                     end_time: str) -> pd.DataFrame:
        """
        查询K线数据（智能路由）
        
        Args:
            instrument_id: 合约代码
            interval: K线周期
            start_time: 开始时间（ISO格式）
            end_time: 结束时间（ISO格式）
        
        Returns:
            K线数据DataFrame
        """
        try:
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
            cutoff_dt = datetime.now() - timedelta(days=self.retention_days)
            
            results = []
            
            # 1. 查询历史数据（Parquet）
            if start_dt < cutoff_dt:
                parquet_end = min(end_dt, cutoff_dt)
                df_parquet = self._query_klines_from_parquet(
                    instrument_id,
                    interval,
                    start_dt,
                    parquet_end
                )
                if not df_parquet.empty:
                    results.append(df_parquet)
                
                self.logger.debug(
                    f"从Parquet查询到 {len(df_parquet)} 条历史K线数据"
                )
            
            # 2. 查询近期数据（SQLite）
            if end_dt >= cutoff_dt:
                sqlite_start = max(start_dt, cutoff_dt)
                df_sqlite = self.sqlite_storage.query_klines(
                    instrument_id,
                    interval,
                    sqlite_start.isoformat(),
                    end_dt.isoformat()
                )
                if not df_sqlite.empty:
                    results.append(df_sqlite)
                
                self.logger.debug(
                    f"从SQLite查询到 {len(df_sqlite)} 条近期K线数据"
                )
            
            # 3. 合并结果
            if results:
                df = pd.concat(results, ignore_index=True)
                df = df.sort_values("datetime")
                return df
            
            return pd.DataFrame()
        
        except Exception as e:
            self.logger.error(f"查询K线数据失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def _query_ticks_from_parquet(self,
                                   instrument_id: str,
                                   start_dt: datetime,
                                   end_dt: datetime) -> pd.DataFrame:
        """
        从Parquet查询Tick数据
        
        Args:
            instrument_id: 合约代码
            start_dt: 开始时间
            end_dt: 结束时间
        
        Returns:
            Tick数据DataFrame
        """
        results = []
        
        # 遍历日期范围
        current_date = start_dt.date()
        end_date = end_dt.date()
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            df = self.parquet_storage.query_tick(
                symbol=instrument_id,
                start=start_dt.isoformat(),
                end=end_dt.isoformat(),
                date=date_str
            )
            if not df.empty:
                results.append(df)
            
            current_date += timedelta(days=1)
        
        if results:
            return pd.concat(results, ignore_index=True)
        
        return pd.DataFrame()
    
    def _query_klines_from_parquet(self,
                                    instrument_id: str,
                                    interval: str,
                                    start_dt: datetime,
                                    end_dt: datetime) -> pd.DataFrame:
        """
        从Parquet查询K线数据
        
        Args:
            instrument_id: 合约代码
            interval: K线周期
            start_dt: 开始时间
            end_dt: 结束时间
        
        Returns:
            K线数据DataFrame
        """
        results = []
        
        # 遍历日期范围
        current_date = start_dt.date()
        end_date = end_dt.date()
        symbol_with_interval = f"{instrument_id}_{interval}"
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            df = self.parquet_storage.query_kline(
                symbol=symbol_with_interval,
                start=start_dt.isoformat(),
                end=end_dt.isoformat(),
                date=date_str
            )
            if not df.empty:
                results.append(df)
            
            current_date += timedelta(days=1)
        
        if results:
            return pd.concat(results, ignore_index=True)
        
        return pd.DataFrame()
    
    def get_statistics(self) -> dict:
        """
        获取存储统计信息
        
        Returns:
            统计信息字典
        """
        sqlite_stats = self.sqlite_storage.get_statistics()
        # parquet_stats = self.parquet_storage.get_statistics()  # 如果有的话
        
        return {
            "storage_type": "hybrid (SQLite + Parquet)",
            "retention_days": self.retention_days,
            "sqlite": sqlite_stats,
            # "parquet": parquet_stats
        }

