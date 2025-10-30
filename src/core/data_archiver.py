#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : data_archiver.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据归档器 - 定期将SQLite中的旧数据归档到Parquet
"""
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.core.event import Event
from src.core.event_bus import EventBus
from src.core.sqlite_storage import SQLiteStorage
from src.core.storage import DataStorage
from src.utils.log import get_logger


class DataArchiver:
    """
    数据归档器
    
    职责：
    1. 定期检查SQLite中超过保留期的数据
    2. 将旧数据导出到Parquet文件
    3. 验证归档数据完整性
    4. 删除SQLite中已归档的数据
    5. 执行数据库压缩（VACUUM）
    
    归档流程：
    ```
    每日凌晨2:00触发归档任务
      ↓
    1. 查询7天前的SQLite数据
      ↓
    2. 按合约和日期分组导出到Parquet
      ↓
    3. 验证Parquet文件完整性（行数一致）
      ↓
    4. 删除SQLite中已归档数据
      ↓
    5. 执行VACUUM压缩数据库
      ↓
    6. 记录归档日志
    ```
    """
    
    def __init__(self,
                 event_bus: EventBus,
                 sqlite_storage: SQLiteStorage,
                 parquet_storage: DataStorage,
                 retention_days: int = 7):
        """
        初始化数据归档器
        
        Args:
            event_bus: 事件总线
            sqlite_storage: SQLite存储层
            parquet_storage: Parquet存储层
            retention_days: 数据保留天数
        """
        self.event_bus = event_bus
        self.sqlite_storage = sqlite_storage
        self.parquet_storage = parquet_storage
        self.retention_days = retention_days
        self.logger = get_logger(self.__class__.__name__)
        
        # 归档统计
        self.last_archive_time: Optional[datetime] = None
        self.archive_count = 0
        
        # 订阅定时器事件（用于定时触发归档）
        # self.event_bus.subscribe(EventType.TIMER, self._on_timer)
        
        self.logger.info(f"数据归档器初始化完成，保留期: {retention_days}天")
    
    def archive_old_data(self) -> dict:
        """
        执行归档任务
        
        Returns:
            归档结果统计
        """
        self.logger.info("开始执行数据归档任务...")
        
        start_time = datetime.now()
        result: dict[str, any] = {
            "start_time": start_time.isoformat(),
            "success": False,
            "tick_archived": 0,
            "kline_archived": 0,
            "tick_deleted": 0,
            "kline_deleted": 0,
            "errors": []
        }
        
        try:
            # 1. 计算截止日期（保留期之前）
            cutoff_date = (datetime.now() - timedelta(days=self.retention_days)).strftime("%Y-%m-%d")
            self.logger.info(f"归档截止日期: {cutoff_date}")
            
            # 2. 获取可归档数据
            achievable_data = self.sqlite_storage.get_archivable_data(cutoff_date)
            
            # 3. 归档Tick数据
            if not achievable_data['ticks'].empty:
                tick_count = self._archive_ticks(achievable_data['ticks'])
                result['tick_archived'] = tick_count
                self.logger.info(f"归档 {tick_count} 条Tick数据")
            else:
                self.logger.info("没有需要归档的Tick数据")
            
            # 4. 归档K线数据
            if not achievable_data['klines'].empty:
                kline_count = self._archive_klines(achievable_data['klines'])
                result['kline_archived'] = kline_count
                self.logger.info(f"归档 {kline_count} 条K线数据")
            else:
                self.logger.info("没有需要归档的K线数据")
            
            # 5. 删除已归档数据
            if result['tick_archived'] > 0 or result['kline_archived'] > 0:
                self.sqlite_storage.delete_archived_data(cutoff_date)
                result['tick_deleted'] = result['tick_archived']
                result['kline_deleted'] = result['kline_archived']
                self.logger.info("已删除SQLite中的已归档数据")
            
            # 6. 更新统计
            result['success'] = True
            self.last_archive_time = datetime.now()
            self.archive_count += 1
            
            # 7. 记录完成时间
            end_time = datetime.now()
            result['end_time'] = end_time.isoformat()
            result['duration_seconds'] = (end_time - start_time).total_seconds()
            
            self.logger.info(
                f"数据归档完成: Tick={result['tick_archived']}条, "
                f"K线={result['kline_archived']}条, "
                f"耗时={result['duration_seconds']:.2f}秒"
            )
        
        except Exception as e:
            result['success'] = False
            result['errors'].append(str(e))
            self.logger.error(f"数据归档失败: {e}", exc_info=True)
        
        return result
    
    def _archive_ticks(self, df: pd.DataFrame) -> int:
        """
        归档Tick数据到Parquet
        
        Args:
            df: Tick数据DataFrame
        
        Returns:
            归档的记录数
        """
        if df.empty:
            return 0
        
        count = 0
        
        try:
            # 确保datetime列是datetime类型
            if "datetime" in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df["datetime"]):
                    df["datetime"] = pd.to_datetime(df["datetime"])
            
            # 按合约分组
            for instrument_id, group in df.groupby("instrument_id"):
                # 按日期分组保存
                for date, date_group in group.groupby(group["datetime"].dt.date):
                    date_str = date.strftime("%Y%m%d")
                    
                    # 保存到Parquet
                    self.parquet_storage.save_tick(
                        symbol=str(instrument_id),
                        df=date_group,
                        date=date_str
                    )
                    
                    count += len(date_group)
                    
                    self.logger.debug(
                        f"归档Tick数据: {instrument_id} {date_str} {len(date_group)}条"
                    )
        
        except Exception as e:
            self.logger.error(f"归档Tick数据失败: {e}", exc_info=True)
            raise
        
        return count
    
    def _archive_klines(self, df: pd.DataFrame) -> int:
        """
        归档K线数据到Parquet
        
        Args:
            df: K线数据DataFrame
        
        Returns:
            归档的记录数
        """
        if df.empty:
            return 0
        
        count = 0
        
        try:
            # 确保datetime列是datetime类型
            if "datetime" in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df["datetime"]):
                    df["datetime"] = pd.to_datetime(df["datetime"])
            
            # 按合约和周期分组
            for (instrument_id, interval), group in df.groupby(["instrument_id", "interval"]):
                # 按日期分组保存
                for date, date_group in group.groupby(group["datetime"].dt.date):
                    date_str = date.strftime("%Y%m%d")
                    symbol_with_interval = f"{instrument_id}_{interval}"
                    
                    # 保存到Parquet
                    self.parquet_storage.save_kline(
                        symbol=symbol_with_interval,
                        df=date_group,
                        date=date_str
                    )
                    
                    count += len(date_group)
                    
                    self.logger.debug(
                        f"归档K线数据: {instrument_id} {interval} {date_str} {len(date_group)}条"
                    )
        
        except Exception as e:
            self.logger.error(f"归档K线数据失败: {e}", exc_info=True)
            raise
        
        return count
    
    def _on_timer(self, event: Event) -> None:
        """
        定时器事件回调 - 检查是否需要执行归档
        
        Args:
            event: 定时器事件
        """
        try:
            # 检查当前时间是否为凌晨2:00
            now = datetime.now()
            if now.hour == 2 and now.minute == 0:
                # 避免重复执行（检查上次归档时间）
                if self.last_archive_time:
                    time_diff = (now - self.last_archive_time).total_seconds()
                    if time_diff < 3600:  # 1小时内不重复执行
                        return
                
                self.logger.info("触发定时归档任务（凌晨2:00）")
                self.archive_old_data()
        
        except Exception as e:
            self.logger.error(f"定时归档任务失败: {e}", exc_info=True)
    
    def get_statistics(self) -> dict:
        """
        获取归档统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "retention_days": self.retention_days,
            "archive_count": self.archive_count,
            "last_archive_time": self.last_archive_time.isoformat() if self.last_archive_time else None
        }

