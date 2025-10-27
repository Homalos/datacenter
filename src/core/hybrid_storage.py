#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : hybrid_storage.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 混合存储 - 智能路由SQLite（热数据）和CSV（冷数据，延迟压缩）
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from collections import deque

from src.core.event import Event, EventType
from src.core.event_bus import EventBus
from src.core.object import TickData
from src.core.sqlite_storage import SQLiteStorage
from src.core.storage import DataStorage
from src.utils.log import get_logger


class HybridStorage:
    """
    混合存储 - 智能路由SQLite和CSV归档（延迟压缩）
    
    存储策略：
    1. 热数据（近7天）：写入SQLite + CSV未压缩归档（交易时间高性能）
    2. 冷数据（7天前）：CSV归档（非交易时间批量压缩为tar.gz）
    
    查询策略：
    1. 查询近7天数据：从SQLite查询（快速）
    2. 查询历史数据：从CSV归档查询（需要时自动解压）
    3. 跨越时间段：合并SQLite和CSV结果
    
    压缩策略：
    - 交易时间：写入未压缩CSV，性能最优
    - 非交易时间：整个交易日文件夹打包为tar.gz，压缩率更高
    """
    
    def __init__(self,
                 event_bus: Optional[EventBus] = None,
                 sqlite_db_path: str = "data/db",
                 parquet_tick_path: str = "data/ticks",
                 parquet_kline_path: str = "data/klines",
                 retention_days: int = 7,
                 batch_size: int = 100,
                 trading_day_manager = None):
        """
        初始化混合存储
        
        Args:
            event_bus: 事件总线（可选，如果提供则自动订阅 TICK 事件）
            sqlite_db_path: SQLite数据库路径
            parquet_tick_path: Tick Parquet文件路径
            parquet_kline_path: K线 Parquet文件路径
            retention_days: SQLite数据保留天数
            batch_size: Tick批量写入大小（默认100条）
            trading_day_manager: 交易日管理器
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # SQLite存储层（热数据）
        self.sqlite_storage = SQLiteStorage(
            db_path=sqlite_db_path,
            retention_days=retention_days
        )
        
        # Parquet存储层（冷数据）- 分别管理Tick和K线
        self.parquet_tick_storage = DataStorage(
            base_path=parquet_tick_path,
            trading_day_manager=trading_day_manager
        )
        self.parquet_kline_storage = DataStorage(
            base_path=parquet_kline_path,
            trading_day_manager=trading_day_manager
        )
        
        self.retention_days = retention_days
        self.batch_size = batch_size
        
        # Tick 数据缓冲区（批量写入以提高性能）
        self.tick_buffer: deque[TickData] = deque(maxlen=batch_size * 2)
        
        # 如果提供了事件总线，订阅 TICK 事件
        if event_bus:
            event_bus.subscribe(EventType.TICK, self._on_tick)
            self.logger.info("已订阅 TICK 事件，将自动保存原始行情数据")
        
        self.logger.info(
            f"混合存储初始化完成，SQLite保留{retention_days}天，"
            f"Tick数据: {parquet_tick_path}，K线数据: {parquet_kline_path}，批量大小: {batch_size}"
        )
    
    def _on_tick(self, event: Event) -> None:
        """
        处理 TICK 事件（批量保存）
        
        Args:
            event: TICK 事件
        """
        try:
            # 解析 Tick 数据
            payload = event.payload
            if not payload or "data" not in payload:
                self.logger.warning("收到空payload或缺少data字段的TICK事件")
                return
            
            tick: TickData = payload["data"]
            if not tick:
                self.logger.warning("TICK事件中的data为空")
                return
            
            # 添加到缓冲区
            self.tick_buffer.append(tick)
            
            # 每100条打印一次日志
            if not hasattr(self, '_tick_recv_count'):
                self._tick_recv_count = 0
            self._tick_recv_count += 1
            
            if self._tick_recv_count % 100 == 0:
                self.logger.info(f"✓ HybridStorage已接收 {self._tick_recv_count} 条Tick | 缓冲区: {len(self.tick_buffer)}/{self.batch_size}")
            
            # 当缓冲区达到批量大小时，执行批量写入
            if len(self.tick_buffer) >= self.batch_size:
                self.logger.info(f"缓冲区已满({len(self.tick_buffer)}条)，开始批量保存...")
                self._flush_tick_buffer()
        
        except Exception as e:
            self.logger.error(f"处理 TICK 事件失败: {e}", exc_info=True)
    
    def _flush_tick_buffer(self) -> None:
        """刷新 Tick 缓冲区到存储层（严格按照 TickData 字段定义）"""
        if not self.tick_buffer:
            return
        
        try:
            # 先创建副本并清空原缓冲区（避免并发修改）
            ticks_to_save = list(self.tick_buffer)
            self.tick_buffer.clear()
            
            self.logger.info(f"→ 开始刷新 {len(ticks_to_save)} 条Tick到存储层...")
            # 将 TickData 对象转换为 DataFrame（包含所有字段）
            tick_dicts = []
            for tick in ticks_to_save:
                # 构建 datetime（用于时间序列查询）
                datetime_val = None
                if tick.trading_day and tick.update_time:
                    try:
                        datetime_val = pd.to_datetime(f"{tick.trading_day} {tick.update_time}.{tick.update_millisec:03d}")
                    except:
                        datetime_val = pd.to_datetime(f"{tick.trading_day} {tick.update_time}")
                
                # 严格按照 TickData 定义的所有字段
                tick_dict = {
                    # 基础信息
                    "instrument_id": tick.instrument_id,
                    "exchange_id": tick.exchange_id.value if tick.exchange_id else None,
                    "exchange_inst_id": tick.exchange_inst_id,
                    "trading_day": tick.trading_day,
                    "action_day": tick.action_day,
                    
                    # 时间信息
                    "datetime": datetime_val,
                    "timestamp": tick.timestamp,
                    "update_time": tick.update_time,
                    "update_millisec": tick.update_millisec,
                    
                    # 价格信息
                    "last_price": tick.last_price,
                    "open_price": tick.open_price,
                    "highest_price": tick.highest_price,
                    "lowest_price": tick.lowest_price,
                    "close_price": tick.close_price,
                    "average_price": tick.average_price,
                    
                    # 前日数据
                    "pre_settlement_price": tick.pre_settlement_price,
                    "pre_close_price": tick.pre_close_price,
                    "pre_open_interest": tick.pre_open_interest,
                    
                    # 当日统计
                    "settlement_price": tick.settlement_price,
                    "upper_limit_price": tick.upper_limit_price,
                    "lower_limit_price": tick.lower_limit_price,
                    "banding_upper_price": tick.banding_upper_price,
                    "banding_lower_price": tick.banding_lower_price,
                    
                    # 成交量和持仓
                    "volume": tick.volume,
                    "turnover": tick.turnover,
                    "open_interest": tick.open_interest,
                    
                    # Delta
                    "pre_delta": tick.pre_delta,
                    "curr_delta": tick.curr_delta,
                    
                    # 买一档
                    "bid_price_1": tick.bid_price_1,
                    "bid_volume_1": tick.bid_volume_1,
                    "ask_price_1": tick.ask_price_1,
                    "ask_volume_1": tick.ask_volume_1,
                    
                    # 买卖二档
                    "bid_price_2": tick.bid_price_2,
                    "bid_volume_2": tick.bid_volume_2,
                    "ask_price_2": tick.ask_price_2,
                    "ask_volume_2": tick.ask_volume_2,
                    
                    # 买卖三档
                    "bid_price_3": tick.bid_price_3,
                    "bid_volume_3": tick.bid_volume_3,
                    "ask_price_3": tick.ask_price_3,
                    "ask_volume_3": tick.ask_volume_3,
                    
                    # 买卖四档
                    "bid_price_4": tick.bid_price_4,
                    "bid_volume_4": tick.bid_volume_4,
                    "ask_price_4": tick.ask_price_4,
                    "ask_volume_4": tick.ask_volume_4,
                    
                    # 买卖五档
                    "bid_price_5": tick.bid_price_5,
                    "bid_volume_5": tick.bid_volume_5,
                    "ask_price_5": tick.ask_price_5,
                    "ask_volume_5": tick.ask_volume_5,
                }
                tick_dicts.append(tick_dict)
            
            df = pd.DataFrame(tick_dicts)
            
            # 批量保存
            self.save_ticks(df)
            
            self.logger.info(f"✓ 已批量保存 {len(tick_dicts)} 条 Tick 数据到存储层（包含完整字段）")
        
        except Exception as e:
            self.logger.error(f"刷新 Tick 缓冲区失败: {e}", exc_info=True)
    
    def save_ticks(self, df: pd.DataFrame) -> None:
        """
        保存Tick数据（同时保存到SQLite和Parquet）
        
        Args:
            df: Tick数据DataFrame
        """
        if df.empty:
            return
        
        try:
            self.logger.info(f"  → 保存 {len(df)} 条Tick到SQLite...")
            # 1. 保存到SQLite（热数据，快速查询）
            self.sqlite_storage.save_ticks(df)
            self.logger.info(f"  ✓ SQLite保存成功")
            
            # 2. 保存到CSV归档（冷数据，按交易日统一保存）
            # 不传date参数，使用trading_day_manager的交易日
            if "instrument_id" in df.columns:
                parquet_count = 0
                for instrument_id, group in df.groupby("instrument_id"):
                    self.parquet_tick_storage.save_ticks(
                        symbol=str(instrument_id),
                        df=group,
                        date=None  # 使用trading_day_manager的交易日
                    )
                    parquet_count += len(group)
                self.logger.info(f"  ✓ CSV保存成功 ({parquet_count}条，未压缩，交易日统一)")
        
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
            
            # 2. 保存到CSV归档（冷数据，按交易日统一保存）
            # 不传date参数，使用trading_day_manager的交易日
            if all(col in df.columns for col in ["instrument_id", "interval"]):
                for (instrument_id, interval), group in df.groupby(["instrument_id", "interval"]):
                    symbol_with_interval = f"{instrument_id}_{interval}"
                    self.parquet_kline_storage.save_kline(
                        symbol=symbol_with_interval,
                        df=group,
                        date=None  # 使用trading_day_manager的交易日
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
                    f"从CSV归档查询到 {len(df_parquet)} 条历史Tick数据"
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
                    f"从CSV归档查询到 {len(df_parquet)} 条历史K线数据"
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
            # 计算当天的开始和结束时间
            day_start = datetime.combine(current_date, datetime.min.time())
            day_end = datetime.combine(current_date, datetime.max.time())
            
            # 使用 start_time 和 end_time 参数
            df = self.parquet_tick_storage.query_ticks(
                symbol=instrument_id,
                start_time=max(day_start, start_dt).isoformat(),
                end_time=min(day_end, end_dt).isoformat()
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
            # 计算当天的开始和结束时间
            day_start = datetime.combine(current_date, datetime.min.time())
            day_end = datetime.combine(current_date, datetime.max.time())
            
            # 使用 start_time 和 end_time 参数
            df = self.parquet_kline_storage.query_kline(
                symbol=symbol_with_interval,
                start_time=max(day_start, start_dt).isoformat(),
                end_time=min(day_end, end_dt).isoformat()
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
            "storage_type": "hybrid (SQLite + CSV, 延迟压缩)",
            "retention_days": self.retention_days,
            "sqlite": sqlite_stats,
            # "parquet": parquet_stats
        }

