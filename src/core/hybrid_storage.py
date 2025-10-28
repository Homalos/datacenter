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
import threading
import time
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
                 parquet_tick_path: str = "data/csv/ticks",
                 parquet_kline_path: str = "data/csv/klines",
                 retention_days: int = 7,
                 flush_interval: int = 60,
                 max_buffer_size: int = 10000,
                 buffer_warning_threshold: float = 0.7,  # 警告阈值（70%）
                 buffer_flush_threshold: float = 0.85,  # 提前刷新阈值（85%）
                 trading_day_manager = None):
        """
        初始化混合存储
        
        Args:
            event_bus: 事件总线（可选，如果提供则自动订阅 TICK 事件）
            sqlite_db_path: SQLite数据库路径
            parquet_tick_path: Tick CSV文件路径
            parquet_kline_path: K线 CSV文件路径
            retention_days: SQLite数据保留天数
            flush_interval: 定时刷新间隔（秒），默认60秒（1分钟）
            max_buffer_size: 缓冲区上限，默认10000条（防止内存爆炸）
            buffer_warning_threshold: 警告阈值（0.7 = 70%）
            buffer_flush_threshold: 提前刷新阈值（0.85 = 85%）
            trading_day_manager: 交易日管理器
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # SQLite存储层（热数据）
        self.sqlite_storage = SQLiteStorage(
            db_path=sqlite_db_path,
            retention_days=retention_days,
            trading_day_manager=trading_day_manager
        )
        
        # CSV存储层（冷数据）- 分别管理Tick和K线
        self.parquet_tick_storage = DataStorage(
            base_path=parquet_tick_path,
            trading_day_manager=trading_day_manager
        )
        self.parquet_kline_storage = DataStorage(
            base_path=parquet_kline_path,
            trading_day_manager=trading_day_manager
        )
        
        self.retention_days = retention_days
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self.buffer_warning_threshold = buffer_warning_threshold
        self.buffer_flush_threshold = buffer_flush_threshold
        
        # 计算实际阈值（条数）
        self._warning_size = int(max_buffer_size * buffer_warning_threshold)  # 7000条
        self._flush_size = int(max_buffer_size * buffer_flush_threshold)      # 8500条
        
        # 缓冲区锁（保护并发访问，防止数据丢失）
        self._buffer_lock = threading.Lock()
        
        # Tick 数据缓冲区
        self.tick_buffer: deque[TickData] = deque()
        
        # 统计计数器
        self._tick_recv_count = 0
        
        # 定时刷新线程
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_flush = threading.Event()
        
        # 启动定时刷新线程
        self._start_flush_thread()
        
        # 如果提供了事件总线，订阅 TICK 事件
        if event_bus:
            event_bus.subscribe(EventType.TICK, self._on_tick)
            self.logger.info(
                f"已订阅 TICK 事件，定时刷新: {flush_interval}秒，"
                f"缓冲区: {max_buffer_size}条（⚠️{self._warning_size} / 🟡{self._flush_size} / 🔴{max_buffer_size}）"
            )
        
        self.logger.info(
            f"混合存储初始化完成，SQLite保留{retention_days}天，"
            f"三级阈值策略: ⚠️警告{int(buffer_warning_threshold*100)}% / "
            f"🟡提前刷新{int(buffer_flush_threshold*100)}% / 🔴紧急刷新100%"
        )
    
    def _start_flush_thread(self) -> None:
        """启动定时刷新线程"""
        self._stop_flush.clear()
        self._flush_thread = threading.Thread(
            target=self._flush_worker,
            name="HybridStorage-FlushWorker",
            daemon=True
        )
        self._flush_thread.start()
        self.logger.info(f"定时刷新线程已启动，间隔: {self.flush_interval}秒")
    
    def _flush_worker(self) -> None:
        """定时刷新工作线程（已适配锁机制 + 快速预检查优化）"""
        last_flush_time = time.time()
        
        while not self._stop_flush.is_set():
            try:
                # 检查是否到达刷新时间
                current_time = time.time()
                elapsed = current_time - last_flush_time
                
                if elapsed >= self.flush_interval:
                    # 快速预检查（无锁）：避免在缓冲区为空时仍然获取锁
                    # 注意：这是一个无锁的快速检查，可能不完全准确，但可以减少锁竞争
                    if len(self.tick_buffer) > 0:
                        # 持锁检查并刷新（二次确认）
                        with self._buffer_lock:
                            buffer_size = len(self.tick_buffer)
                            if buffer_size > 0:
                                self.logger.info(
                                    f"⏰ 定时触发刷新，缓冲区: {buffer_size} 条Tick"
                                )
                                self._flush_tick_buffer_locked()
                    
                    # 无论是否刷新，都重置定时器（避免累积延迟）
                    last_flush_time = current_time
                
                # 短暂休眠（避免CPU占用）
                time.sleep(1)  # 每秒检查一次
                
            except Exception as e:
                self.logger.error(f"定时刷新线程异常: {e}", exc_info=True)
                time.sleep(5)  # 异常后等待5秒
    
    def stop(self) -> None:
        """停止定时刷新线程，刷新剩余缓冲区（已适配锁机制）"""
        self.logger.info("正在停止 HybridStorage...")
        
        # 停止定时刷新线程
        self._stop_flush.set()
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=5.0)
            self.logger.info("定时刷新线程已停止")
        
        # 刷新剩余缓冲区（持锁检查）
        with self._buffer_lock:
            buffer_size = len(self.tick_buffer)
            if buffer_size > 0:
                self.logger.warning(
                    f"优雅关闭：刷新剩余 {buffer_size} 条Tick..."
                )
                self._flush_tick_buffer_locked()
                self.logger.info("✓ 缓冲区已刷新")
        
        self.logger.info("HybridStorage 已停止")
    
    def _on_tick(self, event: Event) -> None:
        """
        处理 TICK 事件（三级阈值策略 + 锁保护）
        
        Args:
            event: TICK 事件
            
        触发策略（三级阈值）：
            1. 主策略：定时刷新（每60秒） - 正常负载
            2. 提前刷新：缓冲区达到85%时提前刷新 - 中高负载
            3. 紧急刷新：缓冲区达到100%时紧急刷新 - 极高负载（安全阀）
        
        线程安全：
            - 使用 _buffer_lock 保护缓冲区的并发访问
            - 复制+清空操作在锁保护下原子执行
            - 实际保存在后台线程执行，避免阻塞Tick接收
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
            
            # ===== 临界区：添加到缓冲区并检查阈值 =====
            with self._buffer_lock:
                self.tick_buffer.append(tick)
                buffer_size = len(self.tick_buffer)
                self._tick_recv_count += 1
                
                # 🔴 紧急刷新（100%）：缓冲区已满（安全阀）
                if buffer_size >= self.max_buffer_size:
                    buffer_usage = buffer_size / self.max_buffer_size * 100
                    self.logger.error(
                        f"🔴 缓冲区已满 ({buffer_size}/{self.max_buffer_size} 条, {buffer_usage:.1f}%)，"
                        f"触发紧急刷新（安全阀）"
                    )
                    self._flush_tick_buffer_locked()
                    return
                
                # 🟡 提前刷新（85%）：缓冲区接近满（主动防御）
                if buffer_size >= self._flush_size:
                    buffer_usage = buffer_size / self.max_buffer_size * 100
                    self.logger.warning(
                        f"🟡 缓冲区达到刷新阈值 ({buffer_size}/{self.max_buffer_size} 条, {buffer_usage:.1f}%)，"
                        f"触发提前刷新"
                    )
                    self._flush_tick_buffer_locked()
                    return
                
                # ⚠️ 警告（70%）：缓冲区使用率偏高（仅记录日志，每1000条打印一次）
                if buffer_size >= self._warning_size:
                    if self._tick_recv_count % 1000 == 0:
                        buffer_usage = buffer_size / self.max_buffer_size * 100
                        self.logger.warning(
                            f"⚠️ 缓冲区使用率偏高 ({buffer_size}/{self.max_buffer_size} 条, {buffer_usage:.1f}%)，"
                            f"等待定时刷新或提前刷新"
                        )
                    return
            
            # ===== 正常日志（在临界区外，避免持锁时间过长）=====
            if self._tick_recv_count % 1000 == 0:
                # 快速获取缓冲区大小
                with self._buffer_lock:
                    buffer_size = len(self.tick_buffer)
                buffer_usage = buffer_size / self.max_buffer_size * 100
                self.logger.info(
                    f"✓ HybridStorage已接收 {self._tick_recv_count} 条Tick | "
                    f"缓冲区: {buffer_size}/{self.max_buffer_size} ({buffer_usage:.1f}%)"
                )
        
        except Exception as e:
            self.logger.error(f"处理 TICK 事件失败: {e}", exc_info=True)
    
    def _flush_tick_buffer_locked(self) -> None:
        """
        刷新 Tick 缓冲区（持锁版本，调用前必须持有_buffer_lock）
        
        关键设计：
        1. 复制并清空缓冲区（原子操作，在锁保护下）
        2. 立即释放锁（避免阻塞Tick接收）
        3. 在后台线程执行实际保存（耗时操作）
        
        Note:
            - 此方法必须在持有_buffer_lock的情况下调用
            - 调用者负责持有锁，本方法不加锁
            - 清空后的缓冲区可立即接收新Tick，不会丢失数据
        """
        if not self.tick_buffer:
            return
        
        # ===== 临界区：复制并清空（原子操作）=====
        ticks_to_save = list(self.tick_buffer)
        self.tick_buffer.clear()
        # 注意：此时锁仍由调用者持有，在with语句结束时自动释放
        # 清空后，新的Tick可以立即进入空缓冲区，不会丢失
        
        self.logger.info(f"→ 准备刷新 {len(ticks_to_save)} 条Tick到存储层...")
        
        # ===== 在后台线程执行保存（避免阻塞Tick接收）=====
        threading.Thread(
            target=self._do_save_ticks,
            args=(ticks_to_save,),
            name=f"TickSaver-{len(ticks_to_save)}",
            daemon=True
        ).start()
    
    def _flush_tick_buffer(self) -> None:
        """刷新 Tick 缓冲区到存储层（兼容旧接口，内部加锁）"""
        with self._buffer_lock:
            self._flush_tick_buffer_locked()
    
    def _do_save_ticks(self, ticks_to_save: list[TickData]) -> None:
        """
        执行实际的Tick数据保存（在后台线程中运行）
        
        Args:
            ticks_to_save: 待保存的Tick数据列表
        """
        try:
            self.logger.info(f"→ 开始保存 {len(ticks_to_save)} 条Tick到存储层...")
            
            # 将 TickData 对象转换为 DataFrame（45个字段，PascalCase命名）
            tick_dicts = []
            for tick in ticks_to_save:
                # 构建 Timestamp（完整datetime用于时间序列查询）
                timestamp_val = None
                if tick.trading_day and tick.update_time:
                    try:
                        timestamp_val = pd.to_datetime(f"{tick.trading_day} {tick.update_time}.{tick.update_millisec:03d}")
                    except Exception:
                        timestamp_val = pd.to_datetime(f"{tick.trading_day} {tick.update_time}")
                
                # 按用户指定的45个字段顺序（PascalCase命名）
                tick_dict = {
                    # 1-2: 基础时间信息
                    "TradingDay": tick.trading_day,
                    "ExchangeID": tick.exchange_id.value if tick.exchange_id else None,
                    
                    # 3-5: 价格信息
                    "LastPrice": tick.last_price,
                    "PreSettlementPrice": tick.pre_settlement_price,
                    "PreClosePrice": tick.pre_close_price,
                    
                    # 6-10: 成交持仓
                    "PreOpenInterest": tick.pre_open_interest,
                    "OpenPrice": tick.open_price,
                    "HighestPrice": tick.highest_price,
                    "LowestPrice": tick.lowest_price,
                    "Volume": tick.volume,
                    
                    # 11-14: 统计数据
                    "Turnover": tick.turnover,
                    "OpenInterest": tick.open_interest,
                    "ClosePrice": tick.close_price,
                    "SettlementPrice": tick.settlement_price,
                    
                    # 15-18: 涨跌停和Delta
                    "UpperLimitPrice": tick.upper_limit_price,
                    "LowerLimitPrice": tick.lower_limit_price,
                    "PreDelta": tick.pre_delta,
                    "CurrDelta": tick.curr_delta,
                    
                    # 19-20: 更新时间
                    "UpdateTime": tick.update_time,
                    "UpdateMillisec": tick.update_millisec,
                    
                    # 21-24: 买一档
                    "BidPrice1": tick.bid_price_1,
                    "BidVolume1": tick.bid_volume_1,
                    "AskPrice1": tick.ask_price_1,
                    "AskVolume1": tick.ask_volume_1,
                    
                    # 25-28: 买卖二档
                    "BidPrice2": tick.bid_price_2,
                    "BidVolume2": tick.bid_volume_2,
                    "AskPrice2": tick.ask_price_2,
                    "AskVolume2": tick.ask_volume_2,
                    
                    # 29-32: 买卖三档
                    "BidPrice3": tick.bid_price_3,
                    "BidVolume3": tick.bid_volume_3,
                    "AskPrice3": tick.ask_price_3,
                    "AskVolume3": tick.ask_volume_3,
                    
                    # 33-36: 买卖四档
                    "BidPrice4": tick.bid_price_4,
                    "BidVolume4": tick.bid_volume_4,
                    "AskPrice4": tick.ask_price_4,
                    "AskVolume4": tick.ask_volume_4,
                    
                    # 37-40: 买卖五档
                    "BidPrice5": tick.bid_price_5,
                    "BidVolume5": tick.bid_volume_5,
                    "AskPrice5": tick.ask_price_5,
                    "AskVolume5": tick.ask_volume_5,
                    
                    # 41-45: 其他信息和时间戳
                    "AveragePrice": tick.average_price,
                    "ActionDay": tick.action_day,
                    "InstrumentID": tick.instrument_id,
                    "ExchangeInstID": tick.exchange_inst_id,
                    "BandingUpperPrice": tick.banding_upper_price,
                    "BandingLowerPrice": tick.banding_lower_price,
                    "Timestamp": timestamp_val,
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
            self.logger.info("  ✓ SQLite保存成功")
            
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
        保存K线数据（同时保存到SQLite和CSV归档）
        
        Args:
            df: K线数据DataFrame（包含13个核心字段，PascalCase命名）
        """
        if df.empty:
            return
        
        try:
            # 验证必要字段（PascalCase命名）
            required_cols = ["InstrumentID", "BarType", "Timestamp"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"K线数据缺少必要字段: {required_cols}，实际字段: {df.columns.tolist()}")
                return
            
            # 1. 保存到SQLite（热数据，快速查询）
            self.sqlite_storage.save_klines(df)
            
            # 2. 保存到CSV归档（冷数据，按交易日统一保存）
            # 不传date参数，使用trading_day_manager的交易日
            for (instrument_id, bar_type), group in df.groupby(["InstrumentID", "BarType"]):
                symbol_with_interval = f"{instrument_id}_{bar_type}"
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

