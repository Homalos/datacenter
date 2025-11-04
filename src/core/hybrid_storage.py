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
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime  # noqa: F401  (保留用于未来的查询功能)
from typing import Optional

import pandas as pd

# 新增：DuckDB + 多线程CSV写入器
from src.core.duckdb_storage import DuckDBSingleFileWriter
from src.core.event import Event, EventType
from src.core.event_bus import EventBus
from src.core.object import TickData
from src.core.partitioned_csv_writer import PartitionedCSVWriter
from src.system_config import Config
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
                 parquet_tick_path: str = "data/csv/ticks",
                 parquet_kline_path: str = "data/csv/klines",
                 retention_days: int = 7,
                 flush_interval: Optional[int] = None,  # 从配置文件读取
                 max_buffer_size: Optional[int] = None,  # 从配置文件读取
                 buffer_warning_threshold: Optional[float] = None,  # 从配置文件读取
                 buffer_flush_threshold: Optional[float] = None,  # 从配置文件读取
                 trading_day_manager = None):
        """
        初始化混合存储
        
        Args:
            event_bus: 事件总线（可选，如果提供则自动订阅 TICK 事件）
            parquet_tick_path: Tick CSV文件路径
            parquet_kline_path: K线 CSV文件路径
            retention_days: SQLite数据保留天数
            flush_interval: 定时刷新间隔（秒），None时从配置文件读取
            max_buffer_size: 缓冲区上限，None时从配置文件读取
            buffer_warning_threshold: 警告阈值，None时从配置文件读取
            buffer_flush_threshold: 提前刷新阈值，None时从配置文件读取
            trading_day_manager: 交易日管理器
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # 🔥 从配置文件读取参数（如果未显式传入）
        flush_interval = flush_interval if flush_interval is not None else Config.storage_flush_interval
        max_buffer_size = max_buffer_size if max_buffer_size is not None else Config.storage_max_buffer_size
        buffer_warning_threshold = buffer_warning_threshold if buffer_warning_threshold is not None else Config.storage_buffer_warning_threshold
        buffer_flush_threshold = buffer_flush_threshold if buffer_flush_threshold is not None else Config.storage_buffer_flush_threshold
        
        # 🔥 DuckDB存储层（极速查询引擎）- 从配置文件读取批量阈值
        self.duckdb_tick_writer = DuckDBSingleFileWriter(
            db_path="data/duckdb/ticks",
            batch_threshold=Config.duckdb_tick_batch_threshold,  # 🔥 从配置读取
            data_type="ticks",
            trading_day_manager=trading_day_manager
        )
        
        self.duckdb_kline_writer = DuckDBSingleFileWriter(
            db_path="data/duckdb/klines",
            batch_threshold=Config.duckdb_kline_batch_threshold,  # 🔥 从配置读取
            data_type="klines",
            trading_day_manager=trading_day_manager
        )
        
        # 🔥 CSV多线程写入器（高吞吐归档）- 从配置文件读取批量阈值
        self.csv_tick_writer = PartitionedCSVWriter(
            base_path=parquet_tick_path,
            num_threads=Config.csv_num_threads,  # 🔥 从配置读取
            batch_threshold=Config.csv_tick_batch_threshold,  # 🔥 从配置读取
            queue_max_size=Config.csv_queue_max_size,  # 🔥 从配置读取
            trading_day_manager=trading_day_manager
        )
        
        self.csv_kline_writer = PartitionedCSVWriter(
            base_path=parquet_kline_path,
            num_threads=Config.csv_num_threads,  # 🔥 从配置读取
            batch_threshold=Config.csv_kline_batch_threshold,  # 🔥 从配置读取
            queue_max_size=Config.csv_queue_max_size,  # 🔥 从配置读取
            trading_day_manager=trading_day_manager
        )
        
        self.retention_days = retention_days
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self.buffer_warning_threshold = buffer_warning_threshold
        self.buffer_flush_threshold = buffer_flush_threshold
        
        # 计算实际阈值（条数）
        self._warning_size = int(max_buffer_size * buffer_warning_threshold)
        self._flush_size = int(max_buffer_size * buffer_flush_threshold)
        
        # 缓冲区锁（保护并发访问，防止数据丢失）
        self._buffer_lock = threading.Lock()
        
        # Tick 数据缓冲区
        self.tick_buffer: deque[TickData] = deque()
        
        # 统计计数器
        self._tick_recv_count = 0
        
        # 定时刷新线程
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_flush = threading.Event()
        
        # 🔥 修复线程泄漏：使用线程池代替创建新线程
        self._save_executor = ThreadPoolExecutor(
            max_workers=2,  # 最多2个并发保存线程
            thread_name_prefix="HybridStorage-Saver"
        )
        
        # 保存 EventBus 引用（用于 stop 时取消订阅）
        self.event_bus = event_bus
        
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
        """定时刷新工作线程（增加健康检查）"""
        last_flush_time = time.time()
        last_health_check = time.time()  # 新增
        
        while not self._stop_flush.is_set():
            try:
                current_time = time.time()
                
                # 新增：定期健康检查（每5分钟）
                if current_time - last_health_check >= 300.0:
                    try:
                        health = self.get_health_metrics()
                        self.logger.info(
                            f"系统健康检查：{health['health_status']} | "
                            f"线程: {health['threads']['total_active']} "
                            f"(工作线程: {health['threads']['worker_count']}) | "
                            f"DuckDB缓冲: Tick={health['duckdb']['tick_buffered']} "
                            f"KLine={health['duckdb']['kline_buffered']} | "
                            f"CSV队列: Tick={health['csv']['tick_queued']} "
                            f"KLine={health['csv']['kline_queued']} | "
                            f"Tick缓冲: {health['buffer']['tick_buffer_usage_pct']}%"
                        )
                        last_health_check = current_time
                    except Exception as e:
                        self.logger.error(f"健康检查失败：{e}")
                
                # 原有的定时刷新逻辑
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
        """
        停止混合存储（🔥 优雅关闭双层存储）
        
        关闭顺序：
        0. 取消订阅 TICK 事件（停止接收新数据）
        1. 停止定时刷新线程
        2. 刷新剩余缓冲区
        3. 停止DuckDB写入器
        4. 停止CSV多线程写入器
        """
        self.logger.info("正在停止 HybridStorage...")
        
        # 0. 🔥 取消订阅 TICK 事件（停止接收新数据）
        if self.event_bus:
            self.event_bus.unsubscribe(EventType.TICK, self._on_tick)
            self.logger.info("✓ 已取消订阅 TICK 事件，停止接收新数据")
        
        # 1. 停止定时刷新线程
        self._stop_flush.set()
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=5.0)
            self.logger.info("✓ 定时刷新线程已停止")
        
        # 2. 刷新剩余缓冲区（持锁检查）
        with self._buffer_lock:
            buffer_size = len(self.tick_buffer)
            if buffer_size > 0:
                self.logger.warning(
                    f"优雅关闭：刷新剩余 {buffer_size} 条Tick..."
                )
                self._flush_tick_buffer_locked()
                self.logger.info("✓ 缓冲区已刷新")
        
        # 3. 🔥 停止DuckDB写入器（刷新所有剩余数据）
        self.logger.info("停止DuckDB写入器...")
        self.duckdb_tick_writer.stop()
        self.duckdb_kline_writer.stop()
        self.logger.info("✓ DuckDB写入器已停止")
        
        # 4. 🔥 停止CSV多线程写入器（刷新所有队列）
        self.logger.info("停止CSV写入器...")
        self.csv_tick_writer.stop(timeout=30)
        self.csv_kline_writer.stop(timeout=30)
        self.logger.info("✓ CSV写入器已停止")
        
        # 5. 🔥 关闭保存线程池
        self.logger.info("关闭保存线程池...")
        self._save_executor.shutdown(wait=True)  # Python 3.10 不支持 timeout 参数
        self.logger.info("✓ 保存线程池已关闭")
        
        self.logger.info("✅ HybridStorage 已完全停止（双层存储已优雅关闭）")
    
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
        # ✅ 修复：检查停止标志（最早返回，避免处理数据）
        if self._stop_flush.is_set():
            return  # 如果已停止，直接返回，不处理任何数据
        
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
                
                # 警告（70%）：缓冲区使用率偏高（仅记录日志，每1000条打印一次）
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
        
        # ===== 使用线程池执行保存（避免阻塞Tick接收，避免线程泄漏）=====
        self._save_executor.submit(self._do_save_ticks, ticks_to_save)
    
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
            
            # 将 TickData 对象转换为 DataFrame（47个字段，PascalCase命名）
            tick_dicts = []
            for tick in ticks_to_save:
                # 转换日期格式：YYYYMMDD → YYYY-MM-DD（DuckDB DATE类型要求）
                def format_date(date_str):
                    """将YYYYMMDD格式转换为YYYY-MM-DD"""
                    if date_str and len(str(date_str)) == 8:
                        s = str(date_str)
                        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
                    return date_str
                
                # 构建 Timestamp（完整datetime用于时间序列查询）
                timestamp_val = None
                if tick.trading_day and tick.update_time:
                    try:
                        timestamp_val = pd.to_datetime(f"{tick.trading_day} {tick.update_time}.{tick.update_millisec:03d}")
                    except Exception as e:
                        # 回退到秒级精度
                        timestamp_val = pd.to_datetime(f"{tick.trading_day} {tick.update_time}")
                        self.logger.warning(f"无法将Tick时间转换为完整datetime: {e}")
                
                # 按用户指定的47个字段顺序（PascalCase命名）
                tick_dict = {
                    # 1-2: 基础时间信息
                    "TradingDay": format_date(tick.trading_day),  # 转换为YYYY-MM-DD格式
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
                    
                    # 41-47: 其他信息和时间戳
                    "AveragePrice": tick.average_price,
                    "ActionDay": format_date(tick.action_day),  # 转换为YYYY-MM-DD格式
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
        保存Tick数据（🔥 双层存储：DuckDB极速查询 + CSV多线程归档）
        
        Args:
            df: Tick数据DataFrame
        
        新架构：
        1. DuckDB：极速查询引擎（单线程写入，排序聚类）
        2. CSV：高吞吐归档（4线程并行，哈希分配）
        """
        if df.empty:
            return
        
        try:
            self.logger.info(f"  → 双层写入 {len(df)} 条Tick...")
            
            # 排序（为DuckDB优化，保证物理连续性）
            # 这是性能的关键！排序后DuckDB的Zone Maps可以精确裁剪
            df = df.sort_values(by=['InstrumentID', 'Timestamp']).reset_index(drop=True)
            
            # 1. 写入DuckDB（极速查询）
            self.duckdb_tick_writer.submit_batch(df)
            self.logger.info("  ✓ DuckDB写入队列提交成功")
            
            # 2. 写入CSV（多线程归档）
            self.csv_tick_writer.submit_batch(df)
            self.logger.info("  ✓ CSV多线程写入队列提交成功")
            
            # 获取统计信息
            duckdb_stats = self.duckdb_tick_writer.get_stats()
            csv_stats = self.csv_tick_writer.get_stats()
            
            self.logger.debug(
                f"  统计信息 - "
                f"DuckDB缓冲: {duckdb_stats['total_buffered']}条 | "
                f"CSV队列: {csv_stats['total_queued']}条"
            )
        
        except Exception as e:
            self.logger.error(f"双层写入Tick数据失败: {e}", exc_info=True)
    
    def save_klines(self, df: pd.DataFrame) -> None:
        """
        保存K线数据（双层存储：DuckDB极速查询 + CSV多线程归档）
        
        Args:
            df: K线数据DataFrame（包含核心字段，PascalCase命名）
        """
        if df.empty:
            return
        
        try:
            # 验证必要字段（PascalCase命名）
            required_cols = ["InstrumentID", "Timestamp"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"K线数据缺少必要字段: {required_cols}，实际字段: {df.columns.tolist()}")
                return
            
            self.logger.debug(f"  → 双层写入 {len(df)} 条K线...")
            
            # 排序（为DuckDB优化）
            df = df.sort_values(by=['InstrumentID', 'Timestamp']).reset_index(drop=True)
            
            # 1. 写入DuckDB（极速查询）
            self.duckdb_kline_writer.submit_batch(df)
            self.logger.debug("  ✓ DuckDB K线写入队列提交成功")
            
            # 2. 写入CSV（多线程归档）
            self.csv_kline_writer.submit_batch(df)
            self.logger.debug("  ✓ CSV K线多线程写入队列提交成功")
        
        except Exception as e:
            self.logger.error(f"双层写入K线数据失败: {e}", exc_info=True)
    
    def query_ticks(self,
                    instrument_id: str,
                    start_time: str,
                    end_time: str) -> pd.DataFrame:
        """
        查询Tick数据（TODO: 需要实现DuckDB查询引擎）
        
        Args:
            instrument_id: 合约代码
            start_time: 开始时间（ISO格式）
            end_time: 结束时间（ISO格式）
        
        Returns:
            Tick数据DataFrame
        
        Note:
            当前版本暂未实现查询功能，需要实现DuckDBQueryEngine
            参考文档：docs/DuckDB存储方案总体设计.md
        """
        self.logger.warning(
            f"查询Tick数据功能暂未实现 [合约: {instrument_id}, "
            f"时间: {start_time} ~ {end_time}]"
        )
        return pd.DataFrame()
    
    def query_klines(self,
                     instrument_id: str,
                     interval: str,
                     start_time: str,
                     end_time: str) -> pd.DataFrame:
        """
        查询K线数据（TODO: 需要实现DuckDB查询引擎）
        
        Args:
            instrument_id: 合约代码
            interval: K线周期
            start_time: 开始时间（ISO格式）
            end_time: 结束时间（ISO格式）
        
        Returns:
            K线数据DataFrame
        
        Note:
            当前版本暂未实现查询功能，需要实现DuckDBQueryEngine
            参考文档：docs/DuckDB存储方案总体设计.md
        """
        self.logger.warning(
            f"查询K线数据功能暂未实现 [合约: {instrument_id}, "
            f"周期: {interval}, 时间: {start_time} ~ {end_time}]"
        )
        return pd.DataFrame()
    
    def get_statistics(self) -> dict:
        """
        获取存储统计信息（新架构：DuckDB + CSV）
        
        Returns:
            统计信息字典
        """
        # 获取DuckDB和CSV写入器的统计信息
        duckdb_tick_stats = self.duckdb_tick_writer.get_stats()
        duckdb_kline_stats = self.duckdb_kline_writer.get_stats()
        csv_tick_stats = self.csv_tick_writer.get_stats()
        csv_kline_stats = self.csv_kline_writer.get_stats()
        
        return {
            "storage_type": "hybrid (DuckDB + CSV 双层存储)",
            "retention_days": self.retention_days,
            "duckdb": {
                "ticks": duckdb_tick_stats,
                "klines": duckdb_kline_stats
            },
            "csv": {
                "ticks": csv_tick_stats,
                "klines": csv_kline_stats
            }
        }
    
    def get_health_metrics(self) -> dict:
        """
        获取系统健康指标（新增监控）
        
        Returns:
            健康指标字典，包含：
            - 后台线程数量（仅数据中心相关线程）
            - DuckDB队列状态
            - CSV队列状态
            - 缓冲区使用率
        """
        import threading
        
        # 1. 监控后台线程数量（仅数据中心相关线程）
        thread_list = threading.enumerate()
        thread_names = [t.name for t in thread_list]
        
        # ✅ 修复：只统计数据中心相关的线程，排除 Uvicorn/FastAPI 线程
        datacenter_keywords = [
            "Worker", "Saver", "Flush", "Pool", "DuckDB", "CSV",
            "EventBus", "SyncLoop", "Timer", "HybridStorage"
        ]
        
        datacenter_threads = []
        worker_threads = []
        
        for name in thread_names:
            # 检查是否是数据中心相关线程
            for keyword in datacenter_keywords:
                if keyword in name:
                    datacenter_threads.append(name)
                    # 同时检查是否是工作线程
                    if any(k in name for k in ["Worker", "Saver", "Flush"]):
                        worker_threads.append(name)
                    break
        
        active_threads = len(datacenter_threads)  # ✅ 只统计数据中心线程
        
        # 2. 监控DuckDB队列
        duckdb_tick_stats = self.duckdb_tick_writer.get_stats()
        duckdb_kline_stats = self.duckdb_kline_writer.get_stats()
        
        # 3. 监控CSV队列大小
        csv_tick_stats = self.csv_tick_writer.get_stats()
        csv_kline_stats = self.csv_kline_writer.get_stats()
        
        # 4. 缓冲区使用率
        with self._buffer_lock:
            buffer_size = len(self.tick_buffer)
        buffer_usage = buffer_size / self.max_buffer_size * 100
        
        # 5. 评估健康状态
        health_status = self._evaluate_health(
            active_threads, 
            duckdb_tick_stats.get('total_buffered', 0),
            csv_tick_stats.get('total_queued', 0),
            buffer_usage
        )
        
        return {
            "timestamp": datetime.now().isoformat(),
            "health_status": health_status,
            "threads": {
                "total_active": active_threads,
                "worker_count": len(worker_threads),
                "worker_names": worker_threads[:10]  # 只显示前10个
            },
            "duckdb": {
                "tick_buffered": duckdb_tick_stats.get('total_buffered', 0),
                "kline_buffered": duckdb_kline_stats.get('total_buffered', 0),
                "tick_threshold": duckdb_tick_stats.get('batch_threshold', 0),
                "kline_threshold": duckdb_kline_stats.get('batch_threshold', 0)
            },
            "csv": {
                "tick_queued": csv_tick_stats.get('total_queued', 0),
                "kline_queued": csv_kline_stats.get('total_queued', 0),
                "tick_workers": csv_tick_stats.get('workers_alive', 0),
                "kline_workers": csv_kline_stats.get('workers_alive', 0),
                "queue_sizes": {
                    "tick": csv_tick_stats.get('queue_sizes', []),
                    "kline": csv_kline_stats.get('queue_sizes', [])
                }
            },
            "buffer": {
                "tick_buffer_size": buffer_size,
                "tick_buffer_max": self.max_buffer_size,
                "tick_buffer_usage_pct": round(buffer_usage, 2)
            }
        }

    @staticmethod
    def _evaluate_health(threads: int, duckdb_buf: int,
                         csv_queue: int, buffer_pct: float) -> str:
        """
        评估系统健康状态
        
        Args:
            threads: 数据中心线程数（不包括 Uvicorn/FastAPI）
            duckdb_buf: DuckDB缓冲区大小
            csv_queue: CSV队列大小
            buffer_pct: Tick缓冲区使用率（百分比）
        
        Returns:
            健康状态字符串
        """
        # 严重（✅ 更新阈值：只统计数据中心线程，正常应该在80个以内）
        if threads > 150:
            return "🔴 CRITICAL: 数据中心线程数过多"
        if duckdb_buf > 200000:
            return "🔴 CRITICAL: DuckDB队列严重积压"
        if csv_queue > 100000:
            return "🔴 CRITICAL: CSV队列严重积压"
        if buffer_pct > 90:
            return "🔴 CRITICAL: Tick缓冲区接近满载"
        
        # 警告（✅ 更新阈值：数据中心正常线程数约70个，>100为警告）
        if threads > 100:
            return "🟡 WARNING: 数据中心线程数偏高"
        if duckdb_buf > 100000:
            return "🟡 WARNING: DuckDB队列积压"
        if csv_queue > 50000:
            return "🟡 WARNING: CSV队列积压"
        if buffer_pct > 70:
            return "🟡 WARNING: Tick缓冲区使用率偏高"
        
        # 健康
        return "✅ HEALTHY"

