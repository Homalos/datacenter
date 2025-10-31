#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : bar_manager.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: K线管理器 - 管理所有合约的K线生成
"""
import threading
import pandas as pd
from typing import Optional

from src.core.bar_generator import MultiBarGenerator
from src.core.event_bus import EventBus
from src.core.event import Event, EventType
from src.core.object import TickData, BarData
from src.core.pack_payload import PackPayload
from src.system_config import Config
from src.utils.log import get_logger


class BarManager:
    """
    K线管理器
    
    职责：
    1. 监听Tick事件
    2. 为每个合约维护多周期K线生成器
    3. K线生成完成后发布BAR事件
    4. 将K线数据保存到存储层
    """
    
    def __init__(self, 
                 event_bus: EventBus,
                 storage,  # 存储层（HybridStorage 或 DataStorage）
                 intervals: Optional[list[str]] = None):
        """
        初始化K线管理器
        
        Args:
            event_bus: 事件总线
            storage: 存储层实例
            intervals: K线周期列表，如 ["1m", "5m", "15m", "30m", "1h", "1d"]
        """
        self.event_bus = event_bus
        self.storage = storage
        self.intervals = intervals or Config.bar_intervals
        self.logger = get_logger(self.__class__.__name__)
        
        # 每个合约对应一个MultiBarGenerator
        # key: instrument_id, value: MultiBarGenerator
        self.generators: dict[str, MultiBarGenerator] = {}
        
        # 🔒 线程锁：保护 generators 字典的并发访问
        self._generators_lock = threading.Lock()
        
        # 订阅Tick事件
        self.event_bus.subscribe(EventType.TICK, self._on_tick)
        
        self.logger.info(f"K线管理器初始化完成，支持周期: {self.intervals}")
    
    def _on_tick(self, event: Event) -> None:
        """
        处理Tick事件
        
        Args:
            event: Tick事件
        """
        try:
            # 解析Tick数据
            payload = event.payload
            if not payload or "data" not in payload:
                return
            
            tick: TickData = payload["data"]
            if not tick or not tick.instrument_id:
                return
            
            # 获取或创建该合约的K线生成器（双重检查锁定）
            instrument_id = tick.instrument_id
            
            # 🔒 第一次检查（无锁，快速路径）
            if instrument_id not in self.generators:
                # 🔒 加锁创建（慢速路径）
                with self._generators_lock:
                    # 🔒 第二次检查（持锁，防止重复创建）
                    if instrument_id not in self.generators:
                        self._create_generator(instrument_id)
            
            # 更新K线（无需持锁，生成器内部是线程安全的）
            self.generators[instrument_id].update_tick(tick)
        
        except Exception as e:
            self.logger.error(f"处理Tick事件失败: {e}", exc_info=True)
    
    def _create_generator(self, instrument_id: str) -> None:
        """
        为指定合约创建K线生成器
        
        Args:
            instrument_id: 合约代码
            
        注意：此方法必须在持有 self._generators_lock 的情况下调用
        """
        # 创建多周期K线生成器
        self.generators[instrument_id] = MultiBarGenerator(
            intervals=self.intervals,
            on_bar=self._on_bar_generated
        )
        
        # ⭐ 在添加到字典后统计数量（确保计数准确）
        current_count = len(self.generators)
        
        # 打印详细进度（使用INFO级别，便于在Web界面显示）
        self.logger.info(
            f"✓ 已为合约 [{instrument_id}] 创建多周期K线合成器 "
            f"(第 {current_count} 个合约) | "
            f"支持周期: {', '.join(self.intervals)}"
        )
    
    def _on_bar_generated(self, bar: BarData, interval: str) -> None:
        """
        K线生成完成的回调
        
        Args:
            bar: 完成的K线
            interval: K线周期
        """
        try:
            self.logger.debug(
                f"K线生成: {bar.instrument_id} {interval} "
                f"{bar.timestamp} C={bar.close_price:.2f} V={bar.volume}"
            )
            
            # 1. 发布BAR事件到事件总线
            self._publish_bar_event(bar, interval)
            
            # 2. 保存K线到存储层
            self._save_bar(bar, interval)
        
        except Exception as e:
            self.logger.error(f"K线生成回调处理失败: {e}", exc_info=True)
    
    def _publish_bar_event(self, bar: BarData, interval: str) -> None:
        """
        发布BAR事件到事件总线
        
        Args:
            bar: K线数据
            interval: K线周期
        """
        try:
            # 构建事件payload
            payload = PackPayload.success(
                message=f"K线生成: {bar.instrument_id} {interval}",
                data={
                    "bar": bar,
                    "interval": interval
                }
            )
            
            # 发布事件
            event = Event.bar(payload=payload, source=self.__class__.__name__)
            self.event_bus.publish(event)
        
        except Exception as e:
            self.logger.error(f"发布BAR事件失败: {e}", exc_info=True)
    
    def _save_bar(self, bar: BarData, interval: str) -> None:
        """
        保存K线到存储层
        
        Args:
            bar: K线数据
            interval: K线周期
        """
        try:
            # 转换为DataFrame
            df = self._bar_to_dataframe(bar, interval)
            
            # 保存到存储层（HybridStorage会自动路由到SQLite）
            if hasattr(self.storage, 'save_klines'):
                # HybridStorage接口
                self.storage.save_klines(df)
            else:
                # 兼容旧的DataStorage接口
                symbol_with_interval = f"{bar.instrument_id}_{interval}"
                date = bar.timestamp.strftime("%Y%m%d") if bar.timestamp else None
                self.storage.save_kline(symbol=symbol_with_interval, df=df, date=date)
        
        except Exception as e:
            self.logger.error(f"保存K线数据失败: {e}", exc_info=True)

    @staticmethod
    def _bar_to_dataframe(bar: BarData, interval: str) -> pd.DataFrame:
        """
        将BarData转换为DataFrame（精简版，包含13个核心字段，PascalCase命名）
        
        Args:
            bar: K线数据
            interval: K线周期
        
        Returns:
            DataFrame，包含以下字段（按顺序）：
            - BarType: K线类型/周期
            - TradingDay: 交易日（用于分库）
            - UpdateTime: 最后更新时间
            - InstrumentID: 合约代码
            - ExchangeID: 交易所代码
            - Volume: 成交量
            - OpenInterest: 持仓量
            - OpenPrice: 开盘价
            - HighestPrice: 最高价
            - LowestPrice: 最低价
            - ClosePrice: 收盘价
            - LastVolume: K线开始时的累计成交量
            - Timestamp: K线开始时间（完整datetime，用于时间范围查询）
        """
        return pd.DataFrame([{
            "BarType": bar.bar_type.value if bar.bar_type else interval,
            "TradingDay": bar.trading_day,
            "UpdateTime": bar.update_time,
            "InstrumentID": bar.instrument_id,
            "ExchangeID": bar.exchange_id.value if bar.exchange_id else "",
            "Volume": bar.volume,
            "OpenInterest": bar.open_interest,
            "OpenPrice": bar.open_price,
            "HighestPrice": bar.high_price,
            "LowestPrice": bar.low_price,
            "ClosePrice": bar.close_price,
            "LastVolume": bar.last_volume,
            "Timestamp": bar.timestamp,
        }])
    
    def get_generator(self, instrument_id: str) -> Optional[MultiBarGenerator]:
        """
        获取指定合约的K线生成器
        
        Args:
            instrument_id: 合约代码
        
        Returns:
            MultiBarGenerator 或 None
        """
        return self.generators.get(instrument_id)
    
    def get_current_bars(self, instrument_id: str) -> dict[str, Optional[BarData]]:
        """
        获取指定合约当前正在生成的所有周期K线
        
        Args:
            instrument_id: 合约代码
        
        Returns:
            {interval: bar} 字典
        """
        generator = self.generators.get(instrument_id)
        if generator:
            return generator.get_current_bars()
        return {}
    
    def get_all_generators(self) -> dict[str, MultiBarGenerator]:
        """获取所有合约的K线生成器（线程安全）"""
        with self._generators_lock:
            return self.generators.copy()
    
    def get_statistics(self) -> dict:
        """
        获取K线管理器统计信息（线程安全）
        
        Returns:
            统计信息字典
        """
        with self._generators_lock:
            return {
                "total_contracts": len(self.generators),
                "intervals": self.intervals,
                "contracts": list(self.generators.keys())
            }
    
    def stop(self) -> None:
        """
        停止 K线管理器（取消订阅事件）
        
        职责：
        - 取消订阅 TICK 事件
        - 停止接收新的 Tick 数据
        - 清理资源
        """
        self.logger.info("正在停止 BarManager...")
        
        try:
            # 取消订阅 TICK 事件
            self.event_bus.unsubscribe(EventType.TICK, self._on_tick)
            self.logger.info("✓ 已取消订阅 TICK 事件")
        except Exception as e:
            self.logger.error(f"取消订阅 TICK 事件失败: {e}")
        
        self.logger.info("✅ BarManager 已停止")
