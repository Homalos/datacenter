#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : bar_generator.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: K线合成器 - 从Tick数据实时合成K线
"""
from datetime import datetime
from typing import Optional, Callable

from src.core.object import TickData, BarData
from src.core.constants import Interval
from src.utils.log import get_logger


class BarGenerator:
    """
    K线合成器 - 从Tick数据实时合成指定周期的K线
    
    核心逻辑：
    1. 接收Tick数据
    2. 判断是否需要开启新K线（根据时间周期）
    3. 更新当前K线的OHLCV数据
    4. K线完成时触发回调
    """
    
    def __init__(self, 
                 interval: str, 
                 on_bar: Optional[Callable[[BarData], None]] = None):
        """
        初始化K线合成器
        
        Args:
            interval: K线周期，如 "1m", "5m", "15m", "30m", "1h", "1d"
            on_bar: K线完成时的回调函数
        """
        self.interval = interval
        self.on_bar = on_bar
        self.logger = get_logger(f"{self.__class__.__name__}[{interval}]")
        
        # 当前正在生成的K线
        self.current_bar: Optional[BarData] = None
        
        # 最后一个Tick（用于计算K线收盘价）
        self.last_tick: Optional[TickData] = None
        
        # 解析周期参数
        self.interval_minutes = self._parse_interval(interval)
        
        self.logger.debug(f"K线合成器初始化完成，周期: {interval}")
    
    def update_tick(self, tick: TickData) -> None:
        """
        更新Tick数据，生成K线
        
        Args:
            tick: 最新的Tick数据
        """
        if not tick or not tick.last_price:
            return
        
        # 1. 检查是否需要开启新K线
        if self._should_start_new_bar(tick):
            # 完成当前K线
            if self.current_bar:
                self._finish_current_bar()
            
            # 开启新K线
            self._start_new_bar(tick)
        
        # 2. 更新当前K线
        if self.current_bar:
            self._update_current_bar(tick)
        
        # 3. 保存最新tick
        self.last_tick = tick
    
    def _should_start_new_bar(self, tick: TickData) -> bool:
        """
        判断是否应该开启新K线
        
        Args:
            tick: 最新的Tick数据
        
        Returns:
            True: 需要开启新K线
            False: 继续更新当前K线
        """
        # 如果还没有当前K线，需要开启新K线
        if not self.current_bar:
            return True
        
        # 如果没有时间戳，无法判断
        if not tick.timestamp or not self.current_bar.timestamp:
            return False
        
        current_time = tick.timestamp
        bar_time = self.current_bar.timestamp
        
        # 根据不同周期判断是否开启新K线
        if self.interval.endswith('m'):
            # 分钟线：按分钟对齐
            current_minute_slot = self._get_minute_slot(current_time)
            bar_minute_slot = self._get_minute_slot(bar_time)
            return current_minute_slot != bar_minute_slot
        
        elif self.interval.endswith('h'):
            # 小时线：按小时对齐
            current_hour_slot = self._get_hour_slot(current_time)
            bar_hour_slot = self._get_hour_slot(bar_time)
            return current_hour_slot != bar_hour_slot
        
        elif self.interval == '1d':
            # 日线：按交易日对齐
            return tick.trading_day != self.current_bar.trading_day
        
        return False
    
    def _get_minute_slot(self, dt: datetime) -> int:
        """
        获取分钟时间槽（用于判断是否同一根K线）
        
        例如：5分钟线
        - 09:00:xx ~ 09:04:59 -> slot = 0
        - 09:05:xx ~ 09:09:59 -> slot = 1
        
        Args:
            dt: 时间戳
        
        Returns:
            时间槽编号
        """
        total_minutes = dt.hour * 60 + dt.minute
        return total_minutes // self.interval_minutes
    
    def _get_hour_slot(self, dt: datetime) -> int:
        """
        获取小时时间槽
        
        Args:
            dt: 时间戳
        
        Returns:
            时间槽编号
        """
        if self.interval == '1h':
            return dt.hour
        else:
            # 未来可以支持4h等
            return 0
    
    def _start_new_bar(self, tick: TickData) -> None:
        """
        开启新K线
        
        Args:
            tick: 触发新K线的Tick数据
        """
        # 标准化时间（对齐到周期开始时间）
        normalized_time = self._normalize_time(tick.timestamp)
        
        self.current_bar = BarData(
            source_name=tick.source_name,
            bar_type=Interval(self.interval),
            instrument_id=tick.instrument_id,
            exchange_id=tick.exchange_id,
            timestamp=normalized_time,
            trading_day=tick.trading_day,
            update_time=tick.update_time,
            open_price=tick.last_price,
            high_price=tick.last_price,
            low_price=tick.last_price,
            close_price=tick.last_price,
            volume=0,
            open_interest=tick.open_interest,
            last_volume=tick.volume  # 记录K线开始时的累计成交量
        )
        
        self.logger.debug(f"开启新K线: {tick.instrument_id} {normalized_time}")
    
    def _update_current_bar(self, tick: TickData) -> None:
        """
        更新当前K线
        
        Args:
            tick: 最新的Tick数据
        """
        if not self.current_bar:
            return
        
        bar = self.current_bar
        
        # 更新价格
        bar.high_price = max(bar.high_price, tick.last_price)
        bar.low_price = min(bar.low_price, tick.last_price)
        bar.close_price = tick.last_price
        
        # 更新成交量（增量计算：当前累计成交量 - K线开始时的成交量）
        bar.volume = tick.volume - bar.last_volume
        
        # 更新持仓量
        bar.open_interest = tick.open_interest
        
        # 更新时间
        bar.update_time = tick.update_time
    
    def _finish_current_bar(self) -> None:
        """完成当前K线，触发回调"""
        if not self.current_bar:
            return
        
        bar = self.current_bar
        
        self.logger.debug(
            f"完成K线: {bar.instrument_id} {bar.timestamp} "
            f"O={bar.open_price:.2f} H={bar.high_price:.2f} "
            f"L={bar.low_price:.2f} C={bar.close_price:.2f} V={bar.volume}"
        )
        
        # 触发回调
        if self.on_bar:
            try:
                self.on_bar(bar)
            except Exception as e:
                self.logger.error(f"K线回调执行失败: {e}", exc_info=True)
    
    def _normalize_time(self, dt: datetime) -> datetime:
        """
        标准化时间（对齐到周期开始时间）
        
        Args:
            dt: 原始时间
        
        Returns:
            标准化后的时间
        """
        if self.interval.endswith('m'):
            # 分钟线：对齐到周期开始的分钟
            minute_slot = self._get_minute_slot(dt)
            start_minute = minute_slot * self.interval_minutes
            hour = start_minute // 60
            minute = start_minute % 60
            return dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        elif self.interval.endswith('h'):
            # 小时线：对齐到小时开始
            return dt.replace(minute=0, second=0, microsecond=0)
        
        elif self.interval == '1d':
            # 日线：对齐到交易日开始（简化处理，使用09:00:00）
            return dt.replace(hour=9, minute=0, second=0, microsecond=0)
        
        return dt
    
    def _parse_interval(self, interval: str) -> int:
        """
        解析周期参数，转换为分钟数
        
        Args:
            interval: 周期字符串，如 "1m", "5m", "1h"
        
        Returns:
            分钟数
        """
        if interval.endswith('m'):
            return int(interval[:-1])
        elif interval.endswith('h'):
            return int(interval[:-1]) * 60
        elif interval == '1d':
            return 24 * 60
        else:
            raise ValueError(f"不支持的K线周期: {interval}")
    
    def get_current_bar(self) -> Optional[BarData]:
        """获取当前正在生成的K线"""
        return self.current_bar


class MultiBarGenerator:
    """
    多周期K线合成器管理器
    
    管理多个不同周期的K线生成器，一个Tick可以同时更新多个周期的K线
    """
    
    def __init__(self, 
                 intervals: list[str], 
                 on_bar: Optional[Callable[[BarData, str], None]] = None):
        """
        初始化多周期K线合成器
        
        Args:
            intervals: K线周期列表，如 ["1m", "5m", "15m", "30m", "1h", "1d"]
            on_bar: K线完成时的回调函数，签名: on_bar(bar: BarData, interval: str)
        """
        self.intervals = intervals
        self.on_bar = on_bar
        self.logger = get_logger(self.__class__.__name__)
        
        # 为每个周期创建一个生成器
        self.generators: dict[str, BarGenerator] = {}
        
        for interval in intervals:
            # 使用闭包捕获interval变量
            def create_callback(intv: str) -> Callable:
                def callback(bar: BarData) -> None:
                    self._on_bar_completed(bar, intv)
                return callback
            
            self.generators[interval] = BarGenerator(
                interval=interval,
                on_bar=create_callback(interval)
            )
        
        self.logger.info(f"多周期K线合成器初始化完成，周期: {intervals}")
    
    def update_tick(self, tick: TickData) -> None:
        """
        更新所有周期的K线生成器
        
        Args:
            tick: 最新的Tick数据
        """
        for generator in self.generators.values():
            generator.update_tick(tick)
    
    def _on_bar_completed(self, bar: BarData, interval: str) -> None:
        """
        K线完成的回调（内部）
        
        Args:
            bar: 完成的K线
            interval: K线周期
        """
        if self.on_bar:
            try:
                self.on_bar(bar, interval)
            except Exception as e:
                self.logger.error(f"K线回调执行失败: {e}", exc_info=True)
    
    def get_current_bars(self) -> dict[str, Optional[BarData]]:
        """
        获取所有周期当前正在生成的K线
        
        Returns:
            {interval: bar} 字典
        """
        return {
            interval: generator.get_current_bar()
            for interval, generator in self.generators.items()
        }

