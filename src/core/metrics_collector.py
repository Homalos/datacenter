#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : metrics_collector.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 监控指标采集器 - 采集系统和业务指标
"""
import psutil  # type: ignore
import time
from datetime import datetime
from typing import Dict, Optional, Deque
from collections import deque

from src.core.event_bus import EventBus
from src.core.event import Event, EventType
from src.utils.log import get_logger


class MetricsCollector:
    """
    监控指标采集器
    
    采集指标：
    1. 系统指标：CPU、内存、磁盘、网络IO
    2. 业务指标：Tick接收速率、事件队列积压、数据存储延迟、API请求QPS
    """
    
    def __init__(self, event_bus: EventBus, window_size: int = 60):
        """
        初始化监控指标采集器
        
        Args:
            event_bus: 事件总线
            window_size: 滑动窗口大小（秒），用于计算速率
        """
        self.event_bus = event_bus
        self.window_size = window_size
        self.logger = get_logger(self.__class__.__name__)
        
        # 系统指标
        self.cpu_percent = 0.0
        self.memory_percent = 0.0
        self.disk_usage_percent = 0.0
        self.network_io_sent_mb = 0.0
        self.network_io_recv_mb = 0.0
        
        # 业务指标 - Tick接收
        self.tick_count = 0
        self.tick_timestamps: Deque[float] = deque(maxlen=window_size * 10)  # 最多保存window_size*10个时间戳
        
        # 业务指标 - K线生成
        self.bar_count = 0
        self.bar_timestamps: Deque[float] = deque(maxlen=window_size * 10)
        
        # 业务指标 - 事件队列
        self.market_queue_size = 0
        self.general_queue_size = 0
        
        # 业务指标 - API请求
        self.api_request_count = 0
        self.api_request_timestamps: Deque[float] = deque(maxlen=window_size * 10)
        
        # 订阅事件以收集业务指标
        self.event_bus.subscribe(EventType.TICK, self._on_tick)
        self.event_bus.subscribe(EventType.BAR, self._on_bar)
        
        # 上次采集时间
        self.last_collect_time: Optional[datetime] = None
        
        self.logger.info("监控指标采集器初始化完成")
    
    def collect_system_metrics(self) -> Dict:
        """
        采集系统指标
        
        Returns:
            系统指标字典
        """
        try:
            # CPU使用率
            self.cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # 内存使用率
            memory = psutil.virtual_memory()
            self.memory_percent = memory.percent
            
            # 磁盘使用率
            disk = psutil.disk_usage('/')
            self.disk_usage_percent = disk.percent
            
            # 网络IO
            net_io = psutil.net_io_counters()
            self.network_io_sent_mb = net_io.bytes_sent / (1024 * 1024)
            self.network_io_recv_mb = net_io.bytes_recv / (1024 * 1024)
            
            return {
                "cpu_percent": self.cpu_percent,
                "memory_percent": self.memory_percent,
                "memory_used_mb": memory.used / (1024 * 1024),
                "memory_total_mb": memory.total / (1024 * 1024),
                "disk_usage_percent": self.disk_usage_percent,
                "disk_used_gb": disk.used / (1024 * 1024 * 1024),
                "disk_total_gb": disk.total / (1024 * 1024 * 1024),
                "network_sent_mb": self.network_io_sent_mb,
                "network_recv_mb": self.network_io_recv_mb
            }
        
        except Exception as e:
            self.logger.error(f"采集系统指标失败: {e}", exc_info=True)
            return {}
    
    def collect_business_metrics(self) -> Dict:
        """
        采集业务指标
        
        Returns:
            业务指标字典
        """
        try:
            now = time.time()
            
            return {
                "tick_count": self.tick_count,
                "tick_rate_per_second": self._calculate_rate(self.tick_timestamps, now),
                "bar_count": self.bar_count,
                "bar_rate_per_minute": self._calculate_rate(self.bar_timestamps, now) * 60,
                "market_queue_size": self.market_queue_size,
                "general_queue_size": self.general_queue_size,
                "api_request_count": self.api_request_count,
                "api_qps": self._calculate_rate(self.api_request_timestamps, now)
            }
        
        except Exception as e:
            self.logger.error(f"采集业务指标失败: {e}", exc_info=True)
            return {}
    
    def _calculate_rate(self, timestamps: Deque[float], now: float) -> float:
        """
        计算速率（事件/秒）
        
        Args:
            timestamps: 时间戳队列
            now: 当前时间戳
        
        Returns:
            速率
        """
        if not timestamps:
            return 0.0
        
        # 过滤掉超过窗口期的时间戳
        cutoff_time = now - self.window_size
        valid_timestamps = [ts for ts in timestamps if ts >= cutoff_time]
        
        if not valid_timestamps:
            return 0.0
        
        # 计算速率
        count = len(valid_timestamps)
        time_span = now - valid_timestamps[0] if len(valid_timestamps) > 1 else 1.0
        
        return count / max(time_span, 1.0)
    
    def _on_tick(self, event: Event) -> None:
        """
        Tick事件回调 - 记录Tick接收统计
        
        Args:
            event: Tick事件
        """
        self.logger.debug(f"Tick事件回调: {event.event_type}")
        try:
            self.tick_count += 1
            self.tick_timestamps.append(time.time())
        except Exception as e:
            self.logger.exception(f"记录Tick接收统计失败: {e}", exc_info=True)
    
    def _on_bar(self, event: Event) -> None:
        """
        K线事件回调 - 记录K线生成统计
        
        Args:
            event: K线事件
        """
        self.logger.debug(f"K线事件回调: {event.event_type}")
        try:
            self.bar_count += 1
            self.bar_timestamps.append(time.time())
        except Exception as e:
            self.logger.exception(f"记录K线生成统计失败: {e}", exc_info=True)
    
    def record_api_request(self) -> None:
        """记录API请求"""
        self.api_request_count += 1
        self.api_request_timestamps.append(time.time())
    
    def update_queue_sizes(self, market_queue_size: int, general_queue_size: int) -> None:
        """
        更新事件队列大小
        
        Args:
            market_queue_size: 行情队列大小
            general_queue_size: 通用队列大小
        """
        self.market_queue_size = market_queue_size
        self.general_queue_size = general_queue_size
    
    def collect_all_metrics(self) -> Dict:
        """
        采集所有指标
        
        Returns:
            所有指标字典
        """
        self.last_collect_time = datetime.now()
        
        return {
            "timestamp": self.last_collect_time.isoformat(),
            "system": self.collect_system_metrics(),
            "business": self.collect_business_metrics()
        }
    
    def get_summary(self) -> Dict:
        """
        获取指标摘要（用于快速查看）
        
        Returns:
            指标摘要
        """
        metrics = self.collect_all_metrics()
        
        return {
            "timestamp": metrics["timestamp"],
            "cpu_percent": metrics["system"].get("cpu_percent", 0),
            "memory_percent": metrics["system"].get("memory_percent", 0),
            "tick_rate": metrics["business"].get("tick_rate_per_second", 0),
            "bar_rate": metrics["business"].get("bar_rate_per_minute", 0),
            "queue_size": {
                "market": metrics["business"].get("market_queue_size", 0),
                "general": metrics["business"].get("general_queue_size", 0)
            }
        }
    
    def check_health(self) -> Dict[str, bool]:
        """
        健康检查 - 判断系统是否正常
        
        Returns:
            健康状态字典
        """
        health = {
            "cpu_ok": self.cpu_percent < 90,
            "memory_ok": self.memory_percent < 90,
            "disk_ok": self.disk_usage_percent < 90,
            "market_queue_ok": self.market_queue_size < 5000,
            "general_queue_ok": self.general_queue_size < 5000
        }
        
        health["overall_healthy"] = all(health.values())
        
        return health
    
    def reset_counters(self) -> None:
        """重置计数器（用于测试或统计重置）"""
        self.tick_count = 0
        self.bar_count = 0
        self.api_request_count = 0
        self.tick_timestamps.clear()
        self.bar_timestamps.clear()
        self.api_request_timestamps.clear()
        self.logger.info("指标计数器已重置")
    
    def get_statistics(self) -> Dict:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "window_size_seconds": self.window_size,
            "tick_total": self.tick_count,
            "bar_total": self.bar_count,
            "api_request_total": self.api_request_count,
            "last_collect_time": self.last_collect_time.isoformat() if self.last_collect_time else None
        }

