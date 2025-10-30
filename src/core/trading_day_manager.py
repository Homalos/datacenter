#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : trading_day_manager.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 交易日管理器 - 获取并管理当前交易日
"""
import threading
from datetime import datetime
from typing import Optional

from src.core.event import Event, EventType
from src.core.event_bus import EventBus
from src.utils.log import get_logger


class TradingDayManager(object):
    """
    交易日管理器
    
    功能：
    1. 订阅交易网关登录事件获取trading_day
    2. 提供全局的trading_day访问
    3. 支持回退到系统日期（如果交易网关未启动）
    """
    
    def __init__(self, event_bus: EventBus):
        """
        初始化交易日管理器
        
        Args:
            event_bus: 事件总线
        """
        self.logger = get_logger(self.__class__.__name__)
        self.event_bus = event_bus
        
        # 当前交易日
        self._trading_day: Optional[str] = None
        self._lock = threading.RLock()
        
        # 订阅交易网关登录事件
        self.event_bus.subscribe(EventType.TD_GATEWAY_LOGIN, self._on_td_gateway_login)
        
        self.logger.info("交易日管理器初始化完成")
    
    def _on_td_gateway_login(self, event: Event) -> None:
        """
        处理交易网关登录事件
        
        Args:
            event: 登录事件
        """
        try:
            payload = event.payload
            if payload.get("code") == 0:  # 登录成功
                data = payload.get("data", {})
                trading_day = data.get("trading_day")
                
                if trading_day:
                    with self._lock:
                        self._trading_day = trading_day
                    self.logger.info(f"✓ 已获取交易日: {trading_day}")
                else:
                    self.logger.warning("交易网关登录成功，但未获取到trading_day")
        
        except Exception as e:
            self.logger.error(f"处理交易网关登录事件失败: {e}", exc_info=True)
    
    def get_trading_day(self) -> str:
        """
        获取当前交易日
        
        Returns:
            交易日字符串（YYYYMMDD格式）
            
        Note:
            如果trading_day未设置（交易网关未登录），回退到系统日期
        """
        with self._lock:
            if self._trading_day:
                return self._trading_day
            else:
                # 回退到系统日期
                fallback_date = datetime.now().strftime("%Y%m%d")
                self.logger.warning(f"trading_day未设置，使用系统日期: {fallback_date}")
                return fallback_date
    
    def is_trading_day_available(self) -> bool:
        """
        检查trading_day是否已设置
        
        Returns:
            True表示已从交易网关获取到trading_day
        """
        with self._lock:
            return self._trading_day is not None
