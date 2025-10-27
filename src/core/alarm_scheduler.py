#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : alarm_scheduler.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 闹钟调度器 - 根据配置的时间点触发特定任务
"""
from datetime import datetime, time
from typing import List, Dict, Callable, Optional
from dataclasses import dataclass

from src.core.event_bus import EventBus
from src.core.event import Event, EventType
from src.utils.log import get_logger


@dataclass
class AlarmTask:
    """闹钟任务"""
    name: str                    # 任务名称
    time_points: List[time]      # 触发时间点列表
    callback: Callable           # 回调函数
    enabled: bool = True         # 是否启用
    last_trigger_date: Optional[str] = None  # 最后触发日期（防止重复触发）


class AlarmScheduler:
    """
    闹钟调度器
    
    职责：
    1. 根据配置的时间点触发特定任务
    2. 支持每日定时任务
    3. 防止同一天内重复触发
    
    内置任务时间点：
    - 08:45/20:45: 登录CTP服务器
    - 08:46/20:46: 创建K线文件夹
    - 08:50/20:50: 开盘前检测
    - 15:30/23:30: 收盘后处理
    - 02:00: 数据归档（每日凌晨）
    """
    
    def __init__(self, event_bus: EventBus):
        """
        初始化闹钟调度器
        
        Args:
            event_bus: 事件总线
        """
        self.event_bus = event_bus
        self.logger = get_logger(self.__class__.__name__)
        
        # 闹钟任务列表
        self.tasks: Dict[str, AlarmTask] = {}
        
        # 订阅定时器事件
        self.event_bus.subscribe(EventType.TIMER, self._on_timer)
        
        self.logger.info("闹钟调度器初始化完成")
    
    def register_task(self,
                      name: str,
                      time_points: List[time],
                      callback: Callable,
                      enabled: bool = True) -> None:
        """
        注册闹钟任务
        
        Args:
            name: 任务名称
            time_points: 触发时间点列表，如 [time(8, 45), time(20, 45)]
            callback: 回调函数
            enabled: 是否启用
        """
        task = AlarmTask(
            name=name,
            time_points=time_points,
            callback=callback,
            enabled=enabled
        )
        
        self.tasks[name] = task
        
        time_str = ", ".join([t.strftime("%H:%M") for t in time_points])
        self.logger.info(f"注册闹钟任务: {name}, 时间点: {time_str}")
    
    def enable_task(self, name: str) -> None:
        """
        启用任务
        
        Args:
            name: 任务名称
        """
        if name in self.tasks:
            self.tasks[name].enabled = True
            self.logger.info(f"启用闹钟任务: {name}")
    
    def disable_task(self, name: str) -> None:
        """
        禁用任务
        
        Args:
            name: 任务名称
        """
        if name in self.tasks:
            self.tasks[name].enabled = False
            self.logger.info(f"禁用闹钟任务: {name}")
    
    def _on_timer(self, event: Event) -> None:
        """
        定时器事件回调 - 检查是否有任务需要触发
        
        Args:
            event: 定时器事件
        """
        try:
            now = datetime.now()
            current_time = now.time()
            current_date = now.strftime("%Y-%m-%d")
            
            # 遍历所有任务，检查是否需要触发
            for task in self.tasks.values():
                if not task.enabled:
                    continue
                
                # 检查是否匹配任何时间点
                for time_point in task.time_points:
                    if self._is_time_match(current_time, time_point):
                        # 检查今天是否已经触发过
                        if task.last_trigger_date == current_date:
                            continue
                        
                        # 触发任务
                        self._trigger_task(task, time_point)
                        
                        # 更新最后触发日期
                        task.last_trigger_date = current_date
        
        except Exception as e:
            self.logger.error(f"定时器事件处理失败: {e}", exc_info=True)
    
    def _is_time_match(self, current: time, target: time) -> bool:
        """
        检查当前时间是否匹配目标时间（精确到分钟）
        
        Args:
            current: 当前时间
            target: 目标时间
        
        Returns:
            True: 匹配, False: 不匹配
        """
        return (current.hour == target.hour and 
                current.minute == target.minute)
    
    def _trigger_task(self, task: AlarmTask, time_point: time) -> None:
        """
        触发任务
        
        Args:
            task: 闹钟任务
            time_point: 触发时间点
        """
        try:
            self.logger.info(
                f"触发闹钟任务: {task.name}, 时间: {time_point.strftime('%H:%M')}"
            )
            
            # 执行回调函数
            task.callback()
            
            self.logger.info(f"闹钟任务 {task.name} 执行完成")
        
        except Exception as e:
            self.logger.error(f"闹钟任务 {task.name} 执行失败: {e}", exc_info=True)
    
    def get_tasks(self) -> List[Dict]:
        """
        获取所有任务信息
        
        Returns:
            任务信息列表
        """
        return [
            {
                "name": task.name,
                "time_points": [t.strftime("%H:%M") for t in task.time_points],
                "enabled": task.enabled,
                "last_trigger_date": task.last_trigger_date
            }
            for task in self.tasks.values()
        ]
    
    def get_statistics(self) -> dict:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "total_tasks": len(self.tasks),
            "enabled_tasks": sum(1 for t in self.tasks.values() if t.enabled),
            "disabled_tasks": sum(1 for t in self.tasks.values() if not t.enabled)
        }


def create_default_tasks(alarm_scheduler: AlarmScheduler,
                          event_bus: EventBus) -> None:
    """
    创建默认的闹钟任务
    
    Args:
        alarm_scheduler: 闹钟调度器实例
        event_bus: 事件总线实例
    """
    logger = get_logger("AlarmScheduler.Default")
    
    # 任务1: 登录时间（日盘、夜盘）
    def task_login():
        logger.info("触发登录任务")
        event_bus.publish(Event(
            event_type=EventType.MD_GATEWAY_LOGIN_REQUEST,
            payload={"message": "定时登录"},
            source="AlarmScheduler"
        ))
    
    alarm_scheduler.register_task(
        name="login",
        time_points=[time(8, 45), time(20, 45)],
        callback=task_login
    )
    
    # 任务2: 创建K线文件夹
    def task_create_folders():
        logger.info("触发创建K线文件夹任务")
        # TODO: 实现创建文件夹逻辑
    
    alarm_scheduler.register_task(
        name="create_folders",
        time_points=[time(8, 46), time(20, 46)],
        callback=task_create_folders
    )
    
    # 任务3: 开盘前检测
    def task_pre_market_check():
        logger.info("触发开盘前检测任务")
        # TODO: 实现开盘前检测逻辑（检查连接、订阅状态等）
    
    alarm_scheduler.register_task(
        name="pre_market_check",
        time_points=[time(8, 50), time(20, 50)],
        callback=task_pre_market_check
    )
    
    # 任务4: 收盘后处理
    def task_post_market_process():
        logger.info("触发收盘后处理任务")
        # TODO: 实现收盘后处理逻辑（数据统计、生成报告等）
    
    alarm_scheduler.register_task(
        name="post_market_process",
        time_points=[time(15, 30), time(23, 30)],
        callback=task_post_market_process
    )
    
    # 任务5: 数据归档（每日凌晨2:00）
    def task_data_archive():
        logger.info("触发数据归档任务")
        # 发布归档事件，由DataArchiver处理
        event_bus.publish(Event(
            event_type=EventType.ALARM,
            payload={
                "alarm_type": "data_archive",
                "message": "定时数据归档"
            },
            source="AlarmScheduler"
        ))
    
    alarm_scheduler.register_task(
        name="data_archive",
        time_points=[time(2, 0)],
        callback=task_data_archive
    )
    
    logger.info("默认闹钟任务创建完成")

