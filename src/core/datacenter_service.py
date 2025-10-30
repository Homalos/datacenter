#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : datacenter_service.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据中心服务类 - 支持运行时启动/停止/状态查询
"""
from __future__ import annotations

import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any, Callable, List
from enum import Enum
from dataclasses import dataclass, asdict, field
from datetime import datetime

from config import settings
from src.core.event import Event, EventType
from src.core.event_bus import EventBus
from src.core.storage import DataStorage
from src.core.sqlite_storage import SQLiteStorage
from src.core.hybrid_storage import HybridStorage
from src.core.bar_manager import BarManager
from src.core.contract_manager import ContractManager
from src.core.data_archiver import DataArchiver
from src.core.datacenter_starter import DataCenterStarter
from src.core.alarm_scheduler import AlarmScheduler, create_default_tasks
from src.core.metrics_collector import MetricsCollector
from src.core.trading_day_manager import TradingDayManager
from src.gateway.market_gateway import MarketGateway
from src.gateway.trader_gateway import TraderGateway
from src.system_config import DatacenterConfig
from src.utils.common import load_broker_config
from src.utils.log import get_logger


class ServiceStatus(str, Enum):
    """服务状态枚举"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class ModuleStatus:
    """模块状态"""
    name: str
    status: str  # pending/starting/running/error
    started_at: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class ServiceState:
    """服务状态"""
    status: ServiceStatus
    start_time: Optional[str] = None
    uptime_seconds: int = 0
    modules: Dict[str, ModuleStatus] = field(default_factory=dict)
    error_message: Optional[str] = None
    last_update: Optional[str] = None
    
    def to_dict(self) -> dict:
        """转换为字典"""
        data = asdict(self)  # type: ignore
        if self.modules:
            data['modules'] = {k: asdict(v) for k, v in self.modules.items()}  # type: ignore
        return data


class DataCenterService:
    """
    数据中心服务
    
    功能：
    1. 支持运行时启动/停止
    2. 提供状态查询
    3. 模块级别的控制
    4. 日志收集和推送
    """
    
    def __init__(self):
        """初始化服务"""
        self.logger = get_logger(self.__class__.__name__)
        
        # 服务状态
        self._state = ServiceState(
            status=ServiceStatus.STOPPED,
            last_update=datetime.now().isoformat()
        )
        self._state_lock = threading.RLock()
        
        # 核心组件
        self.starter: Optional[DataCenterStarter] = None
        self.event_bus: Optional[EventBus] = None
        self.trading_day_manager: Optional[TradingDayManager] = None
        self.market_gateway: Optional[MarketGateway] = None
        self.trader_gateway: Optional[TraderGateway] = None
        self.hybrid_storage: Optional[HybridStorage] = None
        self.contract_manager: Optional[ContractManager] = None
        self.bar_manager: Optional[BarManager] = None
        self.data_archiver: Optional[DataArchiver] = None
        self.alarm_scheduler: Optional[AlarmScheduler] = None
        self.metrics_collector: Optional[MetricsCollector] = None
        
        # 日志收集器（用于Web界面展示）
        self._log_buffer: List[Dict[str, Any]] = []
        self._max_log_size = 1000
        self._log_callbacks: List[Callable] = []
        
        # 启动线程
        self._start_thread: Optional[threading.Thread] = None
        
        # 记录初始化完成（同时输出到日志和Web界面）
        self.logger.info("数据中心服务初始化完成")
        self._add_log("INFO", "数据中心服务初始化完成")
    
    def _update_state(self, **kwargs):
        """更新服务状态"""
        with self._state_lock:
            for key, value in kwargs.items():
                if hasattr(self._state, key):
                    setattr(self._state, key, value)
            self._state.last_update = datetime.now().isoformat()
            
            # 注意：运行时长现在在 get_state() 中实时计算，无需在此处更新
    
    def _add_log(self, level: str, message: str, **extra):
        """添加日志"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message,
            **extra
        }
        
        # 添加到缓冲区
        self._log_buffer.append(log_entry)
        if len(self._log_buffer) > self._max_log_size:
            self._log_buffer.pop(0)
        
        # 通知回调
        for callback in self._log_callbacks:
            try:
                callback(log_entry)
            except Exception as e:
                self.logger.error(f"日志回调失败: {e}")
    
    def add_log_callback(self, callback: Callable):
        """添加日志回调（用于实时推送）"""
        self._log_callbacks.append(callback)
    
    def remove_log_callback(self, callback: Callable):
        """移除日志回调"""
        if callback in self._log_callbacks:
            self._log_callbacks.remove(callback)
    
    def get_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取最近的日志"""
        return self._log_buffer[-limit:]
    
    def get_state(self) -> ServiceState:
        """获取当前状态（实时计算运行时长）"""
        with self._state_lock:
            # 深拷贝状态，避免修改原始对象
            state_copy = ServiceState(
                status=self._state.status,
                start_time=self._state.start_time,
                uptime_seconds=self._state.uptime_seconds,
                modules=self._state.modules.copy(),
                error_message=self._state.error_message,
                last_update=datetime.now().isoformat()
            )
            
            # 实时计算运行时长
            if state_copy.start_time and state_copy.status == ServiceStatus.RUNNING:
                try:
                    start_dt = datetime.fromisoformat(state_copy.start_time)
                    state_copy.uptime_seconds = int((datetime.now() - start_dt).total_seconds())
                except Exception as e:
                    msg = f"计算运行时长失败: {e}"
                    self.logger.error(msg)
                    # 注：此错误极少发生，记录到Web界面供诊断
                    self._add_log("ERROR", msg)
            
            return state_copy
    
    def get_state_dict(self) -> dict:
        """获取状态字典"""
        return self.get_state().to_dict()
    
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._state.status == ServiceStatus.RUNNING
    
    def is_starting(self) -> bool:
        """是否正在启动"""
        return self._state.status == ServiceStatus.STARTING
    
    def start(self) -> bool:
        """
        启动数据中心
        
        Returns:
            True: 启动命令已接受, False: 无法启动
        """
        with self._state_lock:
            if self._state.status in [ServiceStatus.RUNNING, ServiceStatus.STARTING]:
                msg = f"数据中心已在运行或启动中，当前状态: {self._state.status}"
                self.logger.warning(msg)
                self._add_log("WARNING", msg)
                return False
            
            # 更新状态为启动中
            self._update_state(status=ServiceStatus.STARTING)
            self._add_log("INFO", "开始启动数据中心...")
        
        # 在新线程中启动，避免阻塞API调用
        self._start_thread = threading.Thread(target=self._start_internal, daemon=False)
        self._start_thread.start()
        
        return True
    
    def _start_internal(self):
        """内部启动逻辑"""
        try:
            self._add_log("INFO", "=" * 60)
            self._add_log("INFO", "Homalos 数据中心启动流程开始")
            self._add_log("INFO", "=" * 60)
            
            # 1. 创建启动器（禁用信号处理器，因为在非主线程）
            self._add_log("INFO", "创建数据中心启动器...")
            self.starter = DataCenterStarter(register_signals=False)
            
            # 2. 创建 EventBus
            self._add_log("INFO", "初始化事件总线...")
            self.event_bus = EventBus()
            self.starter.register_module(
                name="EventBus",
                instance=self.event_bus,
                dependencies=[],
                start_func=lambda eb: eb.start(),
                stop_func=lambda eb: eb.stop()
            )
            self._update_module_status("EventBus", "registered")
            
            # 2.5 创建交易日管理器
            self._add_log("INFO", "初始化交易日管理器...")
            self.trading_day_manager = TradingDayManager(event_bus=self.event_bus)
            self.starter.register_module(
                name="TradingDayManager",
                instance=self.trading_day_manager,
                dependencies=["EventBus"]
            )
            self._update_module_status("TradingDayManager", "registered")
            
            # 3. 创建存储层
            self._add_log("INFO", "初始化存储层...")
            
            # Parquet 存储（使用trading_day_manager）
            self.parquet_storage = DataStorage(
                base_path="data",
                trading_day_manager=self.trading_day_manager
            )
            self.starter.register_module(
                name="ParquetStorage",
                instance=self.parquet_storage,
                dependencies=[]
            )
            self._update_module_status("ParquetStorage", "registered")
            
            # SQLite 存储（按交易日+合约分库）
            self.sqlite_storage = SQLiteStorage(
                db_path="data/db",
                retention_days=7,
                trading_day_manager=self.trading_day_manager
            )
            self.starter.register_module(
                name="SQLiteStorage",
                instance=self.sqlite_storage,
                dependencies=[],
                stop_func=lambda storage: storage.stop()
            )
            self._update_module_status("SQLiteStorage", "registered")
            
            # 混合存储（订阅 TICK 事件自动保存数据）
            # 🔥 初始化混合存储（DuckDB + CSV双层存储）
            self.hybrid_storage = HybridStorage(
                event_bus=self.event_bus,  # 传入事件总线，自动订阅 TICK 事件
                parquet_tick_path=settings.TICK_PATH,  # Tick数据CSV归档路径 (data/csv/ticks)
                parquet_kline_path=settings.KLINE_PATH,  # K线数据CSV归档路径 (data/csv/klines)
                retention_days=7,  # 保留天数（用于未来的查询分层）
                flush_interval=60,  # 定时刷新间隔（秒）
                max_buffer_size=100000,  # 🔥 缓冲区上限提高到10万（减少IO频率）
                buffer_warning_threshold=0.7,  # 警告阈值（70%）
                buffer_flush_threshold=0.85,  # 提前刷新阈值（85%）
                trading_day_manager=self.trading_day_manager  # 传入交易日管理器
            )
            self.starter.register_module(
                name="HybridStorage",
                instance=self.hybrid_storage,
                dependencies=["SQLiteStorage", "ParquetStorage"],
                stop_func=lambda storage: storage.stop()
            )
            self._update_module_status("HybridStorage", "registered")
            
            # 4. 创建指标收集器
            self._add_log("INFO", "初始化监控指标收集器...")
            self.metrics_collector = MetricsCollector(
                event_bus=self.event_bus,
                window_size=60
            )
            self.starter.register_module(
                name="MetricsCollector",
                instance=self.metrics_collector,
                dependencies=["EventBus"]
            )
            self._update_module_status("MetricsCollector", "registered")
            
            # 5. 创建行情网关
            self._add_log("INFO", "初始化行情网关...")
            self.market_gateway = MarketGateway(event_bus=self.event_bus)
            
            def start_market_gateway(gateway):
                """启动行情网关（使用事件机制判断登录状态）"""
                try:
                    broker_config = load_broker_config()
                    if not broker_config:
                        self._add_log("ERROR", "未找到CTP服务器配置")
                        raise ValueError("未找到CTP服务器配置")
                    
                    broker_name = broker_config.get("broker_name")
                    config = broker_config.get("config")
                    
                    # 创建登录完成事件（用于等待登录结果）
                    login_event = threading.Event()
                    login_success = [False]  # 使用列表避免闭包变量赋值问题
                    
                    def on_login(event: Event):
                        """登录事件回调 - 监听 MD_GATEWAY_LOGIN 事件"""
                        payload = event.payload or {}
                        if payload.get("code") == 0:
                            # code=0 表示登录成功
                            self._add_log("INFO", f"✓ 行情网关 {broker_name} 登录成功")
                            login_success[0] = True
                        else:
                            # code!=0 表示登录失败
                            error_msg = payload.get("message", "未知错误")
                            self._add_log("ERROR", f"✗ 行情网关 {broker_name} 登录失败: {error_msg}")
                        
                        # 无论成功或失败，都设置事件，结束等待
                        login_event.set()
                    
                    # 订阅登录事件（在连接前订阅，确保不会错过事件）
                    self.event_bus.subscribe(EventType.MD_GATEWAY_LOGIN, on_login)
                    
                    try:
                        self._add_log("INFO", f"连接行情网关: {broker_name}...")
                        gateway.connect(config)
                        
                        # 等待登录完成（使用事件机制）
                        max_wait = 10
                        if login_event.wait(timeout=max_wait):
                            # 事件已触发，检查登录是否成功
                            if login_success[0]:
                                # 登录成功！
                                # ContractManager 也会收到 MD_GATEWAY_LOGIN 事件并自动订阅合约
                                time.sleep(0.5)  # 短暂等待，确保其他订阅者也处理了事件
                            else:
                                # 登录失败
                                raise RuntimeError("行情网关登录失败")
                        else:
                            # 超时：没有收到登录事件
                            self._add_log("WARNING", f"行情网关登录超时（{max_wait}秒）")
                            self._add_log("WARNING", "可能原因：网络连接问题或CTP服务器无响应")
                    
                    finally:
                        # 清理：取消订阅登录事件（避免内存泄漏）
                        self.event_bus.unsubscribe(EventType.MD_GATEWAY_LOGIN, on_login)
                
                except Exception as err:
                    self._add_log("ERROR", f"行情网关启动失败: {err}")
                    raise
            
            self.starter.register_module(
                name="MarketGateway",
                instance=self.market_gateway,
                dependencies=["EventBus"],
                start_func=start_market_gateway,
                stop_func=lambda g: g.close()
            )
            self._update_module_status("MarketGateway", "registered")
            
            # 6. 启动交易网关（用于获取trading_day）
            self._add_log("INFO", "初始化交易网关...")
            self.trader_gateway = TraderGateway(event_bus=self.event_bus)
            
            def start_trader_gateway(gateway):
                """启动交易网关并登录（用于获取trading_day）"""
                try:
                    broker_config = load_broker_config()
                    if not broker_config:
                        self._add_log("WARNING", "未找到交易网关配置，跳过启动")
                        return
                    
                    broker_name = broker_config.get("broker_name", "未知")
                    config = broker_config.get("config")
                    
                    # 创建登录完成事件
                    login_event = threading.Event()
                    login_success = [False]
                    
                    def on_td_login(event: Event):
                        """监听 TD_GATEWAY_LOGIN 事件"""
                        payload = event.payload or {}
                        if payload.get("code") == 0:
                            # 登录成功
                            trading_day = payload.get("data", {}).get("TradingDay", "未知")
                            self._add_log("INFO", f"✓ 交易网关 {broker_name} 登录成功，交易日: {trading_day}")
                            login_success[0] = True
                        else:
                            # 登录失败
                            error_msg = payload.get("message", "未知错误")
                            self._add_log("WARNING", f"✗ 交易网关 {broker_name} 登录失败: {error_msg}")
                        
                        # 设置事件，结束等待
                        login_event.set()
                    
                    # 订阅登录事件
                    self.event_bus.subscribe(EventType.TD_GATEWAY_LOGIN, on_td_login)
                    
                    try:
                        self._add_log("INFO", f"连接交易网关: {broker_name}...")
                        gateway.connect(config)
                        
                        # 等待登录完成
                        max_wait = 10
                        if login_event.wait(timeout=max_wait):
                            if login_success[0]:
                                # 登录成功，TradingDayManager已接收到trading_day
                                self._add_log("INFO", "✓ 交易网关已就绪，trading_day已更新")
                                time.sleep(0.5)  # 短暂等待，确保其他订阅者处理完毕
                            else:
                                # 登录失败，使用系统日期作为fallback
                                self._add_log("WARNING", "交易网关登录失败，将使用系统日期作为trading_day")
                        else:
                            # 超时
                            self._add_log("WARNING", f"交易网关登录超时（{max_wait}秒），将使用系统日期")
                    
                    finally:
                        # 清理：取消订阅
                        self.event_bus.unsubscribe(EventType.TD_GATEWAY_LOGIN, on_td_login)
                
                except Exception as err:
                    self._add_log("WARNING", f"交易网关启动失败: {err}，将使用系统日期")
                    # 不抛出异常，允许系统继续运行（使用系统日期作为fallback）
            
            self.starter.register_module(
                name="TraderGateway",
                instance=self.trader_gateway,
                dependencies=["EventBus"],
                start_func=start_trader_gateway
                # 注：交易网关设计为非必需，start_func中捕获异常但不抛出
            )
            self._update_module_status("TraderGateway", "registered")
            
            # 7. 创建合约管理器
            self._add_log("INFO", "初始化合约管理器...")
            self.contract_manager = ContractManager(
                event_bus=self.event_bus,
                config_path=Path("config/instrument_exchange.json")
            )
            self.starter.register_module(
                name="ContractManager",
                instance=self.contract_manager,
                dependencies=["EventBus", "MarketGateway"]
            )
            self._update_module_status("ContractManager", "registered")
            
            # 7. 创建K线管理器
            self._add_log("INFO", "初始化K线管理器...")
            intervals = DatacenterConfig.bar_intervals or ["1m", "5m", "15m", "30m", "1h", "1d"]
            self.bar_manager = BarManager(
                event_bus=self.event_bus,
                storage=self.hybrid_storage,
                intervals=intervals
            )
            self.starter.register_module(
                name="BarManager",
                instance=self.bar_manager,
                dependencies=["EventBus", "HybridStorage"]
            )
            self._update_module_status("BarManager", "registered")
            
            # 8. 创建数据归档器
            self._add_log("INFO", "初始化数据归档器...")
            self.data_archiver = DataArchiver(
                event_bus=self.event_bus,
                sqlite_storage=self.sqlite_storage,
                parquet_storage=self.parquet_storage,
                retention_days=7
            )
            self.starter.register_module(
                name="DataArchiver",
                instance=self.data_archiver,
                dependencies=["EventBus", "SQLiteStorage", "ParquetStorage"]
            )
            self._update_module_status("DataArchiver", "registered")
            
            # 9. 创建定时任务调度器
            self._add_log("INFO", "初始化定时任务调度器...")
            self.alarm_scheduler = AlarmScheduler(event_bus=self.event_bus)
            
            # 创建默认任务（传入所需参数）
            create_default_tasks(
                alarm_scheduler=self.alarm_scheduler,
                event_bus=self.event_bus
            )
            
            # 注册 AlarmScheduler（需要 stop_func 来清理事件订阅）
            self.starter.register_module(
                name="AlarmScheduler",
                instance=self.alarm_scheduler,
                dependencies=["EventBus"],
                stop_func=lambda scheduler: scheduler.stop()
            )
            self._update_module_status("AlarmScheduler", "registered")
            
            # 10. 启动所有模块
            self._add_log("INFO", "=" * 60)
            self._add_log("INFO", "开始启动所有注册的模块...")
            self._add_log("INFO", "=" * 60)
            
            if not self.starter.start():
                self._add_log("ERROR", "数据中心启动失败")
                self._update_state(
                    status=ServiceStatus.ERROR,
                    error_message="模块启动失败"
                )
                return
            
            # 11. 标记为运行中
            self._add_log("INFO", "=" * 60)
            self._add_log("INFO", "🎉 数据中心启动成功！")
            self._add_log("INFO", "=" * 60)
            
            self._update_state(
                status=ServiceStatus.RUNNING,
                start_time=datetime.now().isoformat(),
                error_message=None
            )
            
        except Exception as e:
            self.logger.error(f"数据中心启动异常: {e}", exc_info=True)
            self._add_log("ERROR", f"启动异常: {e}")
            self._update_state(
                status=ServiceStatus.ERROR,
                error_message=str(e)
            )
    
    def stop(self) -> bool:
        """
        停止数据中心
        
        Returns:
            True: 停止命令已接受, False: 无法停止
        """
        with self._state_lock:
            if self._state.status != ServiceStatus.RUNNING:
                msg = f"数据中心未运行，当前状态: {self._state.status}"
                self.logger.warning(msg)
                self._add_log("WARNING", msg)
                return False
            
            self._update_state(status=ServiceStatus.STOPPING)
            self._add_log("INFO", "开始停止数据中心...")
        
        # 在新线程中停止
        threading.Thread(target=self._stop_internal, daemon=False).start()
        return True
    
    def _stop_internal(self):
        """内部停止逻辑"""
        try:
            # 停止所有模块（按启动顺序的逆序，确保依赖关系正确）
            # HybridStorage 和 SQLiteStorage 的 stop() 方法会自动刷新缓冲区
            if self.starter:
                self._add_log("INFO", "停止所有模块...")
                self.starter.stop()
                self._add_log("INFO", "✓ 所有模块已停止")
            
            # 等待启动线程结束
            if self._start_thread and self._start_thread.is_alive():
                self._add_log("INFO", "等待启动线程结束...")
                self._start_thread.join(timeout=5)
                if self._start_thread.is_alive():
                    self._add_log("WARNING", "启动线程未在超时时间内结束")
            
            self._add_log("INFO", "✓ 数据中心已停止")
            self._update_state(
                status=ServiceStatus.STOPPED,
                start_time=None,
                uptime_seconds=0
            )
            
            # 清理资源
            self.starter = None
            self.event_bus = None
            self.market_gateway = None
            self.trader_gateway = None
            self.hybrid_storage = None
            
        except Exception as e:
            self.logger.error(f"停止数据中心异常: {e}", exc_info=True)
            self._add_log("ERROR", f"停止异常: {e}")
            self._update_state(
                status=ServiceStatus.ERROR,
                error_message=str(e)
            )
    
    def restart(self) -> bool:
        """重启数据中心"""
        self._add_log("INFO", "执行重启操作...")
        if self.is_running():
            self.stop()
            # 等待停止完成
            max_wait = 10
            waited = 0.0
            while self._state.status == ServiceStatus.STOPPING and waited < max_wait:
                time.sleep(0.5)
                waited += 0.5
        
        return self.start()
    
    def _update_module_status(
        self, 
        module_name: str, 
        status: str, 
        error: Optional[str] = None
        ):
        """更新模块状态"""
        with self._state_lock:
            if module_name not in self._state.modules:
                self._state.modules[module_name] = ModuleStatus(
                    name=module_name,
                    status=status
                )
            else:
                self._state.modules[module_name].status = status
            
            if status == "running":
                self._state.modules[module_name].started_at = datetime.now().isoformat()
            
            if error:
                self._state.modules[module_name].error_message = error

