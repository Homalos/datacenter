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
from typing import Optional, Dict, Any, Callable, List
from enum import Enum
from dataclasses import dataclass, asdict, field
from datetime import datetime

from config import settings

from src.constants import Const
from src.core.event import Event, EventType
from src.core.event_bus import EventBus
from src.core.storage import DataStorage
from src.core.hybrid_storage import HybridStorage
from src.core.bar_manager import BarManager
from src.core.contract_manager import ContractManager
from src.core.datacenter_starter import DataCenterStarter
from src.core.alarm_scheduler import AlarmScheduler, create_default_tasks
from src.core.metrics_collector import MetricsCollector
from src.core.trading_day_manager import TradingDayManager
from src.gateway.market_gateway import MarketGateway
from src.gateway.trader_gateway import TraderGateway
from src.system_config import DatacenterConfig
from src.utils.common import load_md_broker_config, load_td_broker_config
from src.utils.get_path import get_path_ins
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
        self.alarm_scheduler: Optional[AlarmScheduler] = None
        self.metrics_collector: Optional[MetricsCollector] = None
        
        # 交易网关状态标志（用于严格控制启动顺序）
        self._td_login_status = False
        self._td_confirm_status = False
        self._contract_file_updated = False
        
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
    
    def _handle_td_confirm(self, event: Event):
        """
        处理结算单确认事件
        
        Args:
            event: TD_CONFIRM_SUCCESS 事件
        """
        data = event.payload
        if data and data.get("code") == 0:
            self._td_login_status = True
            self._td_confirm_status = True
            self.logger.info("结算单确认成功，交易网关完全就绪")
            self._add_log("INFO", "结算单确认成功，交易网关完全就绪")
            
            # 发送查询合约事件，触发合约文件更新
            if self.event_bus:
                self.logger.info("发布查询合约事件，开始更新合约文件...")
                self._add_log("INFO", "发布查询合约事件，开始更新合约文件...")
                self.event_bus.publish(Event(
                    event_type=EventType.DATA_CENTER_QRY_INS,
                    payload={},
                    source="DataCenterService"
                ))
        else:
            self._td_login_status = False
            self._td_confirm_status = False
            error_msg = f"结算单确认失败: {data.get('message') if data else 'Unknown'}"
            self.logger.error(error_msg)
            self._add_log("ERROR", error_msg)
    
    def _handle_td_qry_ins(self, event: Event):
        """
        处理合约查询完成事件
        
        Args:
            event: TD_QRY_INS 事件
        """
        data = event.payload
        if data and data.get("code") == 0:
            self._contract_file_updated = True
            self.logger.info("合约文件更新完成")
            self._add_log("INFO", "合约文件更新完成")
        else:
            self._contract_file_updated = False
            error_msg = f"合约文件更新失败: {data.get('message') if data else 'Unknown'}"
            self.logger.warning(error_msg)
            self._add_log("WARNING", error_msg)
    
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
            
            # CSV 存储（使用trading_day_manager，用于历史数据归档）
            self.csv_storage = DataStorage(
                base_path="data",
                trading_day_manager=self.trading_day_manager
            )
            self.starter.register_module(
                name="CSVStorage",
                instance=self.csv_storage,
                dependencies=[]
            )
            self._update_module_status("CSVStorage", "registered")
            
            # 混合存储（订阅 TICK 事件自动保存数据）
            # 初始化混合存储（DuckDB + CSV双层存储）
            self.hybrid_storage = HybridStorage(
                event_bus=self.event_bus,  # 传入事件总线，自动订阅 TICK 事件
                parquet_tick_path=settings.TICK_PATH,  # Tick数据CSV归档路径 (data/csv/ticks)
                parquet_kline_path=settings.KLINE_PATH,  # K线数据CSV归档路径 (data/csv/klines)
                retention_days=7,  # 保留天数（用于未来的查询分层）
                flush_interval=60,  # 定时刷新间隔（秒）
                max_buffer_size=100000,  # 缓冲区上限提高到10万（减少IO频率）
                buffer_warning_threshold=0.7,  # 警告阈值（70%）
                buffer_flush_threshold=0.85,  # 提前刷新阈值（85%）
                trading_day_manager=self.trading_day_manager  # 传入交易日管理器
            )
            self.starter.register_module(
                name="HybridStorage",
                instance=self.hybrid_storage,
                dependencies=["CSVStorage"],
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
                    broker_config = load_md_broker_config()
                    if not broker_config:
                        self._add_log("ERROR", "未找到行情网关配置")
                        raise ValueError("未找到行情网关配置")
                    
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
                            self._add_log("INFO", f"行情网关 {broker_name} 登录成功")
                            login_success[0] = True
                        else:
                            # code!=0 表示登录失败
                            error_msg = payload.get("message", "未知错误")
                            self._add_log("ERROR", f"行情网关 {broker_name} 登录失败: {error_msg}")
                        
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
                                time.sleep(0.2)  # 短暂等待，确保其他订阅者也处理了事件
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
                """
                启动交易网关并登录（用于获取trading_day和合约信息）

                Args:
                    gateway:

                Returns:

                """
                try:
                    broker_config = load_td_broker_config()
                    if not broker_config:
                        self._add_log("WARNING", "未找到交易网关配置，跳过启动")
                        return
                    
                    broker_name = broker_config.get("broker_name", "未知")
                    config = broker_config.get("config")
                    
                    # 创建登录完成事件
                    login_event = threading.Event()
                    login_success = [False]
                    login_error_code = [0]  # 保存错误代码
                    login_error_msg = [""]  # 保存错误消息
                    
                    # 创建结算单确认完成事件
                    confirm_event = threading.Event()
                    confirm_success = [False]
                    
                    # 创建合约文件更新完成事件
                    contract_update_event = threading.Event()
                    contract_update_success = [False]
                    
                    def on_td_login(event: Event):
                        """
                        监听 TD_GATEWAY_LOGIN 事件

                        Args:
                            event:

                        Returns:

                        """
                        payload = event.payload or {}
                        code = payload.get("code")

                        if code == 0:
                            # 登录成功
                            trading_day = payload.get("data", {}).get("trading_day", "未知")
                            self._add_log("INFO", f"交易网关 {broker_name} 登录成功，交易日: {trading_day}")
                            login_success[0] = True
                        else:
                            # 登录失败或认证失败
                            err_msg = payload.get("message", "未知错误")
                            login_error_code[0] = code
                            login_error_msg[0] = err_msg
                            
                            if code == 7002:
                                # 认证失败（致命错误）
                                self._add_log("ERROR", f"交易网关 {broker_name} 认证失败: {err_msg}")
                                self.logger.error(f"检测到认证失败，code={code}")
                                error_detail = payload.get("data", {}).get("error", "")
                                if error_detail:
                                    self._add_log("ERROR", f"详细错误: {error_detail}")
                            else:
                                # 登录失败
                                self._add_log("ERROR", f"交易网关 {broker_name} 登录失败: {err_msg}")
                                self.logger.error(f"检测到登录失败，code={code}")
                        
                        # 设置事件，结束等待
                        self.logger.info(f"设置 login_event")
                        login_event.set()
                    
                    def on_td_confirm(event: Event):
                        """
                        监听 TD_CONFIRM_SUCCESS 事件

                        Args:
                            event:

                        Returns:

                        """
                        payload = event.payload or {}
                        if payload.get("code") == 0:
                            self._add_log("INFO", "结算单确认成功，交易网关完全就绪")
                            confirm_success[0] = True
                            self._td_confirm_status = True
                            
                            # 关键步骤：发布查询合约事件，触发合约文件更新
                            self._add_log("INFO", "发布查询合约事件，开始更新合约文件...")
                            self.event_bus.publish(Event(
                                event_type=EventType.DATA_CENTER_QRY_INS,
                                payload={},
                                source="DataCenterService"
                            ))
                        else:
                            err_msg = payload.get("message", "未知错误")
                            self._add_log("WARNING", f"结算单确认失败: {err_msg}")
                            self._td_confirm_status = False
                        
                        confirm_event.set()
                    
                    def on_td_qry_ins(event: Event):
                        """
                        监听 TD_QRY_INS 事件（合约文件更新完成）

                        Args:
                            event:

                        Returns:

                        """
                        payload = event.payload or {}
                        if payload.get("code") == 0:
                            self._add_log("INFO", "合约文件更新完成，可以开始订阅行情")
                            contract_update_success[0] = True
                        else:
                            err_msg = payload.get("message", "未知错误")
                            self._add_log("WARNING", f"合约文件更新失败: {err_msg}")
                        
                        contract_update_event.set()
                    
                    # 订阅事件（在连接前订阅，确保不会错过事件）
                    self.event_bus.subscribe(EventType.TD_GATEWAY_LOGIN, on_td_login)
                    self.event_bus.subscribe(EventType.TD_CONFIRM_SUCCESS, on_td_confirm)
                    self.event_bus.subscribe(EventType.TD_QRY_INS, on_td_qry_ins)
                    
                    try:
                        self._add_log("INFO", f"连接交易网关: {broker_name}...")
                        gateway.connect(config)
                        
                        # 步骤1: 等待登录完成
                        max_wait_login = 10
                        self._add_log("INFO", "等待交易网关登录...")
                        self.logger.info(f"[DEBUG] 开始等待 login_event, 超时={max_wait_login}秒")
                        
                        if login_event.wait(timeout=max_wait_login):
                            self.logger.info(f"[DEBUG] login_event 已触发, login_success={login_success[0]}")
                            if not login_success[0]:
                                # 登录失败，抛出异常阻止后续启动
                                error_code = login_error_code[0]
                                error_msg = login_error_msg[0]
                                self.logger.error(f"[DEBUG] 准备抛出异常: error_code={error_code}, error_msg={error_msg}")
                                
                                if error_code == 7002:
                                    # 认证失败
                                    error_message = f"交易网关认证失败: {error_msg}，请检查配置文件中的认证信息"
                                    self._add_log("ERROR", error_message)
                                    raise RuntimeError(error_message)
                                else:
                                    # 登录失败
                                    error_message = f"交易网关登录失败: {error_msg}，请检查配置文件中的登录信息"
                                    self._add_log("ERROR", error_message)
                                    raise RuntimeError(error_message)
                        else:
                            self.logger.error(f"[DEBUG] login_event 超时")
                            self._add_log("ERROR", f"交易网关登录超时（{max_wait_login}秒）")
                            raise RuntimeError(f"交易网关登录超时（{max_wait_login}秒），请检查网络连接和CTP服务器状态")
                        
                        # 步骤2: 等待结算单确认
                        max_wait_confirm = 10
                        self._add_log("INFO", "等待结算单确认...")
                        if confirm_event.wait(timeout=max_wait_confirm):
                            if not confirm_success[0]:
                                self._add_log("WARNING", "结算单确认失败，跳过合约文件更新")
                                return
                        else:
                            self._add_log("WARNING", f"结算单确认超时（{max_wait_confirm}秒）")
                            return
                        
                        # 步骤3: 等待合约文件更新完成
                        max_wait_contract = 60  # 合约查询可能需要更长时间
                        self._add_log("INFO", "等待合约文件更新...")
                        if contract_update_event.wait(timeout=max_wait_contract):
                            if contract_update_success[0]:
                                self._add_log("INFO", "交易网关完全就绪，合约文件已更新")
                                self._contract_file_updated = True
                                time.sleep(0.2)  # 短暂等待，确保其他订阅者处理完毕
                            else:
                                self._add_log("WARNING", "合约文件更新失败")
                        else:
                            self._add_log("WARNING", f"合约文件更新超时（{max_wait_contract}秒）")
                    
                    finally:
                        # 清理：取消订阅
                        self.event_bus.unsubscribe(EventType.TD_GATEWAY_LOGIN, on_td_login)
                        self.event_bus.unsubscribe(EventType.TD_CONFIRM_SUCCESS, on_td_confirm)
                        self.event_bus.unsubscribe(EventType.TD_QRY_INS, on_td_qry_ins)
                
                except RuntimeError as err:
                    # 认证失败、登录失败、超时等致命错误，重新抛出异常阻止启动
                    self.logger.error(f"[DEBUG] 捕获到 RuntimeError: {err}")
                    self._add_log("ERROR", f"交易网关启动失败: {err}")
                    self.logger.error(f"[DEBUG] 重新抛出 RuntimeError")
                    raise
                except Exception as err:
                    # 其他非预期异常，记录警告但允许系统继续运行
                    self.logger.warning(f"[DEBUG] 捕获到其他异常: {type(err).__name__}: {err}")
                    self._add_log("WARNING", f"交易网关启动异常: {err}，将使用系统日期")
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
                config_path=get_path_ins.get_config_dir() / Const.INSTRUMENT_EXCHANGE_FILENAME
            )
            self.starter.register_module(
                name="ContractManager",
                instance=self.contract_manager,
                dependencies=["EventBus", "MarketGateway"],
                stop_func=lambda cm: cm.stop()  # 添加停止函数，确保取消订阅
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
                dependencies=["EventBus", "HybridStorage"],
                stop_func=lambda bm: bm.stop()  # 添加停止函数，确保取消订阅
            )
            self._update_module_status("BarManager", "registered")
            
            # 8. 创建定时任务调度器
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
            
            # 9. 启动所有模块
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
            
            # 10. 标记为运行中
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
            # HybridStorage 的 stop() 方法会自动刷新缓冲区
            if self.starter:
                self._add_log("INFO", "停止所有模块...")
                self.starter.stop()
                self._add_log("INFO", "所有模块已停止")
            
            # 等待启动线程结束
            if self._start_thread and self._start_thread.is_alive():
                self._add_log("INFO", "等待启动线程结束...")
                self._start_thread.join(timeout=5)
                if self._start_thread.is_alive():
                    self._add_log("WARNING", "启动线程未在超时时间内结束")
            
            self._add_log("INFO", "数据中心已停止")
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

