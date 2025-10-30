#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : datacenter_starter.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据中心启动器 - 管理整个数据中心的生命周期
"""
import signal
import sys
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

from src.utils.log import get_logger


class ModuleState(Enum):
    """模块状态"""
    INIT = "init"           # 初始化
    STARTING = "starting"   # 正在启动
    RUNNING = "running"     # 运行中
    STOPPING = "stopping"   # 正在停止
    STOPPED = "stopped"     # 已停止
    ERROR = "error"         # 错误


@dataclass
class ModuleInfo:
    """模块信息"""
    name: str                                    # 模块名称
    instance: Any                                # 模块实例
    dependencies: List[str]                      # 依赖的模块列表
    start_func: Optional[Callable] = None        # 启动函数
    stop_func: Optional[Callable] = None         # 停止函数
    health_check_func: Optional[Callable] = None # 健康检查函数
    state: ModuleState = ModuleState.INIT        # 模块状态
    error_message: Optional[str] = None          # 错误信息


class DataCenterStarter:
    """
    数据中心启动器
    
    职责：
    1. 管理整个数据中心的生命周期
    2. 按依赖顺序初始化和启动模块
    3. 处理系统信号（SIGINT、SIGTERM）
    4. 优雅关闭所有模块
    5. 监控模块健康状态
    
    启动顺序（按依赖关系）：
    ```
    1. EventBus（事件总线）
    2. SQLiteStorage（SQLite存储）
    3. DataStorage（Parquet存储）
    4. HybridStorage（混合存储）
    5. MarketGateway（行情网关）
    6. ContractManager（合约管理器）
    7. BarManager（K线管理器）
    8. DataArchiver（数据归档器）
    9. AlarmScheduler（闹钟调度器）
    10. FastAPI Server（Web服务）
    ```
    """
    
    def __init__(self, register_signals: bool = True):
        """
        初始化数据中心启动器
        
        Args:
            register_signals: 是否注册信号处理器（在非主线程中应设为False）
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # 模块注册表
        # key: module_name, value: ModuleInfo
        self.modules: Dict[str, ModuleInfo] = {}
        
        # 模块启动顺序（拓扑排序后）
        self.startup_order: List[str] = []
        
        # 是否正在运行
        self.running = False
        
        # 注册系统信号处理（仅在主线程中）
        if register_signals:
            try:
                self._register_signal_handlers()
            except ValueError as e:
                self.logger.warning(f"无法注册信号处理器（可能不在主线程）: {e}")
        
        self.logger.info("数据中心启动器初始化完成")
    
    def register_module(self,
                        name: str,
                        instance: Any,
                        dependencies: Optional[List[str]] = None,
                        start_func: Optional[Callable] = None,
                        stop_func: Optional[Callable] = None,
                        health_check_func: Optional[Callable] = None) -> None:
        """
        注册模块
        
        Args:
            name: 模块名称
            instance: 模块实例
            dependencies: 依赖的模块列表
            start_func: 启动函数
            stop_func: 停止函数
            health_check_func: 健康检查函数
        """
        if name in self.modules:
            self.logger.warning(f"模块 {name} 已存在，将被覆盖")
        
        module_info = ModuleInfo(
            name=name,
            instance=instance,
            dependencies=dependencies or [],
            start_func=start_func,
            stop_func=stop_func,
            health_check_func=health_check_func
        )
        
        self.modules[name] = module_info
        self.logger.debug(f"注册模块: {name}, 依赖: {dependencies or '无'}")
    
    def start(self) -> bool:
        """
        启动数据中心
        
        Returns:
            True: 启动成功, False: 启动失败
        """
        self.logger.info("=" * 60)
        self.logger.info("开始启动数据中心...")
        self.logger.info("=" * 60)
        
        try:
            # 1. 计算启动顺序（拓扑排序）
            self._calculate_startup_order()
            self.logger.info(f"模块启动顺序: {' -> '.join(self.startup_order)}")
            
            # 2. 按顺序启动模块
            for module_name in self.startup_order:
                if not self._start_module(module_name):
                    self.logger.error(f"模块 {module_name} 启动失败，停止启动流程")
                    self._shutdown()
                    return False
            
            # 3. 标记为运行中
            self.running = True
            
            self.logger.info("=" * 60)
            self.logger.info("数据中心启动成功！")
            self.logger.info("=" * 60)
            
            return True
        
        except Exception as e:
            self.logger.error(f"数据中心启动失败: {e}", exc_info=True)
            self._shutdown()
            return False
    
    def _start_module(self, module_name: str) -> bool:
        """
        启动单个模块
        
        Args:
            module_name: 模块名称
        
        Returns:
            True: 启动成功, False: 启动失败
        """
        module = self.modules[module_name]
        
        try:
            self.logger.info(f"正在启动模块: {module_name}...")
            module.state = ModuleState.STARTING
            
            # 执行启动函数
            if module.start_func:
                module.start_func(module.instance)
            
            # 更新状态
            module.state = ModuleState.RUNNING
            self.logger.info(f"模块 {module_name} 启动成功 ✓")
            
            return True
        
        except Exception as e:
            module.state = ModuleState.ERROR
            module.error_message = str(e)
            self.logger.error(f"模块 {module_name} 启动失败: {e}", exc_info=True)
            return False
    
    def _calculate_startup_order(self) -> None:
        """
        计算模块启动顺序（拓扑排序）
        
        Raises:
            ValueError: 存在循环依赖
        """
        # 使用Kahn算法进行拓扑排序
        # 1. 计算每个模块的入度
        in_degree = {name: 0 for name in self.modules.keys()}
        
        for module in self.modules.values():
            for dep in module.dependencies:
                if dep not in self.modules:
                    raise ValueError(f"模块 {module.name} 依赖的模块 {dep} 不存在")
                in_degree[module.name] = in_degree.get(module.name, 0) + 1
        
        # 2. 找出所有入度为0的模块
        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []
        
        # 3. 拓扑排序
        while queue:
            # 取出一个入度为0的模块
            current = queue.pop(0)
            result.append(current)
            
            # 遍历所有模块，减少依赖于当前模块的入度
            for module in self.modules.values():
                if current in module.dependencies:
                    in_degree[module.name] -= 1
                    if in_degree[module.name] == 0:
                        queue.append(module.name)
        
        # 4. 检查是否有循环依赖
        if len(result) != len(self.modules):
            raise ValueError("模块之间存在循环依赖")
        
        self.startup_order = result
    
    def stop(self) -> None:
        """
        停止数据中心（公开接口）
        
        优雅地停止所有已启动的模块，按启动顺序的逆序执行
        """
        if not self.running:
            self.logger.warning("数据中心未在运行，无需停止")
            return
        
        self._shutdown()
    
    def _shutdown(self) -> None:
        """关闭数据中心（按启动顺序的逆序）"""
        self.logger.info("=" * 60)
        self.logger.info("开始关闭数据中心...")
        self.logger.info("=" * 60)
        
        # 按启动顺序的逆序停止模块
        for module_name in reversed(self.startup_order):
            self._stop_module(module_name)
        
        self.running = False
        
        self.logger.info("=" * 60)
        self.logger.info("数据中心已关闭")
        self.logger.info("=" * 60)
    
    def _stop_module(self, module_name: str) -> None:
        """
        停止单个模块
        
        Args:
            module_name: 模块名称
        """
        module = self.modules.get(module_name)
        if not module:
            return
        
        try:
            self.logger.info(f"正在停止模块: {module_name}...")
            module.state = ModuleState.STOPPING
            
            # 执行停止函数
            if module.stop_func:
                module.stop_func(module.instance)
            
            # 更新状态
            module.state = ModuleState.STOPPED
            self.logger.info(f"模块 {module_name} 已停止 ✓")
        
        except Exception as e:
            module.state = ModuleState.ERROR
            module.error_message = str(e)
            self.logger.error(f"模块 {module_name} 停止失败: {e}", exc_info=True)
    
    def _register_signal_handlers(self) -> None:
        """注册系统信号处理器"""
        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            self.logger.warning(f"接收到信号 {signal_name}，准备优雅关闭...")
            self._shutdown()
            sys.exit(0)
        
        # 注册SIGINT（Ctrl+C）和SIGTERM信号
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        self.logger.debug("系统信号处理器已注册（SIGINT, SIGTERM）")
    
    def get_module_status(self) -> Dict[str, str]:
        """
        获取所有模块状态
        
        Returns:
            {module_name: state}
        """
        return {
            name: module.state.value
            for name, module in self.modules.items()
        }
    
    def health_check(self) -> Dict[str, bool]:
        """
        执行所有模块的健康检查
        
        Returns:
            {module_name: is_healthy}
        """
        results = {}
        
        for name, module in self.modules.items():
            if module.health_check_func and module.state == ModuleState.RUNNING:
                try:
                    is_healthy = module.health_check_func(module.instance)
                    results[name] = is_healthy
                except Exception as e:
                    self.logger.error(f"模块 {name} 健康检查失败: {e}")
                    results[name] = False
            else:
                # 没有健康检查函数，根据状态判断
                results[name] = module.state == ModuleState.RUNNING
        
        return results
    
    def get_statistics(self) -> dict:
        """
        获取数据中心统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "running": self.running,
            "total_modules": len(self.modules),
            "running_modules": sum(1 for m in self.modules.values() if m.state == ModuleState.RUNNING),
            "error_modules": sum(1 for m in self.modules.values() if m.state == ModuleState.ERROR),
            "startup_order": self.startup_order,
            "module_states": self.get_module_status()
        }
