#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : contract_manager.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 合约管理器 - 管理全市场合约列表，自动订阅全部合约
"""
import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from src.core.event_bus import EventBus
from src.core.event import Event, EventType
from src.core.constants import SubscribeAction, Exchange
from src.core.object import ContractData
from src.gateway.gateway_const import symbol_contract_map
from src.utils.log import get_logger


@dataclass
class ContractInfo:
    """合约信息"""
    instrument_id: str          # 合约代码
    exchange_id: str            # 交易所
    subscribed: bool = False    # 是否已订阅
    last_tick_time: Optional[str] = None  # 最后一次tick时间
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "instrument_id": self.instrument_id,
            "exchange_id": self.exchange_id,
            "subscribed": self.subscribed,
            "last_tick_time": self.last_tick_time
        }


class ContractManager:
    """
    合约管理器 - 自动订阅全部合约
    
    职责：
    1. 从 instrument_exchange.json 加载全部合约（800+个）
    2. 登录成功后自动订阅全部合约
    3. 跟踪订阅状态
    4. 提供合约信息查询
    """
    
    def __init__(self, 
                 event_bus: EventBus, 
                 config_path: Optional[Path] = None):
        """
        初始化合约管理器
        
        Args:
            event_bus: 事件总线
            config_path: 合约配置文件路径，默认为 config/instrument_exchange.json
        """
        self.event_bus = event_bus
        self.config_path = config_path or Path("config/instrument_exchange.json")
        self.logger = get_logger(self.__class__.__name__)
        
        # 合约列表（全部合约）
        # key: instrument_id, value: ContractInfo
        self.contracts: dict[str, ContractInfo] = {}
        
        # 已订阅的合约集合
        self.subscribed_symbols: set[str] = set()
        
        # 网关就绪状态管理
        self._md_gateway_ready = False      # 行情网关是否就绪
        self._td_gateway_ready = False      # 交易网关是否就绪（或超时）
        self._contract_file_ready = False   # 合约文件是否已更新完成
        self._subscription_triggered = False  # 是否已触发订阅（防止重复）
        self._gateway_ready_lock = threading.Lock()
        
        # 加载全部合约列表
        self._load_contracts()
        
        # 订阅行情网关登录成功事件
        self.event_bus.subscribe(EventType.MD_GATEWAY_LOGIN, self._on_md_gateway_login)
        
        # 订阅交易网关登录成功事件
        self.event_bus.subscribe(EventType.TD_GATEWAY_LOGIN, self._on_td_gateway_login)
        
        # 订阅合约文件更新完成事件（关键：只有在合约文件更新完成后才订阅行情）
        self.event_bus.subscribe(EventType.TD_QRY_INS, self._on_contract_file_updated)
        
        # 订阅Tick事件，用于更新合约最后tick时间
        self.event_bus.subscribe(EventType.TICK, self._on_tick)
        
        # 启动超时检查线程（最长等待60秒）
        self._start_timeout_checker()
    
    def _load_contracts(self) -> None:
        """从配置文件加载全部合约列表（同时填充全局symbol_contract_map）"""
        if not self.config_path.exists():
            self.logger.error(f"合约配置文件不存在: {self.config_path}")
            return
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # instrument_exchange.json 格式: {"instrument_id": "exchange_id", ...}
            for instrument_id, exchange_id in data.items():
                # 创建ContractInfo用于管理
                contract_info = ContractInfo(
                    instrument_id=instrument_id,
                    exchange_id=exchange_id
                )
                self.contracts[instrument_id] = contract_info
                
                # 同时填充全局symbol_contract_map（用于行情网关过滤）
                try:
                    exchange_enum = Exchange(exchange_id)
                    contract_data = ContractData(
                        instrument_id=instrument_id,
                        exchange_id=exchange_enum,
                        instrument_name=instrument_id  # 默认使用合约代码作为名称
                    )
                    symbol_contract_map[instrument_id] = contract_data
                except ValueError:
                    self.logger.warning(f"未知的交易所ID: {exchange_id}（合约: {instrument_id}），跳过填充到symbol_contract_map")
            
            self.logger.info(f"成功加载 {len(self.contracts)} 个合约（将全部订阅），已填充到symbol_contract_map: {len(symbol_contract_map)} 个")
        
        except Exception as e:
            self.logger.error(f"加载合约配置失败: {e}", exc_info=True)
    
    def _start_timeout_checker(self) -> None:
        """
        启动智能超时检查线程
        
        策略：
        - 每3秒检查一次状态
        - 最长等待60秒
        - 如果在60秒内所有条件满足，立即触发订阅
        - 如果60秒后仍有条件未满足，使用 fallback 并继续订阅：
          * 交易网关未就绪 → 使用系统日期
          * 合约文件未更新 → 使用现有合约列表
        """
        def timeout_worker():
            max_wait_time = 60  # 最长等待60秒
            check_interval = 3   # 每3秒检查一次
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                time.sleep(check_interval)
                elapsed_time += check_interval
                
                with self._gateway_ready_lock:
                    # 检查是否已经触发订阅（可能由交易网关登录成功触发）
                    if self._subscription_triggered:
                        self.logger.debug(
                            f"交易网关已就绪（等待{elapsed_time}秒），超时检查线程退出"
                        )
                        return  # 已经订阅，退出线程
                    
                    # 检查是否交易网关已就绪但行情网关未就绪（罕见情况）
                    if self._td_gateway_ready and not self._md_gateway_ready:
                        self.logger.debug(
                            f"交易网关已就绪但行情网关未就绪（已等待{elapsed_time}秒），继续等待..."
                        )
                        continue
                    
                    # 如果行情网关就绪但交易网关未就绪，每次循环打印等待日志
                    if self._md_gateway_ready and not self._td_gateway_ready:
                        self.logger.info(
                            f"等待交易网关登录... (已等待{elapsed_time}秒/最多60秒)"
                        )
            
            # 超时：60秒后仍未就绪
            with self._gateway_ready_lock:
                if not self._subscription_triggered:
                    # 超时后不再强制订阅，记录错误并退出
                    if not self._td_gateway_ready:
                        self.logger.error(
                            f"交易网关登录超时（{max_wait_time}秒），无法订阅行情"
                        )
                        self.logger.error(
                            "可能原因：1) 交易网关认证失败 2) 网络连接问题 3) CTP服务器无响应"
                        )
                    
                    if not self._contract_file_ready:
                        self.logger.error(
                            f"合约文件更新超时（{max_wait_time}秒），无法订阅行情"
                        )
                    
                    self.logger.error("请检查数据中心启动日志，修复交易网关问题后重新启动")
                else:
                    self.logger.debug("订阅已触发，超时检查线程正常退出")
        
        timeout_thread = threading.Thread(
            target=timeout_worker,
            name="ContractManager-TimeoutChecker",
            daemon=True
        )
        timeout_thread.start()
        self.logger.debug("智能超时检查线程已启动（每3秒检查一次，最长等待60秒）")
    
    def _on_md_gateway_login(self, event: Event) -> None:
        """
        行情网关登录成功回调
        
        Args:
            event: 登录事件
        """
        try:
            payload = event.payload
            if not payload or payload.get("code") != 0:
                self.logger.warning("行情网关登录失败，跳过订阅")
                return
            
            with self._gateway_ready_lock:
                self.logger.info("✓ 行情网关已就绪，等待交易网关...")
                self._md_gateway_ready = True
                self._check_and_subscribe()
        
        except Exception as e:
            self.logger.error(f"处理行情网关登录事件失败: {e}", exc_info=True)
    
    def _on_td_gateway_login(self, event: Event) -> None:
        """
        交易网关登录成功回调
        
        Args:
            event: 登录事件
        
        Note:
            仅标记交易网关就绪状态，等待合约文件更新完成后才触发订阅
        """
        try:
            payload = event.payload

            if payload and payload.get("code") == 0:
                # 登录成功，提取trading_day
                trading_day = payload.get("data", {}).get("trading_day", "未知")
                
                with self._gateway_ready_lock:
                    self.logger.info(f"✓ 交易网关登录成功，交易日: {trading_day}")
                    self.logger.info("等待结算单确认和合约文件更新...")
                    self._td_gateway_ready = True
                    # 🔥 关键修改：不在此处调用 _check_and_subscribe()，等待 TD_QRY_INS 事件
            else:
                # 登录失败，不影响行情订阅（使用系统日期fallback）
                self.logger.warning("交易网关登录失败，将使用系统日期")
                with self._gateway_ready_lock:
                    self._td_gateway_ready = True  # 标记为就绪（fallback）
                    # 🔥 关键修改：不在此处调用 _check_and_subscribe()，等待 TD_QRY_INS 事件
        
        except Exception as e:
            self.logger.error(f"处理交易网关登录事件失败: {e}", exc_info=True)
    
    def _on_contract_file_updated(self, event: Event) -> None:
        """
        处理合约文件更新完成事件 - 触发订阅
        
        Args:
            event: TD_QRY_INS 事件
        
        Note:
            只有在合约文件更新完成（code=0）后才标记就绪并触发订阅
        """
        try:
            payload = event.payload

            if payload and payload.get("code") == 0:
                # 合约文件更新成功
                with self._gateway_ready_lock:
                    self.logger.info("✓ 合约文件更新完成")
                    self._contract_file_ready = True
                    # 重新加载合约列表（确保使用最新的合约文件）
                    self._load_contracts()
                    self.logger.info(f"已重新加载合约列表，共 {len(self.contracts)} 个合约")
                    # 检查是否可以开始订阅
                    self._check_and_subscribe()
            else:
                # 合约文件更新失败，记录警告但仍然允许订阅（使用现有合约列表）
                error_msg = payload.get("message", "未知错误") if payload else "未知错误"
                self.logger.warning(f"合约文件更新失败: {error_msg}，将使用现有合约列表")
                with self._gateway_ready_lock:
                    self._contract_file_ready = True  # 标记为就绪（fallback）
                    self._check_and_subscribe()
        
        except Exception as e:
            self.logger.error(f"处理合约文件更新事件失败: {e}", exc_info=True)
    
    def _check_and_subscribe(self) -> None:
        """
        检查所有条件都满足后触发订阅（需持有锁）
        
        Note:
            此方法必须在持有 _gateway_ready_lock 的情况下调用
            
        条件：
            1. 行情网关就绪 (_md_gateway_ready)
            2. 交易网关就绪 (_td_gateway_ready)
            3. 合约文件已更新 (_contract_file_ready)
            4. 尚未触发订阅 (not _subscription_triggered)
        """
        if (self._md_gateway_ready and 
            self._td_gateway_ready and 
            self._contract_file_ready and 
            not self._subscription_triggered):
            self._subscription_triggered = True  # 防止重复订阅
            self.logger.info("=" * 60)
            self.logger.info("所有就绪条件已满足，开始订阅全部合约...")
            self.logger.info("=" * 60)
            self.subscribe_all()
    
    def _on_tick(self, event: Event) -> None:
        """
        处理Tick事件 - 更新合约最后tick时间
        
        Args:
            event: Tick事件
        """
        try:
            payload = event.payload
            if not payload or "data" not in payload:
                return
            
            tick = payload["data"]
            if not tick or not tick.instrument_id:
                return
            
            # 更新合约最后tick时间
            instrument_id = tick.instrument_id
            if instrument_id in self.contracts:
                self.contracts[instrument_id].last_tick_time = tick.update_time
        
        except Exception as e:
            self.logger.error(f"处理Tick事件失败: {e}", exc_info=True)
    
    def subscribe_all(self) -> None:
        """订阅全部合约"""
        # 获取所有合约代码
        all_symbols = list(self.contracts.keys())
        
        if not all_symbols:
            self.logger.warning("没有可订阅的合约")
            return
        
        self.logger.info(f"准备订阅全部 {len(all_symbols)} 个合约...")
        
        try:
            # 发布订阅请求事件
            self.event_bus.publish(Event(
                event_type=EventType.MARKET_SUBSCRIBE_REQUEST,
                payload={
                    "data": {
                        "instruments": all_symbols,
                        "action": SubscribeAction.SUBSCRIBE.value
                    }
                },
                source=self.__class__.__name__
            ))
            
            # 更新订阅状态
            for symbol in all_symbols:
                self.subscribed_symbols.add(symbol)
                self.contracts[symbol].subscribed = True
            
            self.logger.info(f"已发送全部 {len(all_symbols)} 个合约的订阅请求")
        
        except Exception as e:
            self.logger.error(f"订阅全部合约失败: {e}", exc_info=True)
    
    def get_contract(self, instrument_id: str) -> Optional[ContractInfo]:
        """
        获取合约信息
        
        Args:
            instrument_id: 合约代码
        
        Returns:
            ContractInfo 或 None
        """
        return self.contracts.get(instrument_id)
    
    def get_all_contracts(self) -> list[ContractInfo]:
        """获取所有合约信息"""
        return list(self.contracts.values())
    
    def get_subscribed_contracts(self) -> list[ContractInfo]:
        """获取已订阅的合约"""
        return [
            contract for contract in self.contracts.values()
            if contract.subscribed
        ]
    
    def is_subscribed(self, instrument_id: str) -> bool:
        """
        检查合约是否已订阅
        
        Args:
            instrument_id: 合约代码
        
        Returns:
            True: 已订阅, False: 未订阅
        """
        return instrument_id in self.subscribed_symbols
    
    def get_statistics(self) -> dict:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        active_contracts = [
            c for c in self.contracts.values()
            if c.last_tick_time is not None
        ]
        
        return {
            "total_contracts": len(self.contracts),
            "subscribed_contracts": len(self.subscribed_symbols),
            "active_contracts": len(active_contracts),
            "exchanges": list(set(c.exchange_id for c in self.contracts.values()))
        }
    
    def get_contracts_by_exchange(self, exchange_id: str) -> list[ContractInfo]:
        """
        按交易所获取合约列表
        
        Args:
            exchange_id: 交易所代码，如 "SHFE", "DCE", "CZCE", "CFFEX"
        
        Returns:
            合约列表
        """
        return [
            contract for contract in self.contracts.values()
            if contract.exchange_id == exchange_id
        ]
    
    def stop(self) -> None:
        """
        停止合约管理器（通过事件机制取消所有行情订阅）
        
        设计理念：
        - 通过事件总线发布取消订阅请求
        - 保持与启动时订阅机制的一致性
        - 解耦合约管理和网关控制
        """
        self.logger.info("正在停止 ContractManager...")
        
        # 1. 通过事件机制取消所有行情订阅
        subscribed_list = list(self.subscribed_symbols)
        if subscribed_list:
            self.logger.info(f"通过事件机制取消订阅 {len(subscribed_list)} 个合约...")
            
            try:
                # 发布取消订阅事件（使用与订阅相同的事件机制）
                unsubscribe_event = Event(
                    EventType.MARKET_SUBSCRIBE_REQUEST,
                    payload={
                        "data": {
                            "instruments": subscribed_list,
                            "action": SubscribeAction.UNSUBSCRIBE.value
                        }
                    },
                    source=self.__class__.__name__
                )
                
                self.event_bus.publish(unsubscribe_event)
                self.logger.info("✓ 已发布取消订阅事件，等待行情网关处理...")
                
                # 给网关一些时间处理取消订阅请求
                import time
                time.sleep(2)
                
                # 清空订阅列表
                self.subscribed_symbols.clear()
                self.logger.info("✓ 已清空本地订阅列表")
                
            except Exception as e:
                self.logger.error(f"取消订阅失败: {e}", exc_info=True)
        
        # 2. 取消订阅事件总线
        try:
            self.event_bus.unsubscribe(EventType.MD_GATEWAY_LOGIN, self._on_md_gateway_login)
            self.event_bus.unsubscribe(EventType.TD_GATEWAY_LOGIN, self._on_td_gateway_login)
            self.event_bus.unsubscribe(EventType.TD_QRY_INS, self._on_contract_file_updated)
            self.event_bus.unsubscribe(EventType.TICK, self._on_tick)
            self.logger.info("✓ 已取消订阅所有事件")
        except Exception as e:
            self.logger.error(f"取消订阅事件总线失败: {e}")
        
        self.logger.info("✅ ContractManager 已完全停止")
