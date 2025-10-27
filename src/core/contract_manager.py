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
        
        # 加载全部合约列表
        self._load_contracts()
        
        # 订阅行情网关登录成功事件
        self.event_bus.subscribe(EventType.MD_GATEWAY_LOGIN, self._on_gateway_login)
        
        # 订阅Tick事件，用于更新合约最后tick时间
        self.event_bus.subscribe(EventType.TICK, self._on_tick)
    
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
    
    def _on_gateway_login(self, event: Event) -> None:
        """
        行情网关登录成功回调 - 自动订阅全部合约
        
        Args:
            event: 登录事件
        """
        try:
            payload = event.payload
            if not payload or payload.get("code") != 0:
                self.logger.warning("行情网关登录失败，跳过订阅")
                return
            
            self.logger.info("行情网关登录成功，开始订阅全部合约...")
            self.subscribe_all()
        
        except Exception as e:
            self.logger.error(f"处理登录事件失败: {e}", exc_info=True)
    
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
        
        except Exception:
            # Tick事件处理失败不记录日志，避免日志过多
            pass
    
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

