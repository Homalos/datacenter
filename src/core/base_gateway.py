#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : base_gateway.py
@Date       : 2025/9/9 16:11
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 抽象网关类用于外部系统连接。
每个网关都应该继承这个类，
并且应该有一个唯一的网关名称。
"""
from abc import ABC

from src.core.constants import RspCode
from src.core.event import Event
from src.core.event_bus import EventBus
from src.core.object import TickData, OrderData, PositionData, AccountData, ContractData


class BaseGateway(ABC):

    def __init__(
            self,
            event_bus: EventBus,
            gateway_name: str
    ) -> None:
        self.event_bus: EventBus = event_bus
        self.gateway_name: str = gateway_name  # 网关名称
        # 直接回调机制，绕过事件总线处理高频tick
        self.tick_callback = None

    def set_tick_callback(self, callback):
        """设置tick直接回调函数"""
        self.tick_callback = callback

    def on_tick(self, tick: TickData) -> None:
        """
        tick行情推送 - 使用事件总线作为主要机制
        :param tick:
        :return:
        """
        # 主要机制：使用事件总线确保数据完整性
        self.event_bus.publish(
            Event.tick(
                payload={
                    "code": RspCode.SUCCESS,
                    "message": "send on_tick success",
                    "data": tick
                }
        ))

    def on_bar(self, bar) -> None:
        """
        K线推送
        :param bar:
        :return:
        """
        self.event_bus.publish(
            Event.bar(
                payload={
                    "code": RspCode.SUCCESS,
                    "message": "send on_bar success",
                    "data": bar
                }
        ))

    def on_order(self, order: OrderData) -> None:
        """
        订单推送
        :param order:
        :return:
        """
        self.event_bus.publish(
            Event.order(
                payload={
                    "code": RspCode.SUCCESS,
                    "message": "send on_order success",
                    "data": order
                }
        ))

    def on_position(self, position: PositionData) -> None:
        """
        持仓推送
        :param position:
        :return:
        """
        self.event_bus.publish(
            Event.position(
                payload={
                    "code": RspCode.SUCCESS,
                    "message": "send on_position success",
                    "data": position
                }
        ))

    def on_account(self, account: AccountData) -> None:
        """
        账户推送
        :param account:
        :return:
        """
        self.event_bus.publish(
            Event.account(
                payload={
                    "code": RspCode.SUCCESS,
                    "message": "send on_account success",
                    "data": account
                }
        ))

    def on_contract(self, contract: ContractData) -> None:
        """
        合约推送
        :param contract:
        :return:
        """
        self.event_bus.publish(
            Event.contract(
                payload={
                    "code": RspCode.SUCCESS,
                    "message": "send on_contract success",
                    "data": contract
                }
            )
        )
