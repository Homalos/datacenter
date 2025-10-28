#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : constants.py
@Date       : 2025/9/9 17:10
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 全局常量
"""


class Const:
    # 交易日，全局变量
    trading_day: str = ""

    # ================== 项目中目录名称 ==================
    DATA_DIR_NAME = "data"  # 数据目录名

    CSV_DIR_NAME = "csv"  # CSV数据子目录名

    TICK_DIR_NAME = "ticks"  # TICK数据子目录名

    KLINE_DIR_NAME = "klines"  # K线数据子目录名

    RES_USAGE_DIR_NAME = "resource_usage"  # 资源占用文件目录名

    CONFIG_DIR_NAME = "config"  # 配置目录名

    # ================== 项目中文件名称 ==================
    BROKERS_FILENAME = "brokers.yaml"  # 多源服务器节点配置文件名

    DATA_CENTER_CONFIG_FILENAME = "data_center.yaml"

    DEV_CONFIG_FILENAME = "extra.dev.yaml"

    PROD_CONFIG_FILENAME = "extra.prod.yaml"

    LOG_CONFIG_FILENAME = "log_config.yaml"  # 全局日志配置文件名

    INSTRUMENT_EXCHANGE_FILENAME = "instrument_exchange.json"  # 期货合约与交易所映射信息文件名

    PRODUCT_INFO_FILENAME = "product_info.ini"  # 合约乘数及手续费信息文件名

    HOLIDAY_FILENAME = "holidays.json"  # 节假日文件名称

    BAIDU_YUN_FILES = "baidu_list.txt"

    # ================== 代码中常量 ==================
    filename_format = "%Y%m%d"  # 日志文件名格式

    log_time_format = "%Y-%m-%d %H:%M:%S.%f"  # 日志文件中时间格式

    print_time_format = "%Y-%m-%d %H:%M:%S.%f"  # 控制台打印的时间格式

    # tick合成K线系统
    tick_to_kline_sys = None

    # tick合成K线系统
    is_queue = True

    # 文件名称是否按照真实时间来
    file_time_is_true = True

    # 记录文件中时间是否按照真实时间来
    content_time_is_true = True

    # 表头
    pool_column = ['当前时间', "未完成任务数量"]

    # 在程序首次运行时是否抹除今天之前的记录
    is_first = False
