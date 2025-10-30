#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Homalos
@FileName   : system_config.py
@Date       : 2025/9/13 14:08
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 获取系统配置信息
"""
from src.constants import Const
from src.utils.config_manager import ConfigManager
from src.utils.get_path import get_path_ins
from src.utils.utility import load_yaml


class Config(object):
    """
    系统配置类
    """
    # 配置目录
    config_dir_path = get_path_ins.get_config_dir()
    # 系统配置文件
    _datacenter_config_path = config_dir_path / Const.DATA_CENTER_CONFIG_FILENAME
    # 开发配置文件
    _dev_config_path = config_dir_path / Const.DEV_CONFIG_FILENAME
    # 生产配置文件
    _prod_config_path = config_dir_path / Const.PROD_CONFIG_FILENAME

    # 系统配置文件使用 load_yaml 加载
    datacenter_config = load_yaml(str(_datacenter_config_path))

    # 项目名称
    system_name = datacenter_config.get("base", {}).get("name")
    system_describe = datacenter_config.get("base", {}).get("describe")
    system_version = datacenter_config.get("base", {}).get("version")
    timezone = datacenter_config.get("base", {}).get("timezone")
    enable = datacenter_config.get("base", {}).get("enable")
    debug = datacenter_config.get("base", {}).get("debug")

    # 如果是debug模式，则使用开发配置文件
    if debug:
        extra_config = ConfigManager(str(_dev_config_path))
    else:
        extra_config = ConfigManager(str(_prod_config_path))

    # 目录
    assets_dir_name = extra_config.get("base.assets_dir")   # 资产目录名
    flow_dir_name = extra_config.get("base.flow_dir")       # 流水目录名
    config_dir_name = extra_config.get("base.config_dir")   # 配置目录名
    data_dir_name = extra_config.get("base.data_dir")       # 数据目录名
    docs_dir_name = extra_config.get("base.docs_dir")       # 文档目录名
    i18n_dir_name = extra_config.get("base.i18n_dir")       # 国际化目录名
    logs_dir_name = extra_config.get("base.logs_dir")       # 日志目录名
    tests_dir_name = extra_config.get("base.tests_dir")     # 测试目录名

    # K线配置
    bar_intervals = extra_config.get("kline.bar_intervals", [])

    # 数据库
    database_type = extra_config.get("database.type")           # 数据库类型
    database_filename = extra_config.get("database.filename")   # 数据库文件名
    if database_type == "sqlite":
        database_path = f"sqlite+aiosqlite:///{get_path_ins.get_data_dir()}/{database_filename}"
    else:
        database_path = ""

    # 交易时间检查
    trading_hours_check = extra_config.get("trading_hours.enable_check")    # 交易时间检查
    futures = extra_config.get("trading_hours.futures")                     # 期货交易时间

    kline_dir_name = extra_config.get("data.kline_dir")     # K线数据目录名
    tick_dir_name = extra_config.get("data.tick_dir")       # Tick数据目录名
    trading_dir_name = extra_config.get("data.trading_dir")     # 交易时间数据目录名

    # 多源服务器节点配置文件
    brokers_filepath = config_dir_path / Const.BROKERS_FILENAME

    # 微信相关配置
    wx_app_name: str = extra_config.get("wx_app_name", "")
    wx_agent_id: int = extra_config.get("wx_agent_id", 0)
    wx_secret: str = extra_config.get("wx_secret", "")
    wx_corp_id: str = extra_config.get("wx_corp_id", "")
    notify_type: dict[str, bool] = extra_config.get("notify_type", {})
    url_wx_gettoken: str = extra_config.get("url_wx_gettoken", "")
    url_wx_media_upload: str = extra_config.get("url_wx_media_upload", "")
    url_wx_send: str = extra_config.get("url_wx_send", "")

    # 钉钉相关配置
    ding_app_name: str = extra_config.get("ding_app_name", "")
    ding_address: str = extra_config.get("ding_address", "")

    # ============================================================
    #  数据中心存储配置（HybridStorage + DuckDB + CSV）
    # ============================================================
    
    # Level 1: HybridStorage缓冲区配置
    storage_flush_interval: int = extra_config.get("datacenter_storage.level1.flush_interval", 60)
    storage_max_buffer_size: int = extra_config.get("datacenter_storage.level1.max_buffer_size", 100000)
    storage_buffer_warning_threshold: float = extra_config.get("datacenter_storage.level1.buffer_warning_threshold", 0.7)
    storage_buffer_flush_threshold: float = extra_config.get("datacenter_storage.level1.buffer_flush_threshold", 0.8)
    
    # Level 2: DuckDB存储配置
    duckdb_tick_batch_threshold: int = extra_config.get("datacenter_storage.duckdb.tick_batch_threshold", 30000)
    duckdb_kline_batch_threshold: int = extra_config.get("datacenter_storage.duckdb.kline_batch_threshold", 3000)
    duckdb_max_thread_lifetime: int = extra_config.get("datacenter_storage.duckdb.max_thread_lifetime", 300)
    duckdb_monitor_interval: int = extra_config.get("datacenter_storage.duckdb.monitor_interval", 10)
    
    # Level 2: CSV归档配置
    csv_tick_batch_threshold: int = extra_config.get("datacenter_storage.csv.tick_batch_threshold", 30000)
    csv_kline_batch_threshold: int = extra_config.get("datacenter_storage.csv.kline_batch_threshold", 3000)
    csv_num_threads: int = extra_config.get("datacenter_storage.csv.num_threads", 4)
    csv_queue_max_size: int = extra_config.get("datacenter_storage.csv.queue_max_size", 50000)


# 为了向后兼容，创建别名
DatacenterConfig = Config
