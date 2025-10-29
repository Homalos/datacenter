#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : start_datacenter.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据中心启动脚本 - 完整集成所有模块，支持Web管理
"""
import sys
import threading
import uvicorn
from pathlib import Path

from config import settings
from src.api import server
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
from src.gateway.market_gateway import MarketGateway
from src.system_config import DatacenterConfig
from src.utils.common import load_broker_config
from src.utils.log import get_logger

logger = get_logger(__name__)


def create_and_start_datacenter():
    """创建并启动数据中心"""
    
    logger.info("=" * 80)
    logger.info("Homalos 数据中心启动中...")
    logger.info("=" * 80)
    
    # 1. 创建数据中心启动器
    starter = DataCenterStarter()
    
    # 3. 创建并注册EventBus（最基础的模块，无依赖）
    event_bus = EventBus()
    starter.register_module(
        name="EventBus",
        instance=event_bus,
        dependencies=[],
        start_func=lambda eb: eb.start(),
        stop_func=lambda eb: eb.stop()
    )
    
    # 4. 创建并注册SQLiteStorage（依赖：无）
    sqlite_storage = SQLiteStorage(
        db_path=settings.DATA_PATH + "/db",
        retention_days=settings.RETENTION_DAYS
    )
    starter.register_module(
        name="SQLiteStorage",
        instance=sqlite_storage,
        dependencies=[]
    )
    
    # 5. 创建并注册DataStorage（Parquet存储，依赖：无）
    parquet_storage = DataStorage(base_path=settings.DATA_PATH)
    starter.register_module(
        name="ParquetStorage",
        instance=parquet_storage,
        dependencies=[]
    )
    
    # 6. 创建并注册HybridStorage（依赖：SQLiteStorage, ParquetStorage）
    hybrid_storage = HybridStorage(
        sqlite_db_path=settings.DATA_PATH + "/db",
        parquet_base_path=settings.DATA_PATH,
        retention_days=settings.RETENTION_DAYS
    )
    starter.register_module(
        name="HybridStorage",
        instance=hybrid_storage,
        dependencies=["SQLiteStorage", "ParquetStorage"]
    )
    
    # 7. 创建并注册MetricsCollector（依赖：EventBus）
    metrics_collector = MetricsCollector(event_bus=event_bus, window_size=60)
    starter.register_module(
        name="MetricsCollector",
        instance=metrics_collector,
        dependencies=["EventBus"]
    )
    
    # 8. 创建并注册MarketGateway（依赖：EventBus）
    market_gateway: MarketGateway = MarketGateway(event_bus=event_bus)
    
    def start_market_gateway(gateway):
        """启动行情网关"""
        import time
        try:
            # 从配置加载CTP服务器信息
            broker_config = load_broker_config()
            if not broker_config:
                logger.error("未找到CTP服务器配置")
                return

            broker_name = broker_config.get("broker_name")
            broker_type = broker_config.get("broker_type")
            config = broker_config.get("config")
            
            # 连接和登录（MarketGateway.connect() 接受一个字典参数）
            logger.info(f"开始连接行情网关 {broker_name}...")
            gateway.connect(config)
            
            # 等待连接和登录完成（最多等待10秒）
            max_wait_time = 10
            wait_interval = 0.1
            total_waited = 0
            
            while total_waited < max_wait_time:
                if gateway.md_api and gateway.md_api.login_status:
                    logger.info(f"行情网关 {broker_name} 登录成功, Broker Type: {broker_type}")
                    # 额外等待2秒，让订阅请求发送完成
                    logger.info("等待订阅请求发送...")
                    time.sleep(2)
                    break
                time.sleep(wait_interval)
                total_waited += wait_interval
            else:
                logger.warning(f"行情网关连接超时（等待了{max_wait_time}秒），将继续启动其他模块")
                
        except Exception as e:
            logger.error(f"行情网关启动失败: {e}", exc_info=True)
            raise
    
    starter.register_module(
        name="MarketGateway",
        instance=market_gateway,
        dependencies=["EventBus"],
        start_func=start_market_gateway,
        stop_func=lambda g: g.close()
    )
    
    # 9. 创建并注册ContractManager（依赖：EventBus，MarketGateway）
    contract_manager = ContractManager(
        event_bus=event_bus,
        config_path=Path("config/instrument_exchange.json")
    )
    starter.register_module(
        name="ContractManager",
        instance=contract_manager,
        dependencies=["EventBus", "MarketGateway"]
    )
    
    # 10. 创建并注册BarManager（依赖：EventBus, HybridStorage）
    # 从配置读取K线周期，如果配置为空则使用默认值
    intervals = DatacenterConfig.bar_intervals or ["1m", "5m", "15m", "30m", "1h", "1d"]
    
    bar_manager = BarManager(
        event_bus=event_bus,
        storage=hybrid_storage,
        intervals=intervals
    )
    starter.register_module(
        name="BarManager",
        instance=bar_manager,
        dependencies=["EventBus", "HybridStorage"]
    )
    
    # 11. 创建并注册DataArchiver（依赖：EventBus, SQLiteStorage, ParquetStorage）
    data_archiver = DataArchiver(
        event_bus=event_bus,
        sqlite_storage=sqlite_storage,
        parquet_storage=parquet_storage,
        retention_days=settings.RETENTION_DAYS
    )
    starter.register_module(
        name="DataArchiver",
        instance=data_archiver,
        dependencies=["EventBus", "SQLiteStorage", "ParquetStorage"]
    )
    
    # 12. 创建并注册AlarmScheduler（依赖：EventBus）
    alarm_scheduler = AlarmScheduler(event_bus=event_bus)
    
    # 创建默认定时任务
    create_default_tasks(alarm_scheduler, event_bus)
    
    starter.register_module(
        name="AlarmScheduler",
        instance=alarm_scheduler,
        dependencies=["EventBus"]
    )
    
    # 13. 将所有依赖注入到FastAPI服务器
    server.init_dependencies(
        storage=hybrid_storage,
        contract_manager=contract_manager,
        metrics_collector=metrics_collector,
        datacenter_starter=starter,
        bar_manager=bar_manager,
        data_archiver=data_archiver
    )
    
    # 14. 在新线程中启动FastAPI服务器（依赖：所有其他模块）
    def start_api_server():
        """启动API服务器"""
        logger.info(f"启动API服务器: {settings.API_HOST}:{settings.API_PORT}")
        uvicorn.run(
            server.app,
            host=settings.API_HOST,
            port=settings.API_PORT,
            log_level="info"
        )
    
    # 注册FastAPI模块（但不在starter中启动，而是单独线程）
    starter.register_module(
        name="FastAPIServer",
        instance=server.app,
        dependencies=["EventBus", "HybridStorage", "ContractManager", 
                     "BarManager", "MetricsCollector", "DataArchiver"]
    )
    
    # 15. 启动所有模块
    logger.info("=" * 80)
    logger.info("开始启动所有注册的模块...")
    logger.info("=" * 80)
    
    if not starter.start():
        logger.error("数据中心启动失败")
        sys.exit(1)
    
    logger.info("=" * 80)
    logger.info("所有模块启动完成，准备启动API服务器...")
    logger.info("=" * 80)
    
    # 16. 在单独线程中启动API服务器
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()
    
    # 等待API服务器启动
    import time
    time.sleep(1)
    
    logger.info("=" * 80)
    logger.info("🎉 数据中心启动成功！")
    logger.info("")
    logger.info(f"📊 Web管理界面: http://{settings.API_HOST}:{settings.API_PORT}/dashboard")
    logger.info(f"📖 API文档: http://{settings.API_HOST}:{settings.API_PORT}/docs")
    logger.info(f"❤️  健康检查: http://{settings.API_HOST}:{settings.API_PORT}/health")
    logger.info("")
    logger.info("按 Ctrl+C 停止数据中心")
    logger.info("=" * 80)
    
    # 17. 保持主线程运行
    try:
        # 等待API线程
        api_thread.join()
    except KeyboardInterrupt:
        logger.info("接收到停止信号，正在关闭数据中心...")
        # DataCenterStarter会自动处理优雅关闭
    
    return starter


def main():
    """主函数"""
    try:
        create_and_start_datacenter()
    except Exception as e:
        logger.error(f"数据中心启动异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
