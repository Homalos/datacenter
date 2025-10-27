#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : start_web.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: Web控制面板启动脚本 - 只启动FastAPI服务，不启动数据中心核心
"""
import uvicorn
from config import settings
from src.utils.log import get_logger

logger = get_logger(__name__)


def main():
    """启动Web控制面板"""
    logger.info("=" * 80)
    logger.info("Homalos 数据中心 - Web控制面板")
    logger.info("=" * 80)
    logger.info("")
    logger.info(f"📊 控制面板地址: http://{settings.API_HOST}:{settings.API_PORT}/dashboard")
    logger.info(f"📖 API文档: http://{settings.API_HOST}:{settings.API_PORT}/docs")
    logger.info(f"❤️  健康检查: http://{settings.API_HOST}:{settings.API_PORT}/health")
    logger.info("")
    logger.info("提示: 在Web界面中启动/停止数据中心核心服务")
    logger.info("=" * 80)
    
    # 启动FastAPI服务器
    uvicorn.run(
        "src.api.server:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="info",
        reload=False  # 生产环境关闭自动重载
    )


if __name__ == "__main__":
    main()

