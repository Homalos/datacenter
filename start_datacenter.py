#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : start_datacenter.py
@Date       : 2025/10/27 10:26
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
import uvicorn
from config import settings
from src.api.server import app
from src.utils.log import get_logger

_logger = get_logger(__name__)

def run():
    """启动API服务"""
    _logger.info(f"启动API服务 {settings.API_HOST}:{settings.API_PORT}")
    uvicorn.run(app, host=settings.API_HOST, port=settings.API_PORT)


if __name__ == "__main__":
    run()
