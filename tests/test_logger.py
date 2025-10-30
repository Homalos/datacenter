#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : test_logger.py
@Date       : 2025/10/30 18:07
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
from src.utils.log import get_logger

logger = get_logger()


logger.bind(name="donny")
logger.info("这是普通日志")
logger.warning("这是警告日志")
logger.error("这是错误日志")