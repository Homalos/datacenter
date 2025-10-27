#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : time_module.py
@Date       : 2025/9/28 23:40
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 时间模块
"""
import datetime


class TimeModule(object):

    def __init__(self):
        self.time_module = datetime

    def now(self, tz=None):
        return self.time_module.datetime.now(tz)

    def strf_now_time(self, fmt):
        return self.time_module.datetime.now().strftime(fmt)
