#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : logger.py
@Date       : 2025/10/27 10:29
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 全局日志模块（loguru+配置化+上下文标签）

1. 外部配置文件 config/log_config.yaml
可配置 level, log_dir, rotation, retention。
方便开发/生产环境切换。

2. 上下文绑定 (logger.bind)
支持给日志增加 context（比如 "strategy", "engine", "datafeed"）。
打印时自动带上 context，便于过滤。

3. 异步环境支持
已经用 enqueue=True，高频 tick 日志写入不会阻塞主线程/事件循环。

4. 全链路日志用同一个 trace_id 串联
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from src.constants import Const
from src.core.trace_context import get_trace_id

__all__ = ["logger", "get_logger"]

# 从当前文件往上获取项目根目录
current_file = Path(__file__).resolve()

# 从当前文件向上获取到项目根目录 /Homalos
root_path: Path = current_file.parent.parent.parent.parent


def _load_log_config(config_filepath: str) -> dict:
    """内部函数：加载日志配置文件，避免循环导入"""
    if not os.path.exists(config_filepath):
        # 如果配置文件不存在，返回默认配置
        return {"logging": {}}
    try:
        with open(config_filepath, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except (yaml.YAMLError, IOError):
        # 如果配置文件解析失败，返回默认配置
        return {"logging": {}}


# ===================== 初始化全局日志配置 =====================
# 获取日志配置文件名
log_config_name = f"{Const.CONFIG_DIR_NAME}/{Const.LOG_CONFIG_FILENAME}"
config_path = str(root_path / log_config_name)
# 加载日志配置
config = _load_log_config(config_path)
cfg = config.get("logging", {})

is_debug = cfg.get("is_debug", False)  # 是否开启 DEBUG 模式
log_filename: str = cfg.get("log_filename", "datacenter")
error_filename: str = cfg.get("error_filename", "datacenter_error")
filename_format: str = cfg.get("filename_format", "%Y%m%d")

log_dir_name: str = cfg.get("log_dir_name", "logs")
level: str = cfg.get("level", "INFO")  # 输出的最小日志级别
rotation: str = cfg.get("rotation", "10 MB")  # 日志轮转大小
retention: str = cfg.get("retention", "7 days")  # 保留天数
compression: str = cfg.get("compression", "zip")  # 压缩

# 检测是否应该启用颜色输出
# 1. 如果设置了 NO_COLOR 环境变量，禁用颜色
# 2. 如果 stdout 不是 TTY（如重定向到文件），禁用颜色
# 3. 否则使用配置文件中的设置

should_colorize = cfg.get("colorize", True)
# if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
#     should_colorize = False
#
colorize = should_colorize  # 颜色

enqueue = cfg.get("enqueue", True)  # 多进程程安全
backtrace = cfg.get("backtrace", True)  # 堆栈回溯
diagnose = cfg.get("diagnose", True)  # 诊断

today = datetime.now().strftime(filename_format)
log_real_filename = f"{log_filename}_{today}.log"
log_real_error_filename = f"{error_filename}_{today}.log"

is_detail_log = True

log_dir_path = root_path / log_dir_name
# 确保日志目录存在
Path(log_dir_path).mkdir(parents=True, exist_ok=True)


# ===================== TraceId 自动注入 Filter =====================
class TraceIdFilter:
    """自动注入 trace_id 到日志 extra"""

    def __call__(self, record):
        record["extra"]["trace_id"] = get_trace_id() or "-"
        return True


# ===================== 初始化全局日志配置 =====================
# 清理默认 logger 配置（避免重复打印）
logger.remove()

# 控制台输出格式（根据debug模式决定是否显示详细信息）
if is_debug:
    console_format = ("<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                      "<level>{level: <8}</level> | "
                      "<magenta>[{extra[context]}]</magenta> "
                      "<yellow>{extra[trace_id]}</yellow> "
                      "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
                      "- <level>{message}</level>")
else:
    console_format = ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                      "<level>{level: <8}</level> | "
                      "<magenta>[{extra[context]}]</magenta> "
                      "<yellow>{extra[trace_id]}</yellow> "
                      "<cyan>{name}</cyan>:<cyan>{function}</cyan> "
                      "- <level>{message}</level>")

file_format = ("{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | [{extra[context]}] "
               "{extra[trace_id]} {name}:{function}:{line} - {message}")

# 控制台输出
logger.add(
    sys.stdout,
    level=level,
    format=console_format,
    colorize=colorize,
    backtrace=backtrace,
    diagnose=diagnose,
    enqueue=enqueue,
    filter=TraceIdFilter()
)

# 文件输出（全量日志）
logger.add(
    os.path.join(log_dir_path, log_real_filename),
    level=level,
    format=file_format,
    rotation=rotation,
    retention=retention,
    compression=compression,
    encoding="utf-8",
    enqueue=enqueue,
    backtrace=backtrace,
    diagnose=diagnose,
    filter=TraceIdFilter()
)

# 错误日志单独保存
logger.add(
    os.path.join(log_dir_path, log_real_error_filename),
    level="ERROR",
    format=file_format,
    rotation=rotation,
    retention=retention,
    compression=compression,
    encoding="utf-8",
    enqueue=enqueue,
    backtrace=backtrace,
    diagnose=diagnose,
    filter=TraceIdFilter()
)


# ===================== 对外 API =====================
def get_logger(context: str = "Homalos") -> Any:
    """
    根据模块上下文获取 logger
    trace_id 会自动从全局上下文获取（无需手动传）
    :param context: 模块上下文，默认"Homalos"
    :return: logger
    """
    return logger.bind(context=context)

# ===================== SSE日志流支持 =====================
# 根据环境变量决定是否启用SSE日志
if os.environ.get("ENABLE_SSE_LOGS", "false").lower() == "true":
    try:
        from src.web.services.log_buffer import sse_log_sink

        # 添加SSE sink
        logger.add(
            sse_log_sink,
            format="{message}",  # sink内部自己处理格式
            level="INFO",  # 只推送INFO及以上级别
            enqueue=True,  # 异步处理
            catch=True  # 捕获sink内部异常
        )

        # 使用绑定的logger避免context错误
        sse_logger = logger.bind(context="SSE")
        sse_logger.info("SSE日志流已启用")

    except ImportError:
        # Web服务未启动时，log_buffer模块不存在，忽略
        pass
    except Exception as e:
        print(f"启用SSE日志流失败: {e}", file=sys.stderr)
