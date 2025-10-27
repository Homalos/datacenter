#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : utility.py
@Date       : 2025/9/8 15:54
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 公用的工具，和业务没有关系的工具
"""
import configparser
import csv
import json
import os
import platform
import re
import sys
import time
from pathlib import Path
from typing import Any

import yaml  # type: ignore

from src.constants import Const
from src.utils.get_path import get_path_ins
from src.utils.log import get_logger

_logger = get_logger(__name__)


def sleep(seconds: float):
    """睡眠指定秒"""
    time.sleep(seconds)

def create_folder(folder_path: str) -> int:
    """
    创建指定路径的文件夹

    参数:
        folder_path (str): 要创建的文件夹路径

    返回:
        str: 操作结果消息
    """
    try:
        # 使用Path对象处理路径
        path = Path(folder_path)
        # 检查路径是否已存在
        if path.exists():
            return 1
        else:
            # 创建文件夹（包括所有必要的父目录）
            path.mkdir(parents=True, exist_ok=True)
            return 0
    except Exception as e:
        _logger.exception(f"创建文件夹时出错: {str(e)}")
        return -1

def file_exists(file_path: str) -> bool:
    """
    检查文件是否存在

    参数:
    file_path (str): 文件路径

    返回:
    bool: 如果文件存在返回True，否则返回False
    """
    return os.path.exists(file_path) and os.path.isfile(file_path)

def is_file_in_folder(dir_name):
    """
    如果需要多次检查同一目录中的文件
    高效批量检查目录中的文件
    用法：
    checker = is_file_in_folder(dir_name)
    for ins_id in ins.keys():
    if checker(f"{ins_id}.csv"):
        print(f"{ins_id}.csv 文件已存在")
    else:
        print(f"{ins_id}.csv 文件不存在")
    :param dir_name:
    :return:
    """
    file_set = set(os.listdir(dir_name))

    def file_in_directory(filename):
        return filename in file_set

    return file_in_directory

def is_file_in(filename, dir_name):
    checker = is_file_in_folder(dir_name)
    return checker(filename)

def delete_file(file_path) -> bool:
    """
    如果文件存在，则删除文件

    参数:
    file_path (str): 要检查并可能删除的文件路径

    返回:
    bool: 如果文件存在并被成功删除返回True，否则返回False
    """
    if os.path.exists(file_path):
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except PermissionError:
                _logger.exception(f"权限不足，无法删除文件: {file_path}")
                return False
            except Exception as e:
                _logger.exception(f"删除文件 {file_path} 时出错: {e}")
                return False
            return True
        else:
            _logger.warning(f"路径存在但不是文件: {file_path}")
            return False
    else:
        _logger.warning(f"文件 {file_path} 不存在")
        return False


def load_json(file_path: str) -> dict[str, Any]:
    """
    加载 JSON 文件。

    Loads a JSON file.
    """
    if not os.path.exists(file_path):
        _logger.info("未找到可选的 JSON 配置文件：{}".format(file_path))
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        _logger.error("无法解析 JSON 文件 {}: {}".format(file_path, e))
        return {}
    except IOError as e:
        _logger.error("无法读取文件 {}: {}".format(file_path, e))
        return {}

def write_json(file_path: str, data: dict[str, Any]) -> None:
    """
    将数据写入 JSON 文件。

    Writes the given data into a JSON file at the specified path.
    """
    try:
        with open(file_path, 'w', newline='\n', encoding='utf-8') as f:
            f.write(json.dumps(data, indent=4, ensure_ascii=False))
    except IOError as e:
        _logger.error("无法写入文件 {}: {}".format(file_path, e))

def load_ini(file_path: str) -> configparser.ConfigParser:
    """
    从指定路径加载INI配置文件。

    Args:
        file_path (str): INI配置文件的路径。

    Returns:
        ConfigParser: 加载的INI配置文件对象。

    说明：
        如果指定的文件不存在，则会创建一个空的INI文件。
        如果文件存在，则读取文件内容并返回一个ConfigParser对象。
        在创建空文件时，可以选择写入一些默认的空section或者注释，如果需要的话。

    """
    config_parser: configparser.ConfigParser = configparser.ConfigParser()
    # 检查文件是否存在，如果不存在则创建一个空的ini文件
    if not os.path.exists(file_path):
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("")
    config_parser.read(file_path, encoding='utf-8')

    return config_parser

def write_ini(config_parser: configparser.ConfigParser, file_path: str) -> None:
    """
    将配置写入ini文件。

    Args:
        config_parser (ConfigParser): 配置文件对象。
        file_path (str): 要写入的ini文件路径。

    """
    with open(file_path, "w", encoding='utf-8') as f:
        config_parser.write(f)  # type: ignore

def load_yaml(config_path: str) -> dict:
    """
    加载 yaml 配置文件，单纯加载 yaml 文件后直接返回 dict 数据
    如果需要更强大的配置加载功能请使用 utils/config_manager.py 中的配置管理，支持热加载
    :param config_path:
    :return:
    """
    if not os.path.exists(config_path):
        _logger.error(f"未找到配置文件: {config_path}")
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except (yaml.YAMLError, IOError):
        # 如果配置文件解析失败，返回默认配置
        return {}

def prepare_address(address: str) -> str:
    """
    如果没有协议，则帮助程序会在前面添加 tcp:// 作为前缀。

    If there is no protocol, the helper prefixes it with tcp:// .
    :param address: 行情服务器地址 Market server address
    :return: 返回带协议的服务器地址 Returns the server address with protocol
    """
    if not any(address.startswith(scheme) for scheme in ["tcp://", "ssl://", "socks://"]):
        return "tcp://" + address
    return address

def del_num(content) -> str:
    """
    删除字符串中的所有数字。

    Args:
        content (str): 需要删除数字的字符串。

    Returns:
        str: 删除数字后的字符串。
    """
    return re.sub(r'\d', '', content)

def write_csv(file_name, method, content):
    """
    将数据写入csv中,w代表删除原有的写入，w+代表读写，a代表追写，a+代表读+追写
    @param file_name:
    @param method: w代表删除原有的写入，w+代表读写，a代表追写，a+代表读+追写
    @param content: 注意！！！，内容为一个列表，即：需要用[]括起来，如['1',str(a)]
    """
    # 1. 创建文件对象
    csv_file = open(file_name, method, newline='', encoding='utf-8')
    # 2. 基于文件对象构建 csv写入对象
    csv_writer = csv.writer(csv_file)
    # 3. 写入csv文件内容
    csv_writer.writerow(content)
    # 4. 关闭文件
    csv_file.close()

def get_file_name(path, ext):
    """
    获取指定路径下，指定格式的所有文件名
    @param path: 路径，如：上一层 '../'
    @param ext: 指定后缀
    """
    list_all_files = os.listdir(path)
    list_file = []

    # 获取指定格式的文件
    for file in list_all_files:
        if ext in file:
            list_file.append(file)
    return list_file[:]

def convert_intervals_to_minutes(interval_strings: list[str]) -> list[int]:
    """将时间间隔字符串转换为分钟数"""
    conversion_map = {
        'm': 1, 'h': 60, 'd': 1440
    }
    intervals = []
    for interval_str in interval_strings:
        if interval_str and interval_str[-1] in conversion_map:
            try:
                number = int(interval_str[:-1])
                unit = interval_str[-1]
                intervals.append(number * conversion_map[unit])
            except ValueError:
                _logger.warning(f"无法解析时间间隔: {interval_str}")
        else:
            _logger.warning(f"不支持的时间间隔格式: {interval_str}")

    if not intervals:
        _logger.warning("未找到有效的时间间隔配置，使用默认值")
        intervals = [1, 5, 15, 30, 60]  # 默认分钟间隔

    _logger.info(f"K线时间间隔配置: {intervals} 分钟")
    return intervals

def load_all_instruments() -> dict[str, str]:
    """
    从instrument_exchange.json加载全市场期货合约
    :return: 所有合约和交易所映射字典
    """
    try:
        instrument_exchange_json = load_json(str(get_path_ins.get_config_dir() / Const.INSTRUMENT_EXCHANGE_FILENAME))

        if instrument_exchange_json:
            _logger.info(f"从文件加载了 {len(instrument_exchange_json)} 个期货合约")
            return instrument_exchange_json
        else:
            _logger.warning("instrument_exchange_id.json文件不存在或为空")
            return {}
    except Exception as e:
        _logger.exception(f"加载合约列表失败: {e}")
        return {}

def wait_with_timeout(condition_check: bool, check_interval: float = 0.1, timeout: float = 5.0):
    """
    循环等待机制，超过指定时间自动退出

    :param condition_check: 条件检查，返回True表示条件满足，停止等待
    :param check_interval: 检查间隔(秒)，默认0.5秒
    :param timeout: 超时时间(秒)，默认20秒
    :return: bool: 如果条件满足返回True，超时返回False
    """
    start_time = time.time()
    while not condition_check:
        # 检查是否超时
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            _logger.info(f"等待登录超时 ({timeout}秒)")
            break
        # 等待一段时间再检查
        time.sleep(check_interval)


def get_os_info() -> dict:
    """获取详细的系统信息"""
    system = platform.system()  # Windows/Linux/Darwin(Linux和macOS都是类Unix系统)
    release = platform.release()
    version = platform.version()

    os_info = {
        'system': system,
        'release': release,
        'version': version,
        'platform': sys.platform
    }
    return os_info

# if __name__ == '__main__':
#     is_exist = is_file_in('SA601_1m.csv', 'D:/Project/PycharmProjects/Homalos/data/kline/20250930')
#     print(is_exist)
