#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos_data_center
@FileName   : data_compressor.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据压缩调度器 - 在非交易时间批量压缩历史数据
"""
from datetime import datetime, time
from pathlib import Path
from typing import Optional
import threading
import time as time_module

from src.core.storage import DataStorage
from src.core.trading_day_manager import TradingDayManager
from src.utils.log import get_logger


class DataCompressor:
    """
    数据压缩调度器
    
    功能：
    1. 在非交易时间（收盘后）自动压缩历史数据
    2. 将整个交易日文件夹压缩为tar.gz
    3. 定期清理旧的未压缩数据
    
        工作流程：
        - 每日收盘后（如16:00）开始压缩前一交易日数据
        - 将 data/csv/ticks/20251027/ 压缩为 data/csv/ticks/20251027.tar.gz
        - 压缩成功后删除原文件夹
    """
    
    def __init__(
        self,
        tick_storage: DataStorage,
        kline_storage: DataStorage,
        trading_day_manager: Optional[TradingDayManager] = None,
        compress_time: tuple[int, int] = (16, 0),  # 默认16:00开始压缩
        enabled: bool = True
    ):
        """
        初始化数据压缩器
        
        Args:
            tick_storage: Tick数据存储实例
            kline_storage: K线数据存储实例
            trading_day_manager: 交易日管理器
            compress_time: 压缩时间（时, 分）
            enabled: 是否启用自动压缩
        """
        self.tick_storage = tick_storage
        self.kline_storage = kline_storage
        self.trading_day_manager = trading_day_manager
        self.compress_hour, self.compress_minute = compress_time
        self.enabled = enabled
        self.logger = get_logger(self.__class__.__name__)
        
        self._stop_event = threading.Event()
        self._compress_thread: Optional[threading.Thread] = None
        self._last_compress_date: Optional[str] = None
        
        if enabled:
            self.start()
    
    def start(self) -> None:
        """启动压缩调度器"""
        if self._compress_thread and self._compress_thread.is_alive():
            self.logger.warning("压缩调度器已在运行")
            return
        
        self._stop_event.clear()
        self._compress_thread = threading.Thread(
            target=self._compress_worker,
            name="DataCompressor",
            daemon=True
        )
        self._compress_thread.start()
        self.logger.info(
            f"数据压缩调度器已启动，每日 {self.compress_hour:02d}:{self.compress_minute:02d} 自动压缩"
        )
    
    def stop(self) -> None:
        """停止压缩调度器"""
        if self._compress_thread and self._compress_thread.is_alive():
            self.logger.info("正在停止数据压缩调度器...")
            self._stop_event.set()
            self._compress_thread.join(timeout=5.0)
            self.logger.info("数据压缩调度器已停止")
    
    def _compress_worker(self) -> None:
        """压缩工作线程"""
        while not self._stop_event.is_set():
            try:
                # 检查是否到了压缩时间
                if self._should_compress():
                    self._do_compress()
                
                # 每分钟检查一次
                time_module.sleep(60)
                
            except Exception as e:
                self.logger.error(f"压缩工作线程异常: {e}", exc_info=True)
                time_module.sleep(60)
    
    def _should_compress(self) -> bool:
        """
        判断是否应该开始压缩
        
        Returns:
            是否应该压缩
        """
        now = datetime.now()
        current_date = now.strftime("%Y%m%d")
        
        # 检查是否已经压缩过今天
        if self._last_compress_date == current_date:
            return False
        
        # 检查当前时间是否到了压缩时间
        compress_time = time(self.compress_hour, self.compress_minute)
        current_time = now.time()
        
        # 在压缩时间后的1小时内都可以执行压缩
        if compress_time <= current_time < time(self.compress_hour + 1, 0):
            return True
        
        return False
    
    def _do_compress(self) -> None:
        """执行压缩任务"""
        current_date = datetime.now().strftime("%Y%m%d")
        
        self.logger.info("=" * 60)
        self.logger.info("开始执行数据压缩任务")
        self.logger.info("=" * 60)
        
        try:
            # 获取需要压缩的交易日列表（排除今日）
            trading_days_to_compress = self._get_folders_to_compress()
            
            if not trading_days_to_compress:
                self.logger.info("没有需要压缩的数据")
                self._last_compress_date = current_date
                return
            
            self.logger.info(f"找到 {len(trading_days_to_compress)} 个交易日待压缩")
            
            # 步骤1: 去重和排序Tick数据
            self.logger.info("步骤1/4: 去重和排序Tick数据...")
            for trading_day in trading_days_to_compress:
                self._deduplicate_folder(
                    self.tick_storage.base_path,
                    trading_day,
                    "Tick"
                )
            
            # 步骤2: 压缩Tick数据
            self.logger.info("步骤2/4: 压缩Tick数据...")
            tick_success = 0
            for trading_day in trading_days_to_compress:
                if self.tick_storage.compress_trading_day_folder(trading_day):
                    tick_success += 1
            
            # 步骤3: 去重和排序K线数据
            self.logger.info("步骤3/4: 去重和排序K线数据...")
            for trading_day in trading_days_to_compress:
                self._deduplicate_folder(
                    self.kline_storage.base_path,
                    trading_day,
                    "K线"
                )
            
            # 步骤4: 压缩K线数据
            self.logger.info("步骤4/4: 压缩K线数据...")
            kline_success = 0
            for trading_day in trading_days_to_compress:
                if self.kline_storage.compress_trading_day_folder(trading_day):
                    kline_success += 1
            
            self.logger.info("=" * 60)
            self.logger.info(
                f"压缩任务完成: "
                f"Tick: {tick_success}/{len(trading_days_to_compress)}, "
                f"K线: {kline_success}/{len(trading_days_to_compress)}"
            )
            self.logger.info("=" * 60)
            
            # 记录本次压缩日期
            self._last_compress_date = current_date
            
        except Exception as e:
            self.logger.error(f"执行压缩任务失败: {e}", exc_info=True)
    
    def _deduplicate_folder(self, base_path: str, trading_day: str, data_type: str) -> None:
        """
        去重和排序文件夹中所有CSV文件
        
            Args:
                base_path: 数据根目录（如 data/csv/ticks）
                trading_day: 交易日（如 20251028）
                data_type: 数据类型（"Tick" 或 "K线"）
        """
        folder_path = Path(base_path) / trading_day
        
        if not folder_path.exists() or not folder_path.is_dir():
            self.logger.debug(f"文件夹不存在，跳过去重: {trading_day}")
            return
        
        csv_files = list(folder_path.glob("*.csv"))
        if not csv_files:
            self.logger.debug(f"文件夹为空，跳过去重: {trading_day}")
            return
        
        self.logger.info(
            f"开始去重和排序 {trading_day} 中的 {len(csv_files)} 个{data_type}文件..."
        )
        
        success_count = 0
        
        # 使用对应存储实例的去重方法
        storage = self.tick_storage if data_type == "Tick" else self.kline_storage
        
        for csv_file in csv_files:
            if storage.deduplicate_and_sort_file(csv_file):
                success_count += 1
        
        self.logger.info(
            f"✓ 去重完成: {trading_day} "
            f"({success_count}/{len(csv_files)} 个{data_type}文件处理成功)"
        )
    
    def _get_folders_to_compress(self) -> list[str]:
        """
        获取需要压缩的文件夹列表
        
        Returns:
            交易日列表（YYYYMMDD格式）
        """
        folders_to_compress = []
        current_date = datetime.now().strftime("%Y%m%d")
        
        # 扫描Tick数据目录
        tick_base_path = Path(self.tick_storage.base_path)
        if tick_base_path.exists():
            for folder in tick_base_path.iterdir():
                if folder.is_dir() and folder.name.isdigit() and len(folder.name) == 8:
                    # 排除今日数据
                    if folder.name < current_date:
                        # 检查是否已压缩
                        archive_path = tick_base_path / f"{folder.name}.tar.gz"
                        if not archive_path.exists():
                            folders_to_compress.append(folder.name)
        
        # 去重并排序
        folders_to_compress = sorted(set(folders_to_compress))
        
        return folders_to_compress
    
    def compress_now(self, trading_day: Optional[str] = None) -> bool:
        """
        立即压缩指定交易日（手动触发）
        
        Args:
            trading_day: 交易日期（YYYYMMDD），None表示压缩所有未压缩的
            
        Returns:
            是否成功
        """
        if trading_day:
            self.logger.info(f"手动压缩交易日: {trading_day}")
            tick_ok = self.tick_storage.compress_trading_day_folder(trading_day)
            kline_ok = self.kline_storage.compress_trading_day_folder(trading_day)
            return tick_ok and kline_ok
        else:
            self._do_compress()
            return True

