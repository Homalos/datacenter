#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos_data_center
@FileName   : storage.py
@Date       : 2025/10/14 15:40
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据存储与加载封装（支持多合约、日期文件夹，使用trading_day命名）
"""
import os
import duckdb
import pandas as pd  # type: ignore
import threading
import tarfile
import shutil
from config import settings
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

from src.core.trading_day_manager import TradingDayManager
from src.utils.log import get_logger

os.makedirs(settings.TICK_PATH, exist_ok=True)
os.makedirs(settings.KLINE_PATH, exist_ok=True)


class DataStorage:
    """
    数据存储类 - 支持多合约、按日期文件夹存储（分离式压缩策略）
    
    文件结构：
    - 交易时间：data/csv/ticks/{date}/{symbol}.csv（未压缩，高性能）
    - 非交易时间：data/csv/ticks/{date}.tar.gz（整个文件夹压缩为一个文件）
    
    优势：
    1. 交易时间：CSV未压缩写入，性能最优
    2. 非交易时间：整个交易日文件夹打包压缩，节省空间
    3. CSV格式直观易读，可用Excel/文本编辑器打开
    4. 整体压缩率更高（tar.gz对整个文件夹压缩）
    """

    def __init__(self, base_path: str, trading_day_manager: Optional[TradingDayManager] = None):
        self.base_path = base_path
        self.conn = duckdb.connect(database=':memory:')
        self.trading_day_manager = trading_day_manager
        self.logger = get_logger(self.__class__.__name__)
        self.compressed_folders: set[str] = set()  # 记录已压缩的文件夹
        
        # 文件锁字典：每个文件一把锁（防止并发写入冲突）
        self._file_locks: Dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()  # 保护file_locks字典本身的锁
    
    def _get_file_lock(self, file_path: Path) -> threading.Lock:
        """
        获取指定文件的锁（如果不存在则创建）
        
        Args:
            file_path: 文件路径
            
        Returns:
            该文件的锁对象
        """
        file_key = str(file_path.resolve())  # 使用绝对路径作为key
        
        with self._locks_lock:
            if file_key not in self._file_locks:
                self._file_locks[file_key] = threading.Lock()
            return self._file_locks[file_key]

    def _get_file_path(self, symbol: str, date: Optional[str] = None) -> Path:
        """
        获取文件路径（使用trading_day作为文件夹名）
        
        Args:
            symbol: 合约代码
            date: 日期字符串，格式YYYYMMDD，如果为None则使用trading_day
        
        Returns:
            完整的文件路径（.csv格式，未压缩）
        """
        if date is None:
            # 优先使用trading_day_manager获取交易日
            if self.trading_day_manager:
                date = self.trading_day_manager.get_trading_day()
            else:
                # 回退到系统日期
                date = datetime.now().strftime("%Y%m%d")
        
        # data/csv/ticks/{trading_day}/{symbol}.csv（未压缩）
        date_dir = Path(self.base_path) / date
        date_dir.mkdir(parents=True, exist_ok=True)
        return date_dir / f"{symbol}.csv"

    def _save_data(self, symbol: str, df: pd.DataFrame, date: Optional[str], data_type: str):
        """
        通用数据保存方法 - 追加写入CSV格式
        
        Args:
            symbol: 合约代码
            df: 数据DataFrame
            date: 日期（YYYYMMDD），默认今天
            data_type: 数据类型名称（用于日志显示，如 "Tick" 或 "K线"）
            
        Note:
            - 使用追加写入模式（mode='a'）
            - 通过文件锁防止并发冲突
            - 不进行去重和排序（定期批量处理）
        """
        if df.empty:
            self.logger.warning(f"尝试保存空DataFrame [{symbol}]，已跳过")
            return
            
        file_path = self._get_file_path(symbol, date)
        
        # 获取文件锁（防止并发写入冲突）
        file_lock = self._get_file_lock(file_path)
        
        with file_lock:
            try:
                # 检查文件是否存在（决定是否写入表头）
                file_exists = file_path.exists() and file_path.stat().st_size > 0
                
                # 追加写入（原子操作，无需临时文件）
                df.to_csv(
                    file_path,
                    mode='a',           # 追加模式
                    header=not file_exists,  # 文件不存在时写表头
                    index=False
                )
                
                self.logger.debug(f"✓ 成功追加 {len(df)} 条{data_type}到 {file_path.name}")
                
            except Exception as e:
                self.logger.error(f"追加写入{data_type}数据失败 [{symbol}]: {e}", exc_info=True)
                raise

    def save_ticks(self, symbol: str, df: pd.DataFrame, date: Optional[str] = None):
        """
        保存 Tick 数据到CSV格式（追加写入，高性能）
        
        Args:
            symbol: 合约代码
            df: 数据DataFrame
            date: 日期（YYYYMMDD），默认今天
        """
        self._save_data(symbol, df, date, data_type="Tick")

    def save_kline(self, symbol: str, df: pd.DataFrame, date: Optional[str] = None):
        """
        保存 K线 数据到CSV格式（追加写入，高性能）
        
        Args:
            symbol: 合约代码（含周期，如 a2511_1m）
            df: 数据DataFrame
            date: 日期（YYYYMMDD），默认今天
        """
        self._save_data(symbol, df, date, data_type="K线")
    
    def deduplicate_and_sort_file(self, file_path: Path) -> bool:
        """
        去重并排序指定的CSV文件
        
        Args:
            file_path: CSV文件路径
            
        Returns:
            是否成功
            
        Note:
            - 在非交易时间调用
            - 去重规则：同一datetime保留最后一条
            - 按datetime升序排序
        """
        if not file_path.exists() or file_path.stat().st_size == 0:
            return True
        
        file_lock = self._get_file_lock(file_path)
        
        with file_lock:
            temp_path = None
            try:
                self.logger.info(f"开始去重和排序: {file_path.name}")
                
                # 读取数据
                df = pd.read_csv(file_path)
                original_count = len(df)
                
                # 转换datetime类型
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                
                # 去重（保留最后一条）
                if 'datetime' in df.columns:
                    df = df.drop_duplicates(subset=['datetime'], keep='last')
                    
                # 排序
                if 'datetime' in df.columns:
                    df = df.sort_values('datetime').reset_index(drop=True)
                
                duplicates = original_count - len(df)
                
                # 原子写入：先写临时文件，再替换
                temp_path = file_path.parent / f"{file_path.stem}.dedup.tmp.csv"
                df.to_csv(temp_path, index=False)
                temp_path.replace(file_path)
                
                if duplicates > 0:
                    self.logger.info(
                        f"✓ 去重完成: {file_path.name} "
                        f"({original_count} → {len(df)}条, 去除{duplicates}条重复)"
                    )
                else:
                    self.logger.debug(f"✓ 无重复数据: {file_path.name}")
                
                return True
                
            except Exception as e:
                self.logger.error(f"去重失败 [{file_path.name}]: {e}", exc_info=True)
                if temp_path and temp_path.exists():
                    temp_path.unlink()
                return False

    def _load_data_by_date_range(
        self,
        base_path: Path,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        通用数据加载方法 - 按日期范围加载CSV数据
        
        Args:
            base_path: 数据根目录路径
            symbol: 合约代码
            start_date: 开始日期（YYYYMMDD），默认今天
            end_date: 结束日期（YYYYMMDD），默认今天
        
        Returns:
            合并后的DataFrame
        """
        if not base_path.exists():
            return pd.DataFrame()
        
        # 如果未指定日期，使用今天
        if start_date is None and end_date is None:
            start_date = end_date = datetime.now().strftime("%Y%m%d")
        elif start_date is None:
            start_date = end_date if end_date else datetime.now().strftime("%Y%m%d")
        elif end_date is None:
            end_date = start_date
        
        # 收集所有日期文件夹中的该symbol数据
        dfs = []
        for date_folder in sorted(base_path.iterdir()):
            if not date_folder.is_dir():
                continue
            
            date_str = date_folder.name
            if start_date and end_date and start_date <= date_str <= end_date:
                symbol_file = date_folder / f"{symbol}.csv"
                if symbol_file.exists() and symbol_file.stat().st_size > 0:
                    try:
                        df = pd.read_csv(symbol_file)
                        dfs.append(df)
                    except Exception as e:
                        self.logger.error(f"读取{symbol_file}失败: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        # 合并所有数据
        combined = pd.concat(dfs, ignore_index=True)
        combined['datetime'] = pd.to_datetime(combined['datetime'])
        combined = combined.sort_values('datetime').reset_index(drop=True)
        
        return combined

    def load_ticks(self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        加载 Tick 数据（从CSV日期文件夹）
        
        Args:
            symbol: 合约代码
            start_date: 开始日期（YYYYMMDD），默认今天
            end_date: 结束日期（YYYYMMDD），默认今天
        
        Returns:
            合并后的DataFrame
        """
        return self._load_data_by_date_range(
            base_path=Path(settings.TICK_PATH),
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

    def load_kline(self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        加载 K线 数据（从CSV日期文件夹）
        
        Args:
            symbol: 合约代码
            start_date: 开始日期（YYYYMMDD），默认今天
            end_date: 结束日期（YYYYMMDD），默认今天
        
        Returns:
            合并后的DataFrame
        """
        return self._load_data_by_date_range(
            base_path=Path(settings.KLINE_PATH),
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )

    def query_kline(self, symbol: str, start_time: str, end_time: str) -> pd.DataFrame:
        """
        查询指定时间区间K线（兼容性接口）
        
        Args:
            symbol: 合约代码
            start_time: 开始时间（可以是日期或完整时间）
            end_time: 结束时间
        
        Returns:
            查询结果DataFrame
        """
        # 从时间字符串提取日期
        start_date = start_time[:10].replace('-', '')  # YYYY-MM-DD -> YYYYMMDD
        end_date = end_time[:10].replace('-', '')
        
        # 加载数据
        df = self.load_kline(symbol, start_date, end_date)
        
        if df.empty:
            return df
        
        # 时间范围过滤
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)]
        
        return df
    
    def query_ticks(self, symbol: str, start_time: str, end_time: str) -> pd.DataFrame:
        """
        查询指定时间区间Tick（兼容性接口）
        
        Args:
            symbol: 合约代码
            start_time: 开始时间（可以是日期或完整时间）
            end_time: 结束时间
        
        Returns:
            查询结果DataFrame
        """
        # 从时间字符串提取日期
        start_date = start_time[:10].replace('-', '')  # YYYY-MM-DD -> YYYYMMDD
        end_date = end_time[:10].replace('-', '')
        
        # 加载数据
        df = self.load_ticks(symbol, start_date, end_date)
        
        if df.empty:
            return df
        
        # 时间范围过滤
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)]
        
        return df
    
    def compress_trading_day_folder(self, trading_day: str) -> bool:
        """
        压缩整个交易日文件夹为tar.gz格式（在非交易时间调用）
        
        Args:
            trading_day: 交易日期（YYYYMMDD）
            
        Returns:
            是否压缩成功
            
        Note:
            - 在非交易时间调用
            - 压缩整个文件夹为 {trading_day}.tar.gz
            - 压缩成功后删除原文件夹
        """
        folder_path = Path(self.base_path) / trading_day
        
        # 检查文件夹是否存在
        if not folder_path.exists() or not folder_path.is_dir():
            self.logger.warning(f"文件夹不存在，无需压缩: {folder_path}")
            return False
        
        # 检查是否已经压缩
        archive_path = Path(self.base_path) / f"{trading_day}.tar.gz"
        if archive_path.exists():
            self.logger.info(f"压缩文件已存在，跳过: {archive_path.name}")
            return True
        
        # 统计文件数量和大小
        csv_files = list(folder_path.glob("*.csv"))
        if not csv_files:
            self.logger.warning(f"文件夹为空，跳过压缩: {trading_day}")
            return False
        
        total_size = sum(f.stat().st_size for f in csv_files)
        total_size_mb = total_size / 1024 / 1024
        
        self.logger.info(
            f"开始压缩交易日文件夹: {trading_day} "
            f"({len(csv_files)}个文件, {total_size_mb:.2f}MB)"
        )
        
        temp_archive = None
        try:
            # 创建tar.gz压缩包
            temp_archive = archive_path.with_suffix('.tar.gz.tmp')
            with tarfile.open(temp_archive, "w:gz", compresslevel=6) as tar:
                # 添加整个文件夹（保留目录结构）
                tar.add(folder_path, arcname=trading_day)
            
            # 原子重命名
            temp_archive.rename(archive_path)
            
            # 压缩成功，删除原文件夹
            shutil.rmtree(folder_path)
            
            # 统计压缩率
            compressed_size = archive_path.stat().st_size
            compressed_size_mb = compressed_size / 1024 / 1024
            compression_ratio = (1 - compressed_size / total_size) * 100
            
            self.logger.info(
                f"✓ 压缩成功: {trading_day}.tar.gz "
                f"({compressed_size_mb:.2f}MB, 压缩率: {compression_ratio:.1f}%)"
            )
            
            # 标记为已压缩
            self.compressed_folders.add(trading_day)
            return True
            
        except Exception as e:
            self.logger.error(f"压缩文件夹失败 [{trading_day}]: {e}", exc_info=True)
            # 清理临时文件
            if temp_archive and temp_archive.exists():
                temp_archive.unlink()
            return False
    
    def decompress_trading_day_folder(self, trading_day: str) -> bool:
        """
        解压交易日压缩包（临时解压，用于查询）
        
        Args:
            trading_day: 交易日期（YYYYMMDD）
            
        Returns:
            是否解压成功
        """
        archive_path = Path(self.base_path) / f"{trading_day}.tar.gz"
        folder_path = Path(self.base_path) / trading_day
        
        # 检查压缩包是否存在
        if not archive_path.exists():
            self.logger.debug(f"压缩包不存在: {trading_day}.tar.gz")
            return False
        
        # 如果文件夹已存在，无需解压
        if folder_path.exists():
            return True
        
        try:
            self.logger.info(f"解压交易日数据: {trading_day}.tar.gz")
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(Path(self.base_path))
            
            self.logger.info(f"✓ 解压成功: {trading_day}")
            return True
            
        except Exception as e:
            self.logger.error(f"解压失败 [{trading_day}]: {e}", exc_info=True)
            return False
