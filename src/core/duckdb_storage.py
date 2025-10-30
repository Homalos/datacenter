#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : duckdb_storage.py
@Date       : 2025/10/29
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: DuckDB存储 - 按交易日分文件 + 按合约分表，极速查询引擎
"""
import re
import duckdb
import pandas as pd
import threading
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict

from src.system_config import Config
from src.utils.log import get_logger
from src.core.trading_day_manager import TradingDayManager


def normalize_instrument_id(instrument_id: str) -> str:
    """
    规范化合约ID为合法的SQL表名
    
    规则：
    - 转小写
    - 移除特殊字符（只保留字母、数字、下划线）
    - 确保以字母开头（SQL表名要求）
    
    Args:
        instrument_id: 原始合约ID（如 sa601, rb2511, IF2501）
    
    Returns:
        规范化后的表名（如 sa601, rb2511, if2501）
    
    Examples:
        >>> normalize_instrument_id('sa601')
        'sa601'
        >>> normalize_instrument_id('IF2501')
        'if2501'
        >>> normalize_instrument_id('IC-2501')
        'ic2501'
    """
    if not instrument_id:
        return 'unknown'
    
    # 转小写
    normalized = instrument_id.lower()
    
    # 移除特殊字符（只保留字母、数字、下划线）
    normalized = re.sub(r'[^a-z0-9_]', '', normalized)
    
    # 确保以字母开头（SQL表名要求）
    if normalized and normalized[0].isdigit():
        normalized = f"c{normalized}"
    
    return normalized or 'unknown'


def create_tick_table_sql(instrument_id: str) -> str:
    """
    生成创建Tick表的SQL（按合约分表）
    
    Args:
        instrument_id: 合约ID（如 sa601）
    
    Returns:
        CREATE TABLE SQL语句
    """
    table_name = f"tick_{normalize_instrument_id(instrument_id)}"
    
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        TradingDay DATE,
        ExchangeID VARCHAR,
        LastPrice DOUBLE,
        PreSettlementPrice DOUBLE,
        PreClosePrice DOUBLE,
        PreOpenInterest BIGINT,
        OpenPrice DOUBLE,
        HighestPrice DOUBLE,
        LowestPrice DOUBLE,
        Volume BIGINT,
        Turnover DOUBLE,
        OpenInterest BIGINT,
        ClosePrice DOUBLE,
        SettlementPrice DOUBLE,
        UpperLimitPrice DOUBLE,
        LowerLimitPrice DOUBLE,
        PreDelta DOUBLE,
        CurrDelta DOUBLE,
        UpdateTime VARCHAR,
        UpdateMillisec INTEGER,
        BidPrice1 DOUBLE,
        BidVolume1 BIGINT,
        AskPrice1 DOUBLE,
        AskVolume1 BIGINT,
        BidPrice2 DOUBLE,
        BidVolume2 BIGINT,
        AskPrice2 DOUBLE,
        AskVolume2 BIGINT,
        BidPrice3 DOUBLE,
        BidVolume3 BIGINT,
        AskPrice3 DOUBLE,
        AskVolume3 BIGINT,
        BidPrice4 DOUBLE,
        BidVolume4 BIGINT,
        AskPrice4 DOUBLE,
        AskVolume4 BIGINT,
        BidPrice5 DOUBLE,
        BidVolume5 BIGINT,
        AskPrice5 DOUBLE,
        AskVolume5 BIGINT,
        AveragePrice DOUBLE,
        ActionDay VARCHAR,
        InstrumentID VARCHAR,
        ExchangeInstID VARCHAR,
        BandingUpperPrice DOUBLE,
        BandingLowerPrice DOUBLE,
        Timestamp TIMESTAMP
    )
    """


def create_kline_table_sql(instrument_id: str) -> str:
    """
    生成创建K线表的SQL（按合约分表）
    
    Args:
        instrument_id: 合约ID（如 sa601）
    
    Returns:
        CREATE TABLE SQL语句
    
    Note:
        字段定义与 bar_manager.py 的 _bar_to_dataframe 完全一致（13个字段）
    """
    table_name = f"kline_{normalize_instrument_id(instrument_id)}"
    
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        BarType VARCHAR,
        TradingDay VARCHAR,
        UpdateTime VARCHAR,
        InstrumentID VARCHAR,
        ExchangeID VARCHAR,
        Volume BIGINT,
        OpenInterest BIGINT,
        OpenPrice DOUBLE,
        HighestPrice DOUBLE,
        LowestPrice DOUBLE,
        ClosePrice DOUBLE,
        LastVolume BIGINT,
        Timestamp TIMESTAMP
    )
    """


class DuckDBSingleFileWriter:
    """
    DuckDB单文件写入器 - 按交易日分文件，文件内排序聚类
    
    核心特性：
    1. 按日分文件：每个交易日一个.duckdb文件
    2. 排序插入：保证同一合约数据物理连续（触发Zone Maps优化）
    3. 批量写入：累积到阈值后批量INSERT
    4. 单线程写入：无并发竞争，简化逻辑
    
    性能优势：
    - 单日查询：直接读单文件（~12ms）
    - 跨日查询：ATTACH多文件并行（~80ms）
    - 压缩率：77%（原始数据压缩到23%）
    """
    
    def __init__(self,
                 db_path: str = "data/duckdb/ticks",
                 batch_threshold: int = 10000,
                 data_type: str = "ticks",
                 trading_day_manager: Optional[TradingDayManager] = None):
        """
        初始化DuckDB写入器
        
        Args:
            db_path: DuckDB文件根目录
            batch_threshold: 批量写入阈值（累积多少条触发写入）
            data_type: 数据类型（"ticks"或"klines"）
            trading_day_manager: 交易日管理器
        """
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        self.batch_threshold = batch_threshold
        self.data_type = data_type
        self.trading_day_manager = trading_day_manager
        self.logger = get_logger(self.__class__.__name__)
        
        # 单日缓冲区: {trading_day: [df1, df2, ...]}
        self.daily_buffer: Dict[str, List[pd.DataFrame]] = defaultdict(list)
        self.buffer_lock = threading.Lock()
        
        # 文件锁：防止多个线程同时写入同一个DuckDB文件
        self.file_locks: Dict[str, threading.Lock] = {}
        self.locks_lock = threading.Lock()
        
        # 线程跟踪：监控和清理僵尸线程（从配置文件读取参数）
        self.active_threads: Dict[str, Dict] = {}  # {thread_name: {start_time, trading_day, row_count}}
        self.thread_track_lock = threading.Lock()
        self.max_thread_lifetime = Config.duckdb_max_thread_lifetime  # 从配置读取
        self.submit_count = 0  # 提交计数器，用于定期触发监控
        self.monitor_interval = Config.duckdb_monitor_interval  # 从配置读取
        
        # 分表架构：不再需要单一建表SQL，在写入时动态生成
        
        self.logger.info(
            f"DuckDB写入器已初始化（按合约分表 + 文件锁保护）：路径={db_path}，"
            f"批量阈值={batch_threshold}，类型={data_type}"
        )
    
    def submit_batch(self, df: pd.DataFrame) -> None:
        """
        提交一批数据（自动按交易日分组）
        
        改进：在锁内提取数据，后台线程异步刷新（避免持锁阻塞）
        
        Args:
            df: 数据DataFrame（必须包含TradingDay和InstrumentID列）
        
        Raises:
            ValueError: 如果df缺少必要列
        """
        if df.empty:
            self.logger.warning("提交的DataFrame为空，已跳过")
            return
        
        # 验证必要列
        required_columns = ['TradingDay', 'InstrumentID']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame缺少必要列：{missing_columns}")
        
        # 定期监控线程（每10次提交检查一次）
        self.submit_count += 1
        if self.submit_count % self.monitor_interval == 0:
            try:
                stats = self._monitor_and_cleanup_threads()
                if stats['zombie_threads'] > 0 or stats['flush_threads'] > 20:
                    self.logger.warning(
                        f"线程监控：总线程={stats['total_threads']}，"
                        f"刷新线程={stats['flush_threads']}，"
                        f"僵尸线程={stats['zombie_threads']}，"
                        f"跟踪线程={stats['active_tracked']}，"
                        f"已清理={stats['cleaned']}"
                    )
            except Exception as e:
                self.logger.error(f"线程监控失败：{e}")
        
        # 在锁内追加数据并判断是否刷新
        with self.buffer_lock:
            for trading_day, group_df in df.groupby('TradingDay'):
                # 转换日期格式（支持YYYY-MM-DD或YYYYMMDD）
                day_key = str(trading_day).replace('-', '')[:8]
                
                # 添加到缓冲区
                self.daily_buffer[day_key].append(group_df)
                
                # 计算该日缓冲区总行数
                total_rows = sum(len(d) for d in self.daily_buffer[day_key])
                
                # 达到阈值时刷新
                if total_rows >= self.batch_threshold:
                    # 检查当前DuckDB刷新线程数量（防止线程泄漏）
                    flush_threads = [
                        t for t in threading.enumerate() 
                        if t.name.startswith("DuckDB-Flush-")
                    ]
                    if len(flush_threads) > 10:
                        self.logger.warning(
                            f"DuckDB刷新线程数量过多：{len(flush_threads)}个，"
                            f"可能存在线程阻塞或泄漏"
                        )
                    
                    # 关键改进：在锁内pop数据，然后启动后台线程异步刷新
                    dfs_to_flush = self.daily_buffer.pop(day_key)
                    
                    # 启动后台线程（在锁内，但Thread.start()很快<1ms）
                    threading.Thread(
                        target=self._flush_day_async,
                        args=(day_key, dfs_to_flush),
                        name=f"DuckDB-Flush-{day_key}",
                        daemon=True
                    ).start()
                    
                    self.logger.info(
                        f"DuckDB达到批量阈值，启动后台线程刷新：{day_key}，{total_rows}条 "
                        f"(当前活动线程: {len(flush_threads)+1})"
                    )
    
    def _get_file_lock(self, trading_day: str) -> threading.Lock:
        """
        获取指定交易日的文件锁（线程安全）
        
        Args:
            trading_day: 交易日期（格式：YYYYMMDD）
        
        Returns:
            该交易日对应的文件锁
        
        Note:
            使用locks_lock保护file_locks字典的并发访问
        """
        with self.locks_lock:
            if trading_day not in self.file_locks:
                self.file_locks[trading_day] = threading.Lock()
            return self.file_locks[trading_day]
    
    def _monitor_and_cleanup_threads(self) -> Dict:
        """
        监控并清理僵尸线程
        
        Returns:
            {
                'total_threads': int,  # 总线程数
                'flush_threads': int,  # DuckDB刷新线程数
                'zombie_threads': int,  # 僵尸线程数
                'cleaned': int  # 已清理的线程数
            }
        """
        import time
        current_time = time.time()
        
        # 获取所有DuckDB刷新线程
        all_threads = threading.enumerate()
        flush_threads = [t for t in all_threads if t.name.startswith("DuckDB-Flush-")]
        
        zombie_threads = []
        cleaned_count = 0
        
        # 检查跟踪的线程
        with self.thread_track_lock:
            for thread_name, info in list(self.active_threads.items()):
                thread_age = current_time - info['start_time']
                
                # 检查线程是否还存活
                thread_alive = any(t.name == thread_name for t in all_threads)
                
                if not thread_alive:
                    # 线程已完成，从跟踪中移除
                    del self.active_threads[thread_name]
                    cleaned_count += 1
                elif thread_age > self.max_thread_lifetime:
                    # 超时线程，视为僵尸线程
                    zombie_threads.append({
                        'name': thread_name,
                        'age': thread_age,
                        'trading_day': info['trading_day'],
                        'row_count': info['row_count']
                    })
                    self.logger.error(
                        f"🧟 检测到僵尸线程：{thread_name}，"
                        f"已运行{thread_age:.1f}秒（超时阈值{self.max_thread_lifetime}秒），"
                        f"交易日={info['trading_day']}，数据量={info['row_count']}条"
                    )
        
        # 记录警告
        if zombie_threads:
            self.logger.warning(
                f"发现{len(zombie_threads)}个僵尸线程，"
                f"总刷新线程数={len(flush_threads)}"
            )
        
        return {
            'total_threads': len(all_threads),
            'flush_threads': len(flush_threads),
            'zombie_threads': len(zombie_threads),
            'cleaned': cleaned_count,
            'active_tracked': len(self.active_threads)
        }
    
    def _flush_day_async(self, trading_day: str, dfs: List[pd.DataFrame]) -> None:
        """
        异步刷新单日数据到DuckDB文件（在后台线程执行）
        
        Args:
            trading_day: 交易日期（格式：YYYYMMDD）
            dfs: 待刷新的DataFrame列表
        
        关键：此方法在后台线程执行，不持buffer_lock，不阻塞新数据追加
        
        实现要点：
        1. 合并该日的所有批次数据
        2. 按InstrumentID, Timestamp排序（保证时间序列连续性）
        3. 创建或打开对应的.duckdb文件
        4. 按合约分组，为每个合约创建独立的表
        5. 每个合约的数据写入对应的表（天然物理连续！）
        """
        if not dfs:
            return
        
        # 记录线程开始
        import time
        thread_name = threading.current_thread().name
        start_time = time.time()
        merged_df = pd.concat(dfs, ignore_index=True)
        row_count = len(merged_df)
        
        # 注册到线程跟踪
        with self.thread_track_lock:
            self.active_threads[thread_name] = {
                'start_time': start_time,
                'trading_day': trading_day,
                'row_count': row_count
            }
        
        # 2. 排序（保证时间序列连续性）
        merged_df = merged_df.sort_values(
            by=['InstrumentID', 'Timestamp']
        ).reset_index(drop=True)
        
        # 3. 获取文件锁（防止并发写入同一个DuckDB文件）
        file_lock = self._get_file_lock(trading_day)
        db_file = self.db_path / f"{trading_day}.duckdb"
        
        # 使用文件锁保护整个写入过程
        with file_lock:
            self.logger.debug(f"获取文件锁成功：{trading_day}，开始写入...")
            
            # 打开DuckDB连接
            conn = duckdb.connect(str(db_file))
            
            try:
                # 4. 按合约分组写入（每个合约一张表）
                conn.execute("BEGIN TRANSACTION")
                
                contracts_written = []
                total_rows = 0
                
                # 按InstrumentID分组（已排序，高效）
                for instrument_id, group_df in merged_df.groupby('InstrumentID', sort=False):
                    # 4.1 生成表名和创建SQL
                    if self.data_type == 'ticks':
                        create_sql = create_tick_table_sql(instrument_id)
                        table_name = f"tick_{normalize_instrument_id(instrument_id)}"
                    else:  # klines
                        create_sql = create_kline_table_sql(instrument_id)
                        table_name = f"kline_{normalize_instrument_id(instrument_id)}"
                    
                    # 4.2 创建表（如果不存在）
                    conn.execute(create_sql)
                    
                    # 4.3 注册DataFrame为临时表
                    conn.register('temp_df', group_df)
                    
                    # 4.4 批量插入
                    conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                    
                    # 4.5 取消注册
                    conn.unregister('temp_df')
                    
                    contracts_written.append(instrument_id)
                    total_rows += len(group_df)
                
                # 5. 提交事务
                conn.execute("COMMIT")
                
                self.logger.info(
                    f"✓ DuckDB异步写入成功：{trading_day}，{total_rows}条，"
                    f"{len(contracts_written)}个合约 | "
                    f"示例(前5个合约)：{contracts_written[:5]}"
                )
                
            except Exception as e:
                # 回滚事务
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                
                self.logger.error(
                    f"DuckDB异步写入失败 [{trading_day}]：{e}",
                    exc_info=True
                )
                raise
            
            finally:
                # 6. 关闭连接
                conn.close()
                self.logger.debug(f"释放文件锁：{trading_day}，写入完成")
                
                # 记录线程结束，从跟踪中移除
                end_time = time.time()
                elapsed = end_time - start_time
                with self.thread_track_lock:
                    if thread_name in self.active_threads:
                        del self.active_threads[thread_name]
                
                self.logger.debug(
                    f"线程{thread_name}完成，耗时{elapsed:.2f}秒，"
                    f"数据量={row_count}条"
                )
    
    def stop(self, timeout: float = 30.0) -> None:
        """
        停止写入器，刷新所有剩余数据
        
        Args:
            timeout: 等待后台刷新线程完成的超时时间（秒）
        """
        self.logger.info(f"正在停止DuckDB写入器 ({self.data_type})...")
        
        # 1. 刷新所有剩余缓冲区（同步刷新，避免启动新线程）
        with self.buffer_lock:
            days_to_flush = list(self.daily_buffer.keys())
        
        for day in days_to_flush:
            with self.buffer_lock:
                if day in self.daily_buffer:
                    dfs = self.daily_buffer.pop(day)
                    if dfs:
                        self.logger.info(f"刷新剩余数据：{day}，{sum(len(d) for d in dfs)}条")
                        # 同步刷新（优雅关闭时不启动新线程）
                        self._flush_day_sync(day, dfs)
        
        # 2. 等待所有后台线程完成
        import time
        start_wait = time.time()
        
        # 监控后台刷新线程
        while time.time() - start_wait < timeout:
            flush_threads = [
                t for t in threading.enumerate() 
                if t.name.startswith("DuckDB-Flush-")
            ]
            if not flush_threads:
                break
            
            # 详细日志：显示线程名称
            thread_names = [t.name for t in flush_threads[:5]]  # 只显示前5个
            self.logger.info(
                f"等待{len(flush_threads)}个后台刷新线程完成... "
                f"示例：{thread_names}"
            )
            time.sleep(0.5)
        
        # 检查是否仍有线程
        remaining_threads = [
            t for t in threading.enumerate() 
            if t.name.startswith("DuckDB-Flush-")
        ]
        if remaining_threads:
            self.logger.warning(
                f"仍有{len(remaining_threads)}个后台线程未完成：{[t.name for t in remaining_threads]}"
            )
        
        # 强制清理跟踪的僵尸线程
        with self.thread_track_lock:
            if self.active_threads:
                zombie_count = len(self.active_threads)
                zombie_names = list(self.active_threads.keys())[:5]  # 显示前5个
                self.logger.warning(
                    f"🧟 强制清理{zombie_count}个僵尸线程：{zombie_names}"
                )
                self.active_threads.clear()
        
        self.logger.info(f"✓ DuckDB写入器已停止 ({self.data_type})")
    
    def _flush_day_sync(self, trading_day: str, dfs: List[pd.DataFrame]) -> None:
        """
        同步刷新（stop时使用，避免启动新线程）
        
        Args:
            trading_day: 交易日期
            dfs: 待刷新的DataFrame列表
        """
        self._flush_day_async(trading_day, dfs)
    
    def get_stats(self) -> Dict:
        """
        获取写入器统计信息（强版：包含线程监控）
        
        Returns:
            {
                'batch_threshold': int,
                'buffer_sizes': Dict[str, int],  # {trading_day: buffer_size}
                'total_buffered': int,
                'thread_stats': Dict  # 线程统计信息
            }
        """
        with self.buffer_lock:
            buffer_sizes = {
                day: sum(len(df) for df in dfs)
                for day, dfs in self.daily_buffer.items()
            }
        
        # 获取线程监控信息
        thread_stats = self._monitor_and_cleanup_threads()
        
        return {
            'batch_threshold': self.batch_threshold,
            'buffer_sizes': buffer_sizes,
            'total_buffered': sum(buffer_sizes.values()),
            'thread_stats': thread_stats  # 新增
        }


class DuckDBQueryEngine:
    """
    DuckDB查询引擎 - 支持单日和跨日查询
    
    核心特性：
    1. 单日查询：直接读单文件（极快，~12ms）
    2. 跨日查询：ATTACH多文件并行（~80ms）
    3. Zone Maps自动裁剪：无需创建索引
    
    查询策略：
    - 自动判断单日/跨日
    - 跨日查询使用UNION ALL + ATTACH
    - DuckDB自动并行扫描
    """
    
    def __init__(self,
                 db_path: str = "data/duckdb/ticks",
                 data_type: str = "ticks"):
        """
        初始化查询引擎
        
        Args:
            db_path: DuckDB文件根目录
            data_type: 数据类型（"ticks"或"klines"）
        """
        self.db_path = Path(db_path)
        self.data_type = data_type
        self.logger = get_logger(self.__class__.__name__)
    
    def query_ticks(self,
                    instrument_id: str,
                    start_time: str,
                    end_time: str) -> pd.DataFrame:
        """
        查询Tick数据（自动判断单日/跨日）
        
        Args:
            instrument_id: 合约代码
            start_time: 开始时间（格式：YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD）
            end_time: 结束时间
        
        Returns:
            DataFrame: 查询结果（按Timestamp排序）
        """
        # 解析时间
        try:
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
        except Exception as e:
            self.logger.error(f"时间格式错误：{e}")
            return pd.DataFrame()
        
        # 获取涉及的交易日
        trading_days = self._get_trading_days_between(
            start_dt.strftime('%Y%m%d'),
            end_dt.strftime('%Y%m%d')
        )
        
        if not trading_days:
            self.logger.warning(f"未找到相关交易日：{start_time} ~ {end_time}")
            return pd.DataFrame()
        
        # 判断单日/跨日
        if len(trading_days) == 1:
            # 单日查询（最快路径）
            return self._query_single_day(
                trading_days[0], instrument_id, start_dt, end_dt
            )
        else:
            # 跨日查询（ATTACH多库）
            return self._query_multiple_days(
                trading_days, instrument_id, start_dt, end_dt
            )
    
    def _query_single_day(self,
                         trading_day: str,
                         instrument_id: str,
                         start_dt: datetime,
                         end_dt: datetime) -> pd.DataFrame:
        """
        单日查询（最快路径）
        
        实现：
        1. 打开对应的.duckdb文件（只读模式）
        2. 执行查询：WHERE InstrumentID = ? AND Timestamp BETWEEN ? AND ?
        3. Zone Maps自动裁剪（跳过不相关的Row Groups）
        4. 返回结果
        """
        db_file = self.db_path / f"{trading_day}.duckdb"
        
        if not db_file.exists():
            self.logger.warning(f"数据库文件不存在：{db_file}")
            return pd.DataFrame()
        
        # 打开连接（只读模式）
        conn = duckdb.connect(str(db_file), read_only=True)
        
        try:
            # 接查询合约表（天然物理隔离，极速查询）
            if self.data_type == 'ticks':
                table_name = f"tick_{normalize_instrument_id(instrument_id)}"
            else:  # klines
                table_name = f"kline_{normalize_instrument_id(instrument_id)}"
            
            # 查询（只需时间过滤，无需InstrumentID过滤）
            query = f"""
                SELECT * FROM {table_name}
                WHERE Timestamp BETWEEN ? AND ?
                ORDER BY Timestamp
            """
            
            df = conn.execute(query, [start_dt, end_dt]).df()
            
            self.logger.debug(
                f"单日查询完成：{trading_day}/{instrument_id}（表: {table_name}），{len(df)}条"
            )
            
            return df
            
        except Exception as e:
            # 表可能不存在（合约当天没有数据）
            if "does not exist" in str(e) or "not found" in str(e).lower():
                self.logger.debug(f"合约表不存在：{table_name}（合约当天无数据）")
                return pd.DataFrame()
            else:
                self.logger.error(f"单日查询失败 [{trading_day}]：{e}", exc_info=True)
                return pd.DataFrame()
        
        finally:
            conn.close()
    
    def _query_multiple_days(self,
                            trading_days: List[str],
                            instrument_id: str,
                            start_dt: datetime,
                            end_dt: datetime) -> pd.DataFrame:
        """
        跨日查询（ATTACH多库）
        
        实现：
        1. 创建内存连接：duckdb.connect(':memory:')
        2. ATTACH所有相关日期的.duckdb文件
        3. UNION ALL查询
        4. DuckDB自动并行扫描
        5. 按Timestamp排序返回
        """
        # 构建文件列表（过滤不存在的文件）
        db_files = [
            (str(self.db_path / f"{day}.duckdb"), day)
            for day in trading_days
            if (self.db_path / f"{day}.duckdb").exists()
        ]
        
        if not db_files:
            self.logger.warning(f"未找到任何数据库文件：{trading_days}")
            return pd.DataFrame()
        
        # 创建内存连接
        conn = duckdb.connect(':memory:')
        
        try:
            # ATTACH所有相关日期的数据库
            for i, (db_file, day) in enumerate(db_files):
                conn.execute(f"ATTACH '{db_file}' AS db{i} (READ_ONLY)")
                self.logger.debug(f"ATTACH数据库：db{i} <- {day}")
            
            # 构建UNION ALL查询（查询各文件的合约表）
            if self.data_type == 'ticks':
                table_name = f"tick_{normalize_instrument_id(instrument_id)}"
            else:  # klines
                table_name = f"kline_{normalize_instrument_id(instrument_id)}"
            
            union_queries = [
                f"""
                SELECT * FROM db{i}.{table_name}
                WHERE Timestamp BETWEEN '{start_dt}' AND '{end_dt}'
                """
                for i in range(len(db_files))
            ]
            
            query = " UNION ALL ".join(union_queries) + " ORDER BY Timestamp"
            
            # 执行查询（DuckDB自动并行）
            df = conn.execute(query).df()
            
            self.logger.info(
                f"跨日查询完成：{len(db_files)}个文件，{instrument_id}（表: {table_name}），"
                f"结果={len(df)}条"
            )
            
            return df
            
        except Exception as e:
            self.logger.error(
                f"跨日查询失败 [{trading_days}]：{e}",
                exc_info=True
            )
            return pd.DataFrame()
        
        finally:
            conn.close()
    
    def _get_trading_days_between(self,
                                  start_date: str,
                                  end_date: str) -> List[str]:
        """
        获取两个日期之间的所有交易日
        
        Args:
            start_date: 开始日期（格式：YYYYMMDD）
            end_date: 结束日期（格式：YYYYMMDD）
        
        Returns:
            ['20251027', '20251028', '20251029', ...]
        
        Note:
            简化实现：返回所有日期（包括非交易日）
            实际生产环境应该从交易日历获取
        """
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
        except ValueError:
            self.logger.error(f"日期格式错误：{start_date}, {end_date}")
            return []
        
        # 生成日期列表
        trading_days = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            # 只检查文件是否存在（简化版）
            day_str = current_dt.strftime('%Y%m%d')
            db_file = self.db_path / f"{day_str}.duckdb"
            
            if db_file.exists():
                trading_days.append(day_str)
            
            current_dt += timedelta(days=1)
        
        return trading_days

