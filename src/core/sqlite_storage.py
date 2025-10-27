#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : sqlite_storage.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: SQLite存储层 - 用于近期数据的快速查询
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

from src.utils.log import get_logger


class SQLiteStorage:
    """
    SQLite存储层 - 用于近期数据的快速查询
    
    特点：
    1. 高速读写：WAL模式，支持并发
    2. 索引优化：合约代码+时间的复合索引
    3. 自动清理：定期删除已归档数据
    4. 轻量级：单文件存储，易于备份
    """
    
    def __init__(self, 
                 db_path: str = "data/db",
                 retention_days: int = 7):
        """
        初始化SQLite存储层
        
        Args:
            db_path: 数据库文件目录
            retention_days: 数据保留天数（默认7天）
        """
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        self.tick_db_file = self.db_path / "tick.db"
        self.kline_db_file = self.db_path / "kline.db"
        
        self.logger = get_logger(self.__class__.__name__)
        self.retention_days = retention_days
        
        # 初始化数据库
        self._init_databases()
        
        self.logger.info(f"SQLite存储层初始化完成，数据保留{retention_days}天")
    
    def _init_databases(self) -> None:
        """初始化数据库表结构"""
        # 初始化Tick数据库
        with self._get_conn(self.tick_db_file) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_id TEXT NOT NULL,
                    exchange_id TEXT NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    trading_day TEXT NOT NULL,
                    last_price REAL,
                    volume INTEGER,
                    open_interest REAL,
                    open_price REAL,
                    highest_price REAL,
                    lowest_price REAL,
                    bid_price_1 REAL,
                    bid_volume_1 INTEGER,
                    ask_price_1 REAL,
                    ask_volume_1 INTEGER,
                    turnover REAL,
                    average_price REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tick_symbol_time
                ON ticks(instrument_id, datetime)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tick_trading_day
                ON ticks(trading_day)
            """)
            
            # 启用WAL模式（提高并发性能）
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            
            self.logger.info("Tick数据库初始化完成")
        
        # 初始化K线数据库
        with self._get_conn(self.kline_db_file) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_id TEXT NOT NULL,
                    exchange_id TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    open_interest REAL,
                    trading_day TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_kline_symbol_interval_time
                ON klines(instrument_id, interval, datetime)
            """)
            
            # 启用WAL模式
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            
            self.logger.info("K线数据库初始化完成")
    
    @contextmanager
    def _get_conn(self, db_file: Path):
        """
        获取数据库连接（上下文管理器）
        
        Args:
            db_file: 数据库文件路径
        
        Yields:
            数据库连接
        """
        conn = sqlite3.connect(str(db_file), check_same_thread=False)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"数据库操作失败: {e}", exc_info=True)
            raise
        finally:
            conn.close()
    
    def save_ticks(self, df: pd.DataFrame) -> None:
        """
        批量保存Tick数据到SQLite
        
        Args:
            df: Tick数据DataFrame
        """
        if df.empty:
            return
        
        try:
            # 数据清洗：确保必要字段存在
            required_cols = ["instrument_id", "exchange_id", "datetime"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"Tick数据缺少必要字段: {required_cols}")
                return
            
            with self._get_conn(self.tick_db_file) as conn:
                # 使用事务批量插入（去重：替换已存在的数据）
                df.to_sql('ticks', conn, if_exists='append', index=False)
                self.logger.debug(f"保存 {len(df)} 条Tick数据到SQLite")
        
        except Exception as e:
            self.logger.error(f"保存Tick数据失败: {e}", exc_info=True)
    
    def save_klines(self, df: pd.DataFrame) -> None:
        """
        批量保存K线数据到SQLite
        
        Args:
            df: K线数据DataFrame
        """
        if df.empty:
            return
        
        try:
            # 数据清洗：确保必要字段存在
            required_cols = ["instrument_id", "exchange_id", "interval", "datetime"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"K线数据缺少必要字段: {required_cols}")
                return
            
            with self._get_conn(self.kline_db_file) as conn:
                # 使用事务批量插入
                df.to_sql('klines', conn, if_exists='append', index=False)
                self.logger.debug(f"保存 {len(df)} 条K线数据到SQLite")
        
        except Exception as e:
            self.logger.error(f"保存K线数据失败: {e}", exc_info=True)
    
    def query_ticks(self,
                    instrument_id: str,
                    start_time: str,
                    end_time: str) -> pd.DataFrame:
        """
        查询Tick数据
        
        Args:
            instrument_id: 合约代码
            start_time: 开始时间（ISO格式）
            end_time: 结束时间（ISO格式）
        
        Returns:
            Tick数据DataFrame
        """
        try:
            with self._get_conn(self.tick_db_file) as conn:
                query = """
                    SELECT * FROM ticks
                    WHERE instrument_id = ?
                    AND datetime >= ?
                    AND datetime <= ?
                    ORDER BY datetime
                """
                df = pd.read_sql_query(
                    query, 
                    conn, 
                    params=(instrument_id, start_time, end_time)
                )
                self.logger.debug(f"查询到 {len(df)} 条Tick数据")
                return df
        
        except Exception as e:
            self.logger.error(f"查询Tick数据失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def query_klines(self,
                     instrument_id: str,
                     interval: str,
                     start_time: str,
                     end_time: str) -> pd.DataFrame:
        """
        查询K线数据
        
        Args:
            instrument_id: 合约代码
            interval: K线周期
            start_time: 开始时间（ISO格式）
            end_time: 结束时间（ISO格式）
        
        Returns:
            K线数据DataFrame
        """
        try:
            with self._get_conn(self.kline_db_file) as conn:
                query = """
                    SELECT * FROM klines
                    WHERE instrument_id = ?
                    AND interval = ?
                    AND datetime >= ?
                    AND datetime <= ?
                    ORDER BY datetime
                """
                df = pd.read_sql_query(
                    query, 
                    conn, 
                    params=(instrument_id, interval, start_time, end_time)
                )
                self.logger.debug(f"查询到 {len(df)} 条K线数据")
                return df
        
        except Exception as e:
            self.logger.error(f"查询K线数据失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def get_archivable_data(self, cutoff_date: str) -> dict:
        """
        获取可归档的数据（超过保留期的数据）
        
        Args:
            cutoff_date: 截止日期，格式：YYYY-MM-DD
        
        Returns:
            {"ticks": DataFrame, "klines": DataFrame}
        """
        result = {}
        
        try:
            # 查询Tick数据
            with self._get_conn(self.tick_db_file) as conn:
                query = "SELECT * FROM ticks WHERE datetime < ?"
                result['ticks'] = pd.read_sql_query(query, conn, params=(cutoff_date,))
            
            # 查询K线数据
            with self._get_conn(self.kline_db_file) as conn:
                query = "SELECT * FROM klines WHERE datetime < ?"
                result['klines'] = pd.read_sql_query(query, conn, params=(cutoff_date,))
            
            self.logger.info(
                f"获取可归档数据: Tick={len(result['ticks'])}条, K线={len(result['klines'])}条"
            )
            return result
        
        except Exception as e:
            self.logger.error(f"获取可归档数据失败: {e}", exc_info=True)
            return {"ticks": pd.DataFrame(), "klines": pd.DataFrame()}
    
    def delete_archived_data(self, cutoff_date: str) -> None:
        """
        删除已归档的数据
        
        Args:
            cutoff_date: 截止日期，格式：YYYY-MM-DD
        """
        try:
            # 删除Tick数据
            with self._get_conn(self.tick_db_file) as conn:
                cursor = conn.execute("DELETE FROM ticks WHERE datetime < ?", (cutoff_date,))
                tick_count = cursor.rowcount
                self.logger.info(f"删除 {tick_count} 条已归档Tick数据")
            
            # 删除K线数据
            with self._get_conn(self.kline_db_file) as conn:
                cursor = conn.execute("DELETE FROM klines WHERE datetime < ?", (cutoff_date,))
                kline_count = cursor.rowcount
                self.logger.info(f"删除 {kline_count} 条已归档K线数据")
            
            # 执行VACUUM以回收空间
            self._vacuum_databases()
        
        except Exception as e:
            self.logger.error(f"删除已归档数据失败: {e}", exc_info=True)
    
    def _vacuum_databases(self) -> None:
        """压缩数据库，回收空间"""
        try:
            with self._get_conn(self.tick_db_file) as conn:
                conn.execute("VACUUM")
            
            with self._get_conn(self.kline_db_file) as conn:
                conn.execute("VACUUM")
            
            self.logger.info("数据库压缩完成")
        
        except Exception as e:
            self.logger.error(f"数据库压缩失败: {e}", exc_info=True)
    
    def get_statistics(self) -> dict:
        """
        获取存储统计信息
        
        Returns:
            统计信息字典
        """
        stats = {
            "retention_days": self.retention_days,
            "tick_count": 0,
            "kline_count": 0,
            "db_size_mb": 0.0
        }
        
        try:
            # 统计Tick数量
            with self._get_conn(self.tick_db_file) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM ticks")
                stats["tick_count"] = cursor.fetchone()[0]
            
            # 统计K线数量
            with self._get_conn(self.kline_db_file) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM klines")
                stats["kline_count"] = cursor.fetchone()[0]
            
            # 统计数据库文件大小
            if self.tick_db_file.exists():
                stats["db_size_mb"] += self.tick_db_file.stat().st_size / (1024 * 1024)
            if self.kline_db_file.exists():
                stats["db_size_mb"] += self.kline_db_file.stat().st_size / (1024 * 1024)
            
            stats["db_size_mb"] = round(stats["db_size_mb"], 2)
        
        except Exception as e:
            self.logger.error(f"获取统计信息失败: {e}", exc_info=True)
        
        return stats

