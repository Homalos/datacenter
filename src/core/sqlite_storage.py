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
import pandas as pd  # type: ignore
import threading
import queue
import time
from pathlib import Path
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
                 retention_days: int = 7,
                 trading_day_manager = None):
        """
        初始化SQLite存储层（按交易日+合约分库存储）
        
        Args:
            db_path: 数据库文件根目录
            retention_days: 数据保留天数（默认7天）
            trading_day_manager: 交易日管理器
            
        数据库文件结构：
            - data/db/tick/20251027/SA601.db
            - data/db/kline/20251027/SA601.db
        """
        self.db_root = Path(db_path)
        self.db_root.mkdir(parents=True, exist_ok=True)
        
        self.tick_db_root = self.db_root / "tick"
        self.kline_db_root = self.db_root / "kline"
        self.tick_db_root.mkdir(parents=True, exist_ok=True)
        self.kline_db_root.mkdir(parents=True, exist_ok=True)
        
        self.logger = get_logger(self.__class__.__name__)
        self.retention_days = retention_days
        self.trading_day_manager = trading_day_manager
        
        # 向后兼容属性（查询方法暂未重构为分库查询）
        # TODO: 重构查询方法以支持分库查询
        self.tick_db_file = self.tick_db_root / "deprecated_single_file.db"
        self.kline_db_file = self.kline_db_root / "deprecated_single_file.db"
        
        # 写入队列（解决并发锁问题，支持动态扩容）
        self._write_queue: "queue.Queue[tuple]" = queue.Queue(maxsize=0)  # 无限大小，动态扩容
        self._write_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        
        # 队列监控阈值
        self._queue_warn_threshold = 5000   # 软性告警阈值
        self._queue_critical_threshold = 20000  # 严重告警阈值
        self._last_queue_warn_time = 0.0  # 上次告警时间（避免日志刷屏）
        
        # 启动写入线程
        self._start_write_thread()
        
        self.logger.info(
            f"SQLite存储层初始化完成（按交易日+合约分库，动态扩容队列），"
            f"数据保留{retention_days}天，已启动单线程写入队列"
        )
    
    def _get_db_path(self, data_type: str, instrument_id: str, trading_day: str) -> Path:
        """
        获取数据库文件路径（按交易日+合约分库）
        
        Args:
            data_type: 数据类型（'tick' 或 'kline'）
            instrument_id: 合约代码
            trading_day: 交易日（YYYYMMDD）
            
        Returns:
            数据库文件路径（例如：data/db/tick/20251027/SA601.db）
        """
        if data_type == "tick":
            root = self.tick_db_root
        elif data_type == "kline":
            root = self.kline_db_root
        else:
            raise ValueError(f"未知的数据类型: {data_type}")
        
        # data/db/tick/20251027/
        day_dir = root / trading_day
        day_dir.mkdir(parents=True, exist_ok=True)
        
        # data/db/tick/20251027/SA601.db
        return day_dir / f"{instrument_id}.db"
    
    def _init_tick_table(self, conn) -> None:
        """初始化Tick表结构"""
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_id TEXT NOT NULL,
                    exchange_id TEXT NOT NULL,
                    exchange_inst_id TEXT,
                    datetime TIMESTAMP NOT NULL,
                    trading_day TEXT NOT NULL,
                    action_day TEXT,
                    timestamp TIMESTAMP,
                    update_time TEXT,
                    update_millisec INTEGER,
                    last_price REAL,
                    open_price REAL,
                    highest_price REAL,
                    lowest_price REAL,
                    close_price REAL,
                    average_price REAL,
                    pre_settlement_price REAL,
                    pre_close_price REAL,
                    pre_open_interest REAL,
                    settlement_price REAL,
                    upper_limit_price REAL,
                    lower_limit_price REAL,
                    banding_upper_price REAL,
                    banding_lower_price REAL,
                    volume INTEGER,
                    turnover REAL,
                    open_interest REAL,
                    pre_delta REAL,
                    curr_delta REAL,
                    bid_price_1 REAL,
                    bid_volume_1 INTEGER,
                    ask_price_1 REAL,
                    ask_volume_1 INTEGER,
                    bid_price_2 REAL,
                    bid_volume_2 INTEGER,
                    ask_price_2 REAL,
                    ask_volume_2 INTEGER,
                    bid_price_3 REAL,
                    bid_volume_3 INTEGER,
                    ask_price_3 REAL,
                    ask_volume_3 INTEGER,
                    bid_price_4 REAL,
                    bid_volume_4 INTEGER,
                    ask_price_4 REAL,
                    ask_volume_4 INTEGER,
                    bid_price_5 REAL,
                    bid_volume_5 INTEGER,
                    ask_price_5 REAL,
                    ask_volume_5 INTEGER,
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
    
    def _init_kline_table(self, conn) -> None:
        """初始化K线表结构"""
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_id TEXT NOT NULL,
                    exchange_id TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    bar_type TEXT,
                    datetime TIMESTAMP NOT NULL,
                    timestamp TIMESTAMP,
                    trading_day TEXT,
                    update_time TEXT,
                    open REAL,
                    open_price REAL,
                    high REAL,
                    high_price REAL,
                    low REAL,
                    low_price REAL,
                    close REAL,
                    close_price REAL,
                    volume INTEGER,
                    last_volume INTEGER,
                    turnover REAL,
                    open_interest REAL,
                    last_open_interest REAL,
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
    
    def _check_queue_health(self) -> None:
        """
        检查队列健康状态并触发告警
        
        采用分级告警机制：
        - 5000条：软性告警（1分钟最多1次）
        - 20000条：严重告警（立即告警）
        """
        queue_size = self._write_queue.qsize()
        current_time = time.time()
        
        # 严重告警（超过20000条）
        if queue_size >= self._queue_critical_threshold:
            self.logger.error(
                f"🔴 CRITICAL: SQLite写入队列严重积压！"
                f"当前: {queue_size}条（超过严重阈值{self._queue_critical_threshold}），"
                f"可能存在性能瓶颈，请检查系统状态！"
            )
            self._last_queue_warn_time = current_time
            
        # 软性告警（超过5000条，且距离上次告警超过60秒）
        elif queue_size >= self._queue_warn_threshold:
            if current_time - self._last_queue_warn_time > 60:  # 60秒内只告警一次
                self.logger.warning(
                    f"⚠️  SQLite写入队列积压: {queue_size}条 "
                    f"(告警阈值: {self._queue_warn_threshold})"
                )
                self._last_queue_warn_time = current_time
    
    def _start_write_thread(self) -> None:
        """启动单独的写入线程（解决并发锁问题）"""
        if self._write_thread is None or not self._write_thread.is_alive():
            self._stop_event.clear()
            self._write_thread = threading.Thread(
                target=self._write_worker,
                name="SQLiteWriteThread",
                daemon=True
            )
            self._write_thread.start()
            self.logger.info("SQLite写入线程已启动")
    
    def _write_worker(self) -> None:
        """写入线程工作函数 - 从队列中取任务并串行写入"""
        self.logger.info("SQLite写入线程开始工作...")
        
        while not self._stop_event.is_set():
            try:
                # 从队列获取写入任务（设置超时以便检查stop_event）
                task = self._write_queue.get(timeout=1.0)
                
                if task[0] == "stop":  # 停止信号
                    break
                
                # 执行写入任务
                task_type, args = task
                try:
                    if task_type == "tick":
                        self._do_write_ticks(args)
                    elif task_type == "kline":
                        self._do_write_klines(args)
                except Exception as e:
                    self.logger.error(f"写入任务执行失败: {e}", exc_info=True)
                finally:
                    self._write_queue.task_done()
                    
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"写入线程异常: {e}", exc_info=True)
        
        self.logger.info("SQLite写入线程已停止")
    
    def get_queue_stats(self) -> dict:
        """
        获取队列统计信息
        
        Returns:
            包含队列状态的字典
        """
        queue_size = self._write_queue.qsize()
        return {
            "queue_size": queue_size,
            "warn_threshold": self._queue_warn_threshold,
            "critical_threshold": self._queue_critical_threshold,
            "health_status": (
                "critical" if queue_size >= self._queue_critical_threshold
                else "warning" if queue_size >= self._queue_warn_threshold
                else "healthy"
            ),
            "usage_percent": round(queue_size / self._queue_warn_threshold * 100, 2) if queue_size > 0 else 0
        }
    
    def stop(self) -> None:
        """停止写入线程"""
        if self._write_thread and self._write_thread.is_alive():
            self.logger.info("正在停止SQLite写入线程...")
            self._stop_event.set()
            self._write_queue.put(("stop", None))  # 发送停止信号
            self._write_thread.join(timeout=5.0)
            
            # 输出队列统计
            stats = self.get_queue_stats()
            self.logger.info(
                f"SQLite写入线程已停止，队列剩余: {stats['queue_size']}条，"
                f"状态: {stats['health_status']}"
            )
    
    @contextmanager
    def _get_conn(self, db_file: Path):
        """
        获取数据库连接（上下文管理器，支持重试机制）
        
        Args:
            db_file: 数据库文件路径
        
        Yields:
            数据库连接
        """
        max_retries = 3
        retry_delay = 0.1  # 100ms
        
        for attempt in range(max_retries):
            try:
                # 设置超时为30秒（避免长时间锁定）
                conn = sqlite3.connect(str(db_file), timeout=30.0, check_same_thread=False)
                try:
                    yield conn
                    conn.commit()
                    return
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"数据库操作失败: {e}", exc_info=True)
                    raise
                finally:
                    conn.close()
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay * (attempt + 1))  # 递增延迟
                    continue
                else:
                    raise
    
    def save_ticks(self, df: pd.DataFrame) -> None:
        """
        批量保存Tick数据到SQLite（异步，加入写入队列）
        
        Args:
            df: Tick数据DataFrame
            
        Note:
            Tick数据不允许丢弃！采用无限大小队列，动态扩容，配合分级告警
        """
        if df.empty:
            return
        
        try:
            # 检查队列健康状态（触发告警但不阻塞）
            self._check_queue_health()
            
            # 将写入任务加入队列（无限大小队列，不会阻塞）
            self._write_queue.put(("tick", df.copy()), block=False)
            
        except Exception as e:
            self.logger.critical(
                f"❌ FATAL: 加入Tick写入队列失败: {e}，"
                f"当前队列大小: {self._write_queue.qsize()}",
                exc_info=True
            )
            raise  # 抛出异常让上层知道有严重问题
    
    def _do_write_ticks(self, df: pd.DataFrame) -> None:
        """
        实际执行Tick数据写入（在写入线程中调用，按合约+交易日分库）
        
        Args:
            df: Tick数据DataFrame
        """
        try:
            # 数据清洗：确保必要字段存在
            required_cols = ["instrument_id", "exchange_id", "datetime", "trading_day"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"Tick数据缺少必要字段: {required_cols}")
                return
            
            # 按合约和交易日分组
            grouped = df.groupby(["instrument_id", "trading_day"])
            
            for (instrument_id, trading_day), group_df in grouped:
                # 获取该合约+交易日的数据库文件路径
                db_path = self._get_db_path("tick", str(instrument_id), str(trading_day))
                
                try:
                    conn = sqlite3.connect(str(db_path), timeout=30.0, check_same_thread=False)
                    try:
                        # 初始化表（如果是新数据库）
                        self._init_tick_table(conn)
                        
                        # 写入数据
                        group_df.to_sql('ticks', conn, if_exists='append', index=False)
                        conn.commit()
                        
                        self.logger.debug(
                            f"✓ Tick数据写入成功: {instrument_id} @ {trading_day} "
                            f"({len(group_df)}条) -> {db_path.name}"
                        )
                    except Exception as e:
                        conn.rollback()
                        self.logger.error(f"写入Tick数据失败 [{instrument_id}@{trading_day}]: {e}", exc_info=True)
                    finally:
                        conn.close()
                        
                except Exception as e:
                    self.logger.error(f"连接Tick数据库失败 [{instrument_id}@{trading_day}]: {e}", exc_info=True)
                    
        except Exception as e:
            self.logger.error(f"Tick数据分组写入失败: {e}", exc_info=True)
    
    def save_klines(self, df: pd.DataFrame) -> None:
        """
        批量保存K线数据到SQLite（异步，加入写入队列）
        
        Args:
            df: K线数据DataFrame
            
        Note:
            使用无限大小队列，动态扩容
        """
        if df.empty:
            return
        
        try:
            # 检查队列健康状态
            self._check_queue_health()
            
            # 将写入任务加入队列（无限大小队列，不会阻塞）
            self._write_queue.put(("kline", df.copy()), block=False)
            
        except Exception as e:
            self.logger.error(
                f"加入K线写入队列失败: {e}，"
                f"当前队列大小: {self._write_queue.qsize()}",
                exc_info=True
            )
    
    def _do_write_klines(self, df: pd.DataFrame) -> None:
        """
        实际执行K线数据写入（在写入线程中调用，按合约+交易日分库）
        
        Args:
            df: K线数据DataFrame
        """
        try:
            # 数据清洗：确保必要字段存在
            required_cols = ["instrument_id", "exchange_id", "interval", "datetime", "trading_day"]
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"K线数据缺少必要字段: {required_cols}")
                return
            
            # 按合约和交易日分组
            grouped = df.groupby(["instrument_id", "trading_day"])
            
            for (instrument_id, trading_day), group_df in grouped:
                # 获取该合约+交易日的数据库文件路径
                db_path = self._get_db_path("kline", str(instrument_id), str(trading_day))
                
                try:
                    conn = sqlite3.connect(str(db_path), timeout=30.0, check_same_thread=False)
                    try:
                        # 初始化表（如果是新数据库）
                        self._init_kline_table(conn)
                        
                        # 写入数据
                        group_df.to_sql('klines', conn, if_exists='append', index=False)
                        conn.commit()
                        
                        self.logger.debug(
                            f"✓ K线数据写入成功: {instrument_id} @ {trading_day} "
                            f"({len(group_df)}条) -> {db_path.name}"
                        )
                    except Exception as e:
                        conn.rollback()
                        self.logger.error(f"写入K线数据失败 [{instrument_id}@{trading_day}]: {e}", exc_info=True)
                    finally:
                        conn.close()
                        
                except Exception as e:
                    self.logger.error(f"连接K线数据库失败 [{instrument_id}@{trading_day}]: {e}", exc_info=True)
                    
        except Exception as e:
            self.logger.error(f"K线数据分组写入失败: {e}", exc_info=True)
    
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

