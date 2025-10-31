#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : event_bus.py
@Date       : 2025/9/18
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 事件总线 (多队列支持同步/异步)
特性：
1. 使用自定义线程池，支持线程池动态扩展
2. 高频行情事件分类：通过 event.event_type.startswith("market") 判断是否走 market 队列，否则走 general 队列。
3. 资源回收彻底：stop() 时确保 _threads、_executors、_async_task 清理干净。
4. 线程池异常兼容：调度时若线程池关闭则直接执行，不丢事件。
5. 定义事件类别映射（可扩展）
self._event_category_map = {
    "market": "market",     # 高频行情事件
    # "order": "order",     # 未来可以单独扩展
    # "account": "account", # 未来可以单独扩展
}
"""
import asyncio
import inspect
import queue
import signal
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Full

from src.core import trace_context
from src.core.event import Event, EventType
from src.utils.log.logger import get_logger


class EventBus:
    """
    多队列事件总线 (支持同步/异步统一接口)
    - 将事件按类别分发到不同队列，避免高频事件堵塞低频关键事件
    - 同步/异步统一接口：publish(event, async_mode=True/False)
    - 队列划分：market（行情高频）、general（普通事件）
    - 内置定时器：定期发布 EventType.TIMER 事件，支持秒级定时任务
    """
    def __init__(self,
                 context: str = "EventBus",
                 interval: int = 1,                     # 定时器间隔
                 timer_enabled: bool = True,            # 是否启用定时器
                 general_max_workers: int = 500,        # 普通队列最大线程数
                 market_max_workers: int = 1000,        # 行情高频队列最大线程数
                 register_signals: bool = True,         # 是否注册信号处理
                 auto_start: bool = True                # 是否自动启动
                 ) -> None:

        self.logger = get_logger(context=context)
        self._context: str = context        # 上下文(可传入服务名/模块名作为上下文)
        self._interval = interval
        # 存储事件类型与订阅者的映射：{event_type: [(subscriber, async_mode), ...]}
        self._subscribers: dict[str, list] = defaultdict(list)

        # 使用标准ThreadPoolExecutor避免自定义线程池的竞态问题
        self._executors: dict[str, ThreadPoolExecutor] = {
            "general": ThreadPoolExecutor(
                max_workers=general_max_workers,
                thread_name_prefix="general"
            ),
            "market": ThreadPoolExecutor(
                max_workers=market_max_workers,
                thread_name_prefix="market"
            ),
        }

        # 用于同步事件的队列，不同类别事件对应不同队列
        self._queues: dict[str, queue.Queue] = {
            "general": queue.Queue(),  # 普通队列
            "market": queue.Queue(),  # 行情队列，高频，默认不限制大小，防止撑满队列
        }

        # 异步事件的队列
        self._async_queue: asyncio.Queue[Event] | None = None  # 用于异步事件的队列

        # 消费线程（同步）
        self._threads: dict[str, threading.Thread] = {}
        # 消费任务（异步）
        self._async_task: asyncio.Task | None = None

        self._loop: asyncio.AbstractEventLoop | None = None  # 异步任务的事件循环
        self._lock = threading.RLock()

        self._queue_timeout: float = 3.0                # 从队列中放入或获取事件超时时间(秒)
        self._sync_thread_quit_timeout: float = 3.0     # 同步任务退出超时时间(秒)

        self._active = False                # 事件总线是否激活
        self._stopped = threading.Event()   # 用于停止事件总线的事件

        self._signal_registered = False     # 是否注册过信号处理
        self._register_signals = register_signals
        self._old_sigint = None
        self._old_sigterm = None

        # 定时器相关
        self._timer_enabled: bool = timer_enabled          # 是否启用定时器
        self._timer_thread: threading.Thread | None = None # 定时器线程

        # 自动启动功能
        if auto_start:
            self.start()

    # ===================== 启动 / 停止 =====================
    def start(self):
        """启动事件总线"""
        if self._active:
            self.logger.info(f"{self._context} 已经启动")
            return

        if self._loop is None:
            self._loop = self._get_or_create_event_loop()

        # 在事件循环中创建异步队列
        if self._async_queue is None:
            self._async_queue = asyncio.Queue()

        self._active = True
        self._stopped.clear()

        # 启动同步消费线程
        for qname in self._queues:
            # 🔒 检查是否已有该线程在运行（防止线程泄漏）
            if qname in self._threads and self._threads[qname].is_alive():
                self.logger.warning(f"{qname} 同步消费线程已在运行，跳过创建")
                continue
            
            thread = threading.Thread(
                target=self._sync_loop, args=(qname,), daemon=True, name=f"SyncLoop-{qname}"
            )
            self._threads[qname] = thread
            thread.start()
            self.logger.info(f"已启动 {qname} 同步消费线程")

        # 启动异步消费任务
        if self._async_task is None and self._loop:
            self._async_task = self._loop.create_task(self._async_loop(), name="AsyncLoop")

        # 启动定时器线程
        if self._timer_enabled and self._timer_thread is None:
            self._timer_thread = threading.Thread(
                target=self._timer_loop, daemon=True, name="TimerLoop"
            )
            self._timer_thread.start()
            self.logger.info(f"定时器线程已启动，间隔: {self._interval}秒")

        # 注册信号
        if (self._register_signals and not self._signal_registered
                and threading.current_thread() is threading.main_thread()):
            try:
                self._old_sigint = signal.getsignal(signal.SIGINT)
                self._old_sigterm = signal.getsignal(signal.SIGTERM)
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
                self._signal_registered = True
            except ValueError:
                # 非主线程调用时 signal.signal 会报错，忽略即可
                pass

        self.logger.info(f"{self._context} 已启动")

    def stop(self):
        """停止事件总线，优雅关闭所有任务和线程"""
        with self._lock:
            if not self._active or self._stopped.is_set():
                return

            self.logger.info("正在停止 EventBus ...")
            self._active = False
            self._stopped.set()  # 标记停止

            try:
                # 发送退出信号
                for sync_queue in self._queues.values():
                    try:
                        sync_queue.put(Event(EventType.EVENT_BUS_SHUTDOWN), timeout=self._queue_timeout)
                    except (queue.Full, queue.Empty, RuntimeError, TimeoutError):
                        pass  # 忽略其他错误

                # 发停止信号给异步队列
                if self._loop and not self._loop.is_closed() and self._async_queue:
                    try:
                        self._loop.call_soon_threadsafe(self._async_queue.put_nowait,
                                                        Event(EventType.EVENT_BUS_SHUTDOWN))
                    except (RuntimeError, asyncio.QueueFull, asyncio.QueueEmpty):
                        # 如果事件循环已经关闭或队列已满，忽略错误
                        pass

                # 等待同步线程退出
                for thread in self._threads.values():
                    if thread and thread.is_alive():
                        try:
                            thread.join(timeout=self._sync_thread_quit_timeout)
                        except Exception as e:
                            self.logger.warning(f"等待线程退出失败: {e}")

                # 等待定时器线程退出
                if self._timer_thread and self._timer_thread.is_alive():
                    try:
                        self._timer_thread.join(timeout=self._sync_thread_quit_timeout)
                        self.logger.info("定时器线程已停止")
                    except Exception as e:
                        self.logger.warning(f"等待定时器线程退出失败: {e}")

                # 关闭线程池
                for name, executor in self._executors.items():
                    try:
                        executor.shutdown(wait=True)
                        self.logger.info(f"线程池 {name} 已关闭")
                    except Exception as e:
                        self.logger.warning(f"关闭线程池 {name} 失败: {e}")

                # 取消异步任务（等待完成，避免 pending 警告）
                if self._async_task and not self._async_task.done():
                    self._async_task.cancel()

            finally:
                # 恢复信号
                if self._signal_registered and threading.current_thread() is threading.main_thread():
                    try:
                        if self._old_sigint:
                            signal.signal(signal.SIGINT, self._old_sigint)
                        if self._old_sigterm:
                            signal.signal(signal.SIGTERM, self._old_sigterm)
                    except ValueError:
                        pass
                    finally:
                        self._signal_registered = False

                # 清理资源（无论前面是否异常，都会执行）
                self._loop = None
                self._threads = {}
                self._async_task = None
                self._timer_thread = None
                self.logger.info("EventBus 已优雅停止")

    def _signal_handler(self, signum, _frame):
        """接收到 SIGINT/SIGTERM 时调用 stop"""
        self.logger.info(f"收到信号 {signum}，准备停止...")
        threading.Thread(target=self.stop, daemon=True).start()

    # ===================== 状态查询 =====================
    def is_active(self) -> bool:
        """检查事件总线是否激活"""
        return self._active

    def timer_enabled(self) -> bool:
        """检查定时器是否启用"""
        return self._timer_enabled

    def get_subscriber_count(self, event_type: str | None = None) -> int:
        """获取订阅者数量"""
        with self._lock:
            if event_type:
                return len(self._subscribers.get(event_type, []))
            else:
                return sum(len(subscribers) for subscribers in self._subscribers.values())

    def get_registered_event_types(self) -> list[str]:
        """获取已注册的事件类型"""
        with self._lock:
            return list(self._subscribers.keys())

    def get_thread_pool_stats(self) -> dict:
        """获取线程池统计信息"""
        stats = {}
        for name, executor in self._executors.items():
            queue_size = self._queues[name].qsize() if name in self._queues else 0
            stats[name] = {
                'max_workers': executor._max_workers,
                'queue_size': queue_size,  # 添加队列积压监控
                'queue_health': 'healthy' if queue_size < 1000 else 'warning' if queue_size < 5000 else 'critical',
                'executor_type': 'ThreadPoolExecutor'
            }
        return stats
    
    def get_tick_queue_status(self) -> dict:
        """获取tick队列专用状态信息"""
        market_queue = self._queues["market"]
        market_executor = self._executors["market"]
        
        queue_size = market_queue.qsize()
        
        return {
            'queue_size': queue_size,
            'queue_health': 'healthy' if queue_size < 1000 else 'warning' if queue_size < 5000 else 'critical',
            'thread_pools_active': 1,
            'current_pool': 1,
            'max_pools': 1,
            'expansion_available': False,
            'total_capacity': market_executor._max_workers,
            'processing_mode': 'standard_executor'
        }

    # ===================== 订阅 / 发布 =====================
    def subscribe(self, event_type: str, subscriber, async_mode=False) -> None:
        """
        订阅事件
        :param event_type: 事件类型
        :param subscriber: 事件的订阅者
        :param async_mode: 是否异步模式(默认同步)
        :return:
        """
        with self._lock:
            self._subscribers[event_type].append((subscriber, async_mode))

    def unsubscribe(self, event_type: str, subscriber) -> None:
        """
        取消订阅事件
        :param event_type: 事件类型
        :param subscriber: 事件的订阅者
        :return:
        """
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type] = [
                    (s, async_mode) for s, async_mode in self._subscribers[event_type] if s != subscriber
                ]
                # 如果列表为空，删除该事件类型
                if not self._subscribers[event_type]:
                    del self._subscribers[event_type]

    def publish(self, event: Event, async_mode=False) -> None:
        """
        发布事件（放入队列）- 带背压机制，确保零丢失
        :param event: 事件
        :param async_mode: 是否异步模式
        :return:
        """
        if not self._active or self._stopped.is_set():
            self.logger.warning("已停止，忽略事件发布")
            return

        # 发布事件时自动继承 trace_id
        if not event.trace_id:
            event.trace_id = trace_context.get_trace_id() or trace_context.set_trace_id()

        if async_mode:
            if self._loop and self._async_queue:
                try:
                    # 使用 put/put_nowait
                    self._loop.call_soon_threadsafe(self._async_queue.put, event)
                except asyncio.QueueFull:
                    self.logger.warning(f"异步队列已满，丢弃事件 {event.event_type}")
                except RuntimeError as e:
                    self.logger.error(f"异步事件发布失败: {e}")
        else:
            qname = "market" if event.event_type.startswith("market") else "general"
            
            # 背压机制：重试和动态调整
            max_retries = 3
            retry_count = 0
            base_timeout = self._queue_timeout
            
            while retry_count < max_retries:
                try:
                    # 动态调整超时时间
                    timeout = base_timeout * (2 ** retry_count)  # 指数退避
                    self._queues[qname].put(event, block=True, timeout=timeout)
                    return  # 成功，退出
                except Full:
                    retry_count += 1
                    if retry_count < max_retries:
                        # 记录警告但不丢弃，继续重试
                        self.logger.warning(f"同步队列 {qname} 已满，重试 {retry_count}/{max_retries}")
                        # 触发队列清理或扩容机制
                        self._try_expand_processing_capacity(qname)
                    else:
                        # 最后一次尝试：使用更长的阻塞时间
                        try:
                            self._queues[qname].put(event, block=True, timeout=10.0)  # 最多等待10秒
                            self.logger.info(f"队列 {qname} 在最后重试中成功入队")
                            return
                        except Full:
                            # 对于tick事件，使用阻塞模式确保不丢失
                            if event.event_type == EventType.TICK:
                                self.logger.critical("tick队列满载，启用阻塞模式确保不丢失")
                                self._queues[qname].put(event, block=True)  # 无超时，确保入队
                            else:
                                # 非tick事件可以丢弃
                                self.logger.error(f"严重：队列 {qname} 持续满载，事件 {event.event_type} 被迫丢弃")

    def _try_expand_processing_capacity(self, qname: str) -> None:
        """
        尝试扩展处理能力，当队列满时调用（标准ThreadPoolExecutor版本）
        :param qname: 队列名称
        """
        try:
            executor = self._executors.get(qname)
            if executor:
                # 标准ThreadPoolExecutor无法动态扩展，只记录警告
                self.logger.warning(f"队列 {qname} 满载，已达到最大处理能力：{executor._max_workers}")
        except Exception as e:
            self.logger.error(f"扩展处理能力失败: {e}")

    # ===================== 内部消费 =====================
    def _sync_loop(self, qname: str) -> None:
        """消费同步事件"""
        self.logger.info(f"开始 {qname} 同步事件循环")
        queue_obj = self._queues[qname]
        try:
            while self._active and not self._stopped.is_set():
                try:
                    event = queue_obj.get(block=True, timeout=self._queue_timeout)
                except Empty:
                    continue
                if event.event_type == EventType.EVENT_BUS_SHUTDOWN:  # 停止事件
                    break
                self._dispatch(event)
        except Exception as e:
            self.logger.exception(f"同步事件循环异常({qname}): {e}")
        finally:
            self.logger.info(f"同步事件循环已退出 ({qname})")

    async def _async_loop(self) -> None:
        """消费异步事件"""
        if not self._async_queue:
            self.logger.warning("异步队列未初始化")
            return
        try:
            while self._active and not self._stopped.is_set():
                try:
                    event = await asyncio.wait_for(self._async_queue.get(), timeout=self._queue_timeout)
                except asyncio.TimeoutError:
                    continue  # 超时继续循环，检查停止条件
                if event.event_type == EventType.EVENT_BUS_SHUTDOWN:  # 停止事件
                    self._dispatch(event)  # 先分发停止事件给订阅者
                    break
                self._dispatch(event)
        except asyncio.CancelledError:
            # 任务被取消时优雅退出
            self.logger.info("异步事件循环被取消")
        except Exception as e:
            self.logger.exception(f"异步事件循环异常: {e}")
        finally:
            self.logger.info("异步事件循环已退出")

    def _timer_loop(self) -> None:
        """
        定时器循环，定期发布TIMER事件
        
        每隔 self._interval 秒发布一次 EventType.TIMER 事件。
        订阅者可以监听该事件执行定时任务（如查询账户、持仓等）。
        """
        self.logger.info(f"定时器线程已启动，间隔: {self._interval}秒")
        try:
            while self._active and not self._stopped.is_set():
                time.sleep(self._interval)
                
                # 检查是否仍在运行
                if not self._active or self._stopped.is_set():
                    break
                
                # 发布定时器事件（走general队列，优先级不高）
                try:
                    timer_event = Event.timer(source="EventBus")
                    self.publish(timer_event, async_mode=False)
                except Exception as e:
                    self.logger.error(f"发布定时器事件失败: {e}")
        except Exception as e:
            self.logger.exception(f"定时器循环异常: {e}")
        finally:
            self.logger.info("定时器线程已退出")

    def _dispatch(self, event: Event) -> None:
        """
        分发事件到订阅者
        :param event: 事件
        :return:
        """
        with self._lock:
            subscribers = list(self._subscribers.get(event.event_type, []))  # 拷贝快照，避免迭代时修改

        for i, (subscriber, async_mode) in enumerate(subscribers):
            try:
                # 自动设置 trace_id 到上下文
                trace_context.set_trace_id(event.trace_id)
                if async_mode:
                    if not inspect.iscoroutinefunction(subscriber):
                        raise ValueError(f"异步订阅者必须是 async 函数: {subscriber}")
                    if self._loop:
                        self._loop.create_task(self._safe_async(subscriber, event))
                else:
                    qname = "market" if event.event_type.startswith("market") else "general"
                    executor = self._executors[qname]
                    
                    # 检查线程池是否还可用
                    try:
                        future = executor.submit(self._safe_sync, subscriber, event)
                        
                        # 对于tick事件，如果提交失败立即在当前线程执行，确保不丢失
                        if event.event_type == EventType.TICK and future is None:
                            self.logger.warning("tick事件线程池提交失败，直接执行")
                            self._safe_sync(subscriber, event)
                    except RuntimeError as e:
                        # 线程池已关闭，直接在当前线程执行
                        if "cannot schedule new futures after shutdown" in str(e):
                            self._safe_sync(subscriber, event)
                        else:
                            raise
                    except Exception as e:
                        # 其他异常，对于tick事件确保不丢失
                        if event.event_type == EventType.TICK:
                            self.logger.error(f"tick事件提交异常，直接执行: {e}")
                            self._safe_sync(subscriber, event)
                        else:
                            raise
            except Exception as e:
                self.logger.exception(f"事件 {event.event_type} 分发失败: {e}")

    # ===================== 安全执行封装 =====================
    def _safe_sync(self, subscriber, event):
        """同步订阅者安全执行"""
        try:
            # 在新线程中自动设置上下文 trace_id
            trace_context.set_trace_id(event.trace_id)
            subscriber(event)
        except Exception as e:
            self.logger.exception(f"同步订阅者 {subscriber} 执行失败: {e}")

    async def _safe_async(self, subscriber, event):
        """异步订阅者安全执行"""
        try:
            # 在异步任务中自动设置上下文 trace_id
            trace_context.set_trace_id(event.trace_id)
            await subscriber(event)
        except Exception as e:
            self.logger.exception(f"异步订阅者 {subscriber} 执行失败: {e}")

    # ===================== 事件循环管理 =====================
    @staticmethod
    def _get_or_create_event_loop():
        """获取或创建事件循环，如果没有运行的事件循环，创建一个新的"""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            threading.Thread(target=loop.run_forever, daemon=True, name="AsyncEventLoopThread").start()
            return loop
