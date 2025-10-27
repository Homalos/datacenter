#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : server.py
@Date       : 2025/10/27
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 数据中心API服务 (FastAPI) - 提供数据查询和系统管理接口
"""
import traceback
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# 这些模块会在运行时注入
storage = None
contract_manager = None
metrics_collector = None
datacenter_starter = None
bar_manager = None
data_archiver = None


app = FastAPI(
    title="Homalos Data Center API",
    description="期货数据中心 - Tick/K线数据查询 + 系统管理接口",
    version="0.2.0"
)

# 挂载静态文件目录
static_path = Path(__file__).parent.parent.parent / "static"
static_path.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


def init_dependencies(**deps):
    """
    初始化依赖（在启动脚本中调用）
    
    Args:
        **deps: 依赖字典，包含storage, contract_manager, metrics_collector等
    """
    global storage, contract_manager, metrics_collector, datacenter_starter, bar_manager, data_archiver
    
    storage = deps.get("storage")
    contract_manager = deps.get("contract_manager")
    metrics_collector = deps.get("metrics_collector")
    datacenter_starter = deps.get("datacenter_starter")
    bar_manager = deps.get("bar_manager")
    data_archiver = deps.get("data_archiver")


# ============================================================
#  基础信息接口
# ============================================================

@app.get("/")
def root():
    """API根路径 - 返回所有可用接口"""
    return {
        "name": "Homalos Data Center API",
        "version": "0.2.0",
        "endpoints": {
            "数据查询": {
                "kline": "GET /kline/{symbol}?start=YYYY-MM-DD&end=YYYY-MM-DD&interval=1m",
                "tick": "GET /tick/{symbol}?start=YYYY-MM-DD&end=YYYY-MM-DD"
            },
            "系统管理": {
                "health": "GET /health",
                "status": "GET /status",
                "metrics": "GET /metrics",
                "contracts": "GET /contracts?exchange=SHFE"
            },
            "可视化": {
                "dashboard": "GET /dashboard"
            }
        }
    }


@app.get("/health")
def health_check():
    """健康检查"""
    health_status = {
        "status": "ok",
        "message": "Service is running"
    }
    
    # 如果有metrics_collector，添加健康检查详情
    if metrics_collector:
        try:
            health_details = metrics_collector.check_health()
            health_status["healthy"] = health_details.get("overall_healthy", True)
            health_status["details"] = health_details
        except Exception:
            pass
    
    return health_status


# ============================================================
#  数据查询接口
# ============================================================

@app.get("/kline/{symbol}")
def get_kline(
    symbol: str,
    start: str = Query(..., description="开始时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS"),
    end: str = Query(..., description="结束时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS"),
    interval: str = Query("1m", description="K线周期，如 1m, 5m, 15m, 30m, 1h, 1d")
):
    """
    查询K线数据
    
    Args:
        symbol: 合约代码，如 rb2505
        start: 开始时间
        end: 结束时间
        interval: K线周期
    
    Returns:
        K线数据列表
    """
    if not storage:
        raise HTTPException(status_code=503, detail="存储服务未初始化")
    
    try:
        # 记录API请求（用于监控）
        if metrics_collector:
            metrics_collector.record_api_request()
        
        # 查询K线数据
        symbol_with_interval = f"{symbol}_{interval}"
        
        # 判断storage类型，使用相应的查询方法
        if hasattr(storage, 'query_klines'):
            # HybridStorage接口
            df = storage.query_klines(symbol, interval, start, end)
        else:
            # 旧的DataStorage接口
            df = storage.query_kline(symbol_with_interval, start, end)
        
        if df.empty:
            return {
                "symbol": symbol,
                "interval": interval,
                "start": start,
                "end": end,
                "count": 0,
                "data": []
            }
        
        # 转换datetime为字符串
        if 'datetime' in df.columns:
            df['datetime'] = df['datetime'].astype(str)
        
        return {
            "symbol": symbol,
            "interval": interval,
            "start": start,
            "end": end,
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
        
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"未找到合约 {symbol} 的K线数据"
        )
    except Exception as e:
        print(f"查询K线出错: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"查询失败: {str(e)}"
        )


@app.get("/tick/{symbol}")
def get_tick(
    symbol: str,
    start: str = Query(..., description="开始时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS"),
    end: str = Query(..., description="结束时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
):
    """
    查询Tick数据
    
    Args:
        symbol: 合约代码，如 rb2505
        start: 开始时间
        end: 结束时间
    
    Returns:
        Tick数据列表
    """
    if not storage:
        raise HTTPException(status_code=503, detail="存储服务未初始化")
    
    try:
        # 记录API请求
        if metrics_collector:
            metrics_collector.record_api_request()
        
        # 查询Tick数据
        if hasattr(storage, 'query_ticks'):
            # HybridStorage接口
            df = storage.query_ticks(symbol, start, end)
        else:
            # 旧的DataStorage接口
            df = storage.query_tick(symbol, start, end)
        
        if df.empty:
            return {
                "symbol": symbol,
                "start": start,
                "end": end,
                "count": 0,
                "data": []
            }
        
        # 转换datetime为字符串
        if 'datetime' in df.columns:
            df['datetime'] = df['datetime'].astype(str)
        
        return {
            "symbol": symbol,
            "start": start,
            "end": end,
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
        
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"未找到合约 {symbol} 的Tick数据"
        )
    except Exception as e:
        print(f"查询Tick出错: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"查询失败: {str(e)}"
        )


# ============================================================
#  系统管理接口
# ============================================================

@app.get("/contracts")
def get_contracts(
    exchange: Optional[str] = Query(None, description="交易所代码，如 SHFE, DCE, CZCE, CFFEX")
):
    """
    获取合约列表
    
    Args:
        exchange: 可选，按交易所筛选
    
    Returns:
        合约列表
    """
    if not contract_manager:
        raise HTTPException(status_code=503, detail="合约管理服务未初始化")
    
    try:
        if exchange:
            # 按交易所筛选
            contracts = contract_manager.get_contracts_by_exchange(exchange)
        else:
            # 返回全部合约
            contracts = contract_manager.get_all_contracts()
        
        return {
            "total": len(contracts),
            "exchange": exchange,
            "contracts": [c.to_dict() for c in contracts]
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"查询合约列表失败: {str(e)}"
        )


@app.get("/status")
def get_system_status():
    """获取系统运行状态"""
    status = {
        "timestamp": None,
        "modules": {},
        "contracts": {},
        "bars": {},
        "storage": {}
    }
    
    try:
        # 数据中心启动器状态
        if datacenter_starter:
            status["modules"] = datacenter_starter.get_statistics()
        
        # 合约管理器状态
        if contract_manager:
            status["contracts"] = contract_manager.get_statistics()
        
        # K线管理器状态
        if bar_manager:
            status["bars"] = bar_manager.get_statistics()
        
        # 存储层状态
        if storage and hasattr(storage, 'get_statistics'):
            status["storage"] = storage.get_statistics()
        
        # 归档器状态
        if data_archiver:
            status["archiver"] = data_archiver.get_statistics()
        
        return status
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"获取系统状态失败: {str(e)}"
        )


@app.get("/metrics")
def get_metrics():
    """获取系统监控指标"""
    if not metrics_collector:
        raise HTTPException(status_code=503, detail="监控服务未初始化")
    
    try:
        # 收集所有指标
        metrics = metrics_collector.collect_all_metrics()
        
        return metrics
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"获取监控指标失败: {str(e)}"
        )


@app.get("/metrics/summary")
def get_metrics_summary():
    """获取监控指标摘要（简化版）"""
    if not metrics_collector:
        raise HTTPException(status_code=503, detail="监控服务未初始化")
    
    try:
        summary = metrics_collector.get_summary()
        return summary
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"获取监控摘要失败: {str(e)}"
        )


@app.post("/archive")
def trigger_archive():
    """手动触发数据归档（管理员操作）"""
    if not data_archiver:
        raise HTTPException(status_code=503, detail="归档服务未初始化")
    
    try:
        result = data_archiver.archive_old_data()
        return result
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"归档失败: {str(e)}"
        )


# ============================================================
#  可视化接口
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """返回可视化仪表板页面"""
    dashboard_file = Path(__file__).parent.parent.parent / "static" / "dashboard.html"
    
    if not dashboard_file.exists():
        raise HTTPException(status_code=404, detail="Dashboard页面不存在")
    
    with open(dashboard_file, 'r', encoding='utf-8') as f:
        return f.read()


# ============================================================
#  异常处理
# ============================================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """全局异常处理"""
    print(f"全局异常: {traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"服务器内部错误: {str(exc)}"}
    )
