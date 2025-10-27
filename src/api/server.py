#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: homalos-datacenter
@FileName   : server.py
@Date       : 2025/10/14 15:40
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 查询服务 (FastAPI)
"""
import json
import traceback
from pathlib import Path
from typing import Dict

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.core.storage import DataStorage

app = FastAPI(
    title="Homalos Data Center API",
    description="期货数据中心 - Tick和K线数据查询 + 可视化接口",
    version="0.0.1"
)

storage = DataStorage()

# 挂载静态文件目录
static_path = Path(__file__).parent.parent / "static"
static_path.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


@app.get("/")
def root():
    """API根路径"""
    return {
        "name": "Homalos Data Center API",
        "version": "0.1.0",
        "endpoints": {
            "kline": "/kline/{symbol}?start=YYYY-MM-DD&end=YYYY-MM-DD",
            "tick": "/tick/{symbol}?start=YYYY-MM-DD&end=YYYY-MM-DD",
            "health": "/health"
        }
    }


@app.get("/health")
def health_check():
    """健康检查"""
    return {"status": "ok", "message": "Service is running"}


@app.get("/kline/{symbol}")
def get_kline(
    symbol: str,
    start: str = Query(..., description="开始时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS"),
    end: str = Query(..., description="结束时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
):
    """
    查询K线数据
    
    Args:
        symbol: 品种代码，如 RB2601
        start: 开始时间
        end: 结束时间
    
    Returns:
        K线数据列表
    """
    try:
        df = storage.query_kline(symbol, start, end)
        
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
            detail=f"未找到品种 {symbol} 的K线数据"
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
        symbol: 品种代码，如 RB2601
        start: 开始时间
        end: 结束时间
    
    Returns:
        Tick数据列表
    """
    try:
        df = storage.query_ticks(symbol, start, end)
        
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
            detail=f"未找到品种 {symbol} 的Tick数据"
        )
    except Exception as e:
        print(f"查询Tick出错: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"查询失败: {str(e)}"
        )


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """全局异常处理"""
    print(f"全局异常: {traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"服务器内部错误: {str(exc)}"}
    )


# ============================================================
#  可视化相关API
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """返回可视化仪表板页面"""
    dashboard_file = Path(__file__).parent.parent / "static" / "dashboard.html"
    
    if not dashboard_file.exists():
        raise HTTPException(status_code=404, detail="Dashboard页面不存在")
    
    with open(dashboard_file, 'r', encoding='utf-8') as f:
        return f.read()


@app.get("/backtest/export/{filename}")
async def export_backtest_data(filename: str):
    """导出回测数据为JSON（供前端ECharts使用）"""
    backtest_data_dir = Path("backtest_data")
    file_path = backtest_data_dir / f"{filename}.json"
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"回测数据文件不存在: {filename}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


@app.post("/backtest/save")
async def save_backtest_result(data: Dict):
    """
    保存回测结果数据（供前端可视化使用）
    
    Args:
        data: 回测结果数据
            - account_history: 交易历史
            - metrics: 绩效指标
            - config: 回测配置
    """
    backtest_data_dir = Path("backtest_data")
    backtest_data_dir.mkdir(exist_ok=True)
    
    from datetime import datetime
    filename = f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    file_path = backtest_data_dir / filename
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    return {
        "success": True,
        "filename": filename,
        "message": "回测结果已保存",
        "url": f"/backtest/export/{filename.replace('.json', '')}"
    }


@app.get("/backtest/list")
async def list_backtest_results():
    """列出所有保存的回测结果"""
    backtest_data_dir = Path("backtest_data")
    
    if not backtest_data_dir.exists():
        return {"results": []}
    
    results = []
    for file in sorted(backtest_data_dir.glob("backtest_*.json"), reverse=True):
        results.append({
            "filename": file.stem,
            "created": file.stat().st_mtime,
            "size": file.stat().st_size
        })
    
    return {"results": results}
