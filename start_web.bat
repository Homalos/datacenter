@echo off
title Homalos 数据中心
REM ===================================================================
REM Homalos 数据中心启动脚本 (Windows)
REM ===================================================================

echo.
echo ========================================================================
echo   Homalos 数据中心启动脚本
echo ========================================================================
echo.

REM 检查虚拟环境
if not exist ".venv\Scripts\activate.bat" (
    echo [错误] 虚拟环境不存在，请先创建虚拟环境
    echo 运行: python -m venv .venv
    pause
    exit /b 1
)

REM 激活虚拟环境
echo 激活虚拟环境...
call .venv\Scripts\activate.bat


REM 启动数据中心
echo.
echo 启动数据中心...
echo ========================================================================
echo.
python start_web.py

pause

