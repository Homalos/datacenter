@echo off
setlocal enabledelayedexpansion
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
    echo 命令: python -m venv .venv
    pause
    exit /b 1
)

REM 激活虚拟环境
echo [1/3] 激活虚拟环境...
call .venv\Scripts\activate.bat

REM 检查端口占用（可选）
echo.
echo [2/3] 检查端口占用...
set DEFAULT_PORT=8001
set API_PORT=%DEFAULT_PORT%
netstat -ano | findstr :%DEFAULT_PORT% > nul
if %errorlevel% == 0 (
    echo [警告] 端口 %DEFAULT_PORT% 已被占用
    echo.
    echo 可选操作：
    echo   1. 使用其他端口启动 （推荐）
    echo   2. 关闭占用端口的进程
    echo   3. 取消启动
    echo.
    set /p choice="请选择 (1/2/3): "
    
    if "!choice!"=="1" (
        set /p custom_port="请输入新端口号 (例如 8002): "
        set API_PORT=!custom_port!
        echo [信息] 将使用端口 !custom_port! 启动
    ) else if "!choice!"=="2" (
        echo.
        echo 当前占用端口 %DEFAULT_PORT% 的进程：
        netstat -ano | findstr :%DEFAULT_PORT%
        echo.
        set /p pid="请输入要关闭的进程ID (PID): "
        taskkill /F /PID !pid!
        echo [信息] 进程已关闭，将使用默认端口 %DEFAULT_PORT% 启动
        set API_PORT=%DEFAULT_PORT%
    ) else (
        echo [信息] 已取消启动
        pause
        exit /b 0
    )
) else (
    echo [信息] 端口 %DEFAULT_PORT% 可用
)

REM 启动 Web 控制面板
echo.
echo [3/3] 启动 Web 控制面板...
echo ========================================================================
echo.
echo 提示：
echo   - Web 控制面板将在 http://localhost:!API_PORT!/dashboard 启动
echo   - 在 Web 界面中点击"启动数据中心"按钮
echo.
echo ========================================================================
echo.
python start_web.py

pause
