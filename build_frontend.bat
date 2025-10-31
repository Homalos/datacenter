@echo off
REM =========================================
REM  Homalos 数据中心 - 前端构建脚本
REM =========================================

echo ========================================
echo  Homalos 数据中心 - 前端构建
echo ========================================
echo.

REM 检查 Node.js 是否安装
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [错误] 未找到 Node.js，请先安装 Node.js 18+ 或 20+
    echo 下载地址: https://nodejs.org/
    pause
    exit /b 1
)

echo [1/3] 检查 Node.js 版本...
node --version
npm --version
echo.

REM 进入前端目录
cd frontend

REM 检查是否已安装依赖
if not exist "node_modules\" (
    echo [2/3] 首次构建，正在安装依赖...
    echo 这可能需要几分钟，请耐心等待...
    npm install
    if %errorlevel% neq 0 (
        echo [错误] 依赖安装失败
        cd ..
        pause
        exit /b 1
    )
) else (
    echo [2/3] 依赖已安装，跳过安装步骤
)
echo.

REM 执行构建
echo [3/3] 正在构建前端资源...
npm run build
if %errorlevel% neq 0 (
    echo [错误] 构建失败
    cd ..
    pause
    exit /b 1
)

cd ..

echo.
echo ========================================
echo  构建完成！
echo ========================================
echo.
echo  构建产物已输出到: static/
echo  启动命令: python start_web.py
echo  访问地址: http://127.0.0.1:8000/dashboard
echo.
pause

