@echo off
chcp 65001 > nul
echo ========================================
echo   Homalos 数据中心 - 开发模式启动
echo ========================================
echo.

REM 检查 Node.js
echo [1/5] 检查 Node.js...
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ❌ 错误: 未找到 Node.js，请先安装 Node.js 18.0+
    echo    下载地址: https://nodejs.org/
    pause
    exit /b 1
)

node --version
echo ✅ Node.js 已安装
echo.

REM 检查前端依赖
echo [2/5] 检查前端依赖...
if not exist "frontend\node_modules" (
    echo ⚠️  未找到 node_modules，开始安装依赖...
    cd frontend
    call npm install
    if %ERRORLEVEL% NEQ 0 (
        echo ❌ 依赖安装失败
        cd ..
        pause
        exit /b 1
    )
    cd ..
    echo ✅ 依赖安装成功
) else (
    echo ✅ 依赖已安装
)
echo.

REM 启动后端
echo [3/5] 启动后端服务...
start "Homalos DataCenter Backend" cmd /k "call .venv\Scripts\activate && python start_web.py"
timeout /t 3 /nobreak > nul
echo ✅ 后端服务已启动（端口 8001）
echo.

REM 启动前端
echo [4/5] 启动前端开发服务器...
cd frontend
start "Homalos 前端" cmd /k "npm run dev"
cd ..
timeout /t 3 /nobreak > nul
echo ✅ 前端服务已启动（端口 5173）
echo.

REM 完成
echo [5/5] 启动完成！
echo.
echo ========================================
echo   🎉 开发环境已启动
echo ========================================
echo.
echo 📊 后端地址:  http://127.0.0.1:8001
echo    - API 文档:    http://127.0.0.1:8001/docs
echo    - 健康检查:    http://127.0.0.1:8001/health
echo    - 控制面板:    http://127.0.0.1:8001/dashboard
echo.
echo 🖥️  前端地址:  http://localhost:5173
echo    - 开发模式（热重载）
echo.
echo 💡 提示:
echo    - 前端开发: 访问 http://localhost:5173（实时热重载）
echo    - 后端测试: 访问 http://127.0.0.1:8001/dashboard（生产模式）
echo    - 关闭服务: 在各自窗口按 Ctrl+C
echo.
echo ⏳ 等待 5 秒后自动打开浏览器...
timeout /t 5 /nobreak > nul

REM 打开浏览器
start http://localhost:5173

echo.
echo ✅ 浏览器已打开
echo.
echo 按任意键退出此窗口（不会关闭服务）...
pause > nul

