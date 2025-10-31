@echo off
REM =========================================
REM  Homalos �������� - ǰ�˹����ű�
REM =========================================

echo ========================================
echo  Homalos �������� - ǰ�˹���
echo ========================================
echo.

REM ��� Node.js �Ƿ�װ
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [����] δ�ҵ� Node.js�����Ȱ�װ Node.js 18+ �� 20+
    echo ���ص�ַ: https://nodejs.org/
    pause
    exit /b 1
)

echo [1/3] ��� Node.js �汾...
node --version
npm --version
echo.

REM ����ǰ��Ŀ¼
cd frontend

REM ����Ƿ��Ѱ�װ����
if not exist "node_modules\" (
    echo [2/3] �״ι��������ڰ�װ����...
    echo �������Ҫ�����ӣ������ĵȴ�...
    npm install
    if %errorlevel% neq 0 (
        echo [����] ������װʧ��
        cd ..
        pause
        exit /b 1
    )
) else (
    echo [2/3] �����Ѱ�װ��������װ����
)
echo.

REM ִ�й���
echo [3/3] ���ڹ���ǰ����Դ...
npm run build
if %errorlevel% neq 0 (
    echo [����] ����ʧ��
    cd ..
    pause
    exit /b 1
)

cd ..

echo.
echo ========================================
echo  ������ɣ�
echo ========================================
echo.
echo  ���������������: static/
echo  ��������: python start_web.py
echo  ���ʵ�ַ: http://127.0.0.1:8000/dashboard
echo.
pause

