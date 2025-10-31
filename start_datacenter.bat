@echo off
setlocal enabledelayedexpansion
REM ===================================================================
REM Homalos �������������ű� (Windows)
REM ===================================================================

echo.
echo ========================================================================
echo   Homalos �������������ű�
echo ========================================================================
echo.

REM ������⻷��
if not exist ".venv\Scripts\activate.bat" (
    echo [����] ���⻷�������ڣ����ȴ������⻷��
    echo ����: python -m venv .venv
    pause
    exit /b 1
)

REM �������⻷��
echo [1/3] �������⻷��...
call .venv\Scripts\activate.bat

REM ���˿�ռ�ã���ѡ��
echo.
echo [2/3] ���˿�ռ��...
set DEFAULT_PORT=8001
set API_PORT=%DEFAULT_PORT%
netstat -ano | findstr :%DEFAULT_PORT% > nul
if %errorlevel% == 0 (
    echo [����] �˿� %DEFAULT_PORT% �ѱ�ռ��
    echo.
    echo ��ѡ������
    echo   1. ʹ�������˿����� ���Ƽ���
    echo   2. �ر�ռ�ö˿ڵĽ���
    echo   3. ȡ������
    echo.
    set /p choice="��ѡ�� (1/2/3): "
    
    if "!choice!"=="1" (
        set /p custom_port="�������¶˿ں� (���� 8002): "
        set API_PORT=!custom_port!
        echo [��Ϣ] ��ʹ�ö˿� !custom_port! ����
    ) else if "!choice!"=="2" (
        echo.
        echo ��ǰռ�ö˿� %DEFAULT_PORT% �Ľ��̣�
        netstat -ano | findstr :%DEFAULT_PORT%
        echo.
        set /p pid="������Ҫ�رյĽ���ID (PID): "
        taskkill /F /PID !pid!
        echo [��Ϣ] �����ѹرգ���ʹ��Ĭ�϶˿� %DEFAULT_PORT% ����
        set API_PORT=%DEFAULT_PORT%
    ) else (
        echo [��Ϣ] ��ȡ������
        pause
        exit /b 0
    )
) else (
    echo [��Ϣ] �˿� %DEFAULT_PORT% ����
)

REM ���� Web �������
echo.
echo [3/3] ���� Web �������...
echo ========================================================================
echo.
echo ��ʾ��
echo   - Web ������彫�� http://localhost:!API_PORT!/dashboard ����
echo   - �� Web �����е��"������������"��ť
echo.
echo ========================================================================
echo.
python start_web.py

pause
