@echo off
setlocal enabledelayedexpansion
title ѹ�����ݵ�ǰĿ¼ָ���ļ��к��ļ�

echo �ļ��б���...
echo.

:: ���ù̶��ļ��б����ʹ�����ʱ���ZIP�ļ���
set "filelist=files.txt"
set "timestamp=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%"
set "timestamp=%timestamp: =0%"
set "zipname=backup_Homalos_%timestamp%.zip"

:: ��� WinRAR �Ƿ����
set "rar_cmd="
where WinRAR >nul 2>&1
if not errorlevel 1 (
    set "rar_cmd=WinRAR"
) else if exist "%ProgramFiles%\WinRAR\WinRAR.exe" (
    set "rar_cmd=%ProgramFiles%\WinRAR\WinRAR.exe"
) else if exist "%ProgramW6432%\WinRAR\WinRAR.exe" (
    set "rar_cmd=%ProgramW6432%\WinRAR\WinRAR.exe"
) else (
    echo ����: δ�ҵ� WinRAR����ȷ���Ѱ�װ WinRAR
    pause
    exit /b 1
)

echo ʹ�� WinRAR ����ѹ��: !rar_cmd!
echo.

:: ����ļ��б����ڣ�������
if not exist "%filelist%" (
    echo ���������ļ��б�...
    
    :: ����ͳһ���ļ��б�
    echo ��ǰĿ¼�µ������ļ��к��ļ��� > "%filelist%"
    echo ======================================== >> "%filelist%"
    echo. >> "%filelist%"
    
    :: �г������ļ���
    for /d %%i in (*) do (
        set "item=%%i"
        call :trim_spaces item
        echo !item! >> "%filelist%"
    )
    
    echo. >> "%filelist%"
    
    :: �г������ļ�
    for %%i in (*) do (
        if not "%%i"=="%~nx0" if not "%%i"=="%filelist%" (
            set "skip=no"
            for %%z in ("!zipname!") do if "%%i"=="%%~nz%%~xz" set "skip=yes"
            if "!skip!"=="no" (
                set "item=%%i"
                call :trim_spaces item
                echo !item! >> "%filelist%"
            )
        )
    )
    
    echo �ļ��б�������: %filelist%
) else (
    echo ʹ�������ļ��б�: %filelist%
)
echo.

:: ѯ���û�ѡ��ѹ����ʽ
echo ��ѡ��ѹ����ʽ��
echo 1. ѹ�������ļ��к��ļ�
echo 2. ѡ���ض��ļ��к��ļ�����ѹ��
echo 3. ��ѹ���ļ���
echo 4. ��ѹ���ļ�
echo 5. �ֶ��༭�ļ��б��ѹ��
echo.
set /p choice="��ѡ�� (1-5): "

:: ����ѡ��ִ�в�ͬ��ѹ������
if "!choice!"=="1" (
    echo ����ѹ�������ļ��к��ļ�...
    "!rar_cmd!" a -afzip -ep1 -r -y "%zipname%" "*.*"
    goto :success
)

if "!choice!"=="2" (
    call :select_files
    goto :compress_selected
)

if "!choice!"=="3" (
    echo ����ѹ�������ļ���...
    set "folders="
    for /d %%i in (*) do (
        set "folders=!folders! "%%i""
    )
    if defined folders (
        "!rar_cmd!" a -afzip -ep1 -r -y "%zipname%" !folders!
    ) else (
        echo δ�ҵ��κ��ļ��У�
    )
    goto :success
)

if "!choice!"=="4" (
    echo ����ѹ�������ļ�...
    set "files="
    for %%i in (*) do (
        if not "%%i"=="%~nx0" if not "%%i"=="%filelist%" (
            set "skip=no"
            for %%z in ("!zipname!") do if "%%i"=="%%~nz%%~xz" set "skip=yes"
            if "!skip!"=="no" (
                set "files=!files! "%%i""
            )
        )
    )
    if defined files (
        "!rar_cmd!" a -afzip -ep1 -y "%zipname%" !files!
    ) else (
        echo δ�ҵ��κ��ļ���
    )
    goto :success
)

if "!choice!"=="5" (
    echo ��༭�ļ� %filelist%��ɾ������Ҫѹ������Ŀ��Ȼ�����������...
    echo ע�⣺��ȷ��ֻ����Ҫѹ�����ļ��к��ļ�����ÿ��һ��
    pause >nul
    call :compress_from_list
    goto :success
)

echo ��Чѡ��
goto :end

:select_files
echo.
echo ��ǰ���õ��ļ��У�
for /d %%i in (*) do echo   %%i
echo.
echo ��ǰ���õ��ļ���
for %%i in (*) do (
    if not "%%i"=="%~nx0" if not "%%i"=="%filelist%" (
        set "skip=no"
        for %%z in ("!zipname!") do if "%%i"=="%%~nz%%~xz" set "skip=yes"
        if "!skip!"=="no" echo   %%i
    )
)
echo.
set /p selected="������Ҫѹ�����ļ��л��ļ���������ÿո�ָ���: "
goto :eof

:compress_selected
echo ����ѹ��ѡ������Ŀ...
set "items="
for %%i in (%selected%) do (
    if exist "%%i" (
        set "items=!items! "%%i""
    ) else (
        echo ����: "%%i" �����ڣ�������
    )
)
if defined items (
    "!rar_cmd!" a -afzip -ep1 -r -y "%zipname%" !items!
) else (
    echo δѡ���κ���Ч��Ŀ��
)
goto :success

:compress_from_list
echo ���ڸ����ļ��б����ѹ��...

:: ������ʱ�б��ļ�
set "tempfile=%temp%\rar_list_%random%.txt"
type nul > "%tempfile%"

set "count=0"

:: ����ͳһ�ļ��б�
if exist "%filelist%" (
    for /f "usebackq delims=" %%i in ("%filelist%") do (
        set "line=%%i"
        call :trim_spaces line
        
        if not "!line!"=="" (
            :: ���������кͷָ���
            echo !line! | findstr /r /c:"^��ǰĿ¼" >nul
            if errorlevel 1 (
                echo !line! | findstr /r /c:"^====" >nul
                if errorlevel 1 (
                    echo !line! | findstr /r /c:"^\\[�ļ���\\]" >nul
                    if errorlevel 1 (
                        echo !line! | findstr /r /c:"^\\[�ļ�\\]" >nul
                        if errorlevel 1 (
                            :: �����Ŀ�Ƿ����
                            if exist "!line!\" (
                                echo ����ļ���: !line!
                                echo "!line!" >> "%tempfile%"
                                set /a count+=1
                            ) else if exist "!line!" (
                                echo ����ļ�: !line!
                                echo "!line!" >> "%tempfile%"
                                set /a count+=1
                            ) else (
                                echo ����: "!line!" �����ڣ�������
                            )
                        )
                    )
                )
            )
        )
    )
)

:: ʹ���б��ļ�����ѹ��
if !count! gtr 0 (
    echo ����ѹ�� !count! ����Ŀ...
    "!rar_cmd!" a -afzip -ep1 -r -y "%zipname%" @"%tempfile%"
) else (
    echo ���ļ��б���δ�ҵ���Ч��Ŀ��
    echo ��ȷ���ļ��б���ֻ����Ҫѹ�����ļ��к��ļ���
)

:: ɾ����ʱ�ļ�
del "%tempfile%" >nul 2>&1
goto :eof

:trim_spaces
:: ȥ������ֵ��ǰ��ո�
set "tmp=!%1!"
:trim_loop
if not "!tmp!"=="" (
    if "!tmp:~0,1!"==" " (
        set "tmp=!tmp:~1!"
        goto trim_loop
    )
)
:trim_loop_end
if not "!tmp!"=="" (
    if "!tmp:~-1!"==" " (
        set "tmp=!tmp:~0,-1!"
        goto trim_loop_end
    )
)
set "%1=!tmp!"
goto :eof

:success
if exist "%zipname%" (
    echo.
    echo ѹ���ɹ���ɣ�
    echo ���ɵ�ZIP�ļ�: %zipname%
    echo �ļ��б�: %filelist%
    echo.
    echo ѹ������С: 
    for %%I in ("%zipname%") do echo   %%~zI �ֽ�
) else (
    echo ѹ��ʧ�ܣ�
)

:end
echo.
echo ��������˳�...
pause >nul