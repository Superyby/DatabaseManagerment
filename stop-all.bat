@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: ============================================
:: 数据库管理微服务 - 自动停止脚本
:: 自动发现并停止所有服务
:: ============================================

echo.
echo ========================================
echo   数据库管理微服务 - 自动停止脚本
echo ========================================
echo.

cd /d "%~dp0"
set "STOP_COUNT=0"

echo [信息] 正在扫描并停止服务...
echo.

:: 停止所有 *-service 服务
for /d %%D in (*-service) do (
    if exist "%%D\src\main.rs" (
        set /a STOP_COUNT+=1
        echo [!STOP_COUNT!] 正在停止服务: %%D
        taskkill /f /fi "WINDOWTITLE eq %%D*" 2>nul
    )
)

:: 停止网关服务
if exist "gateway\src\main.rs" (
    set /a STOP_COUNT+=1
    echo [!STOP_COUNT!] 正在停止服务: gateway
    taskkill /f /fi "WINDOWTITLE eq gateway*" 2>nul
)

echo.
echo ========================================
echo   已停止 !STOP_COUNT! 个服务
echo ========================================
echo.
echo [提示] 按任意键关闭此窗口...
pause >nul
