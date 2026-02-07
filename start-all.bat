@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: ============================================
:: 数据库管理微服务 - 自动启动脚本
:: 自动发现并启动所有包含 main.rs 的服务
:: ============================================

echo.
echo ========================================
echo   数据库管理微服务 - 自动启动脚本
echo ========================================
echo.

:: 获取脚本所在目录
cd /d "%~dp0"
set "ROOT_DIR=%cd%"

:: 服务计数器
set "SERVICE_COUNT=0"

:: 定义服务启动顺序（如果需要特定顺序）
:: 带 -service 后缀的优先启动，gateway 最后启动
set "PRIORITY_SERVICES="

echo [信息] 正在扫描服务目录...
echo.

:: 第一轮：启动基础服务（*-service）
for /d %%D in (*-service) do (
    if exist "%%D\src\main.rs" (
        set /a SERVICE_COUNT+=1
        echo [!SERVICE_COUNT!] 正在启动服务: %%D
        start "%%D" cmd /k "cd /d "%ROOT_DIR%" && cargo run -p %%D"
        timeout /t 2 /nobreak >nul
    )
)

:: 第二轮：启动网关服务
if exist "gateway\src\main.rs" (
    set /a SERVICE_COUNT+=1
    echo [!SERVICE_COUNT!] 正在启动服务: gateway
    start "gateway" cmd /k "cd /d "%ROOT_DIR%" && cargo run -p gateway"
)

echo.
echo ========================================
echo   已启动 !SERVICE_COUNT! 个服务
echo ========================================
echo.
echo [提示] 每个服务在独立的命令行窗口中运行
echo [提示] 关闭对应窗口即可停止服务
echo [提示] 按任意键关闭此窗口...
echo.
pause >nul
