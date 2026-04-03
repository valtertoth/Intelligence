@echo off
title Toth Intelligence v2
color 0A

echo.
echo   ╔═══════════════════════════════════════════╗
echo   ║   Toth Intelligence v2                    ║
echo   ║   Multi-AI Meta Ads Platform              ║
echo   ╚═══════════════════════════════════════════╝
echo.

:: Check Node.js
node --version >nul 2>&1
if %errorlevel% neq 0 (
    echo   ERRO: Node.js nao encontrado!
    echo.
    echo   Instale em: https://nodejs.org  ^(versao LTS^)
    echo   Depois clique duas vezes neste arquivo novamente.
    echo.
    pause
    exit /b 1
)

echo   Node.js encontrado! Iniciando servidor...
echo.

:: Open browser after 2 seconds
start /b cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8765"

:: Start server
node server.js

echo.
echo   Servidor encerrado.
pause
