@echo off
chcp 65001 >nul
setlocal
set PYTHONUTF8=1
cd /d %~dp0

REM Chon lenh Python: thu "python" (co the la ban gia cua Microsoft Store), roi den "py"
set PY=
python --version >nul 2>nul && set PY=python
if not defined PY (py --version >nul 2>nul && set PY=py)
if not defined PY (echo [LOI] Khong tim thay Python. Cai Python 3.11/3.12 tu python.org, tick "Add python.exe to PATH". & goto :fail)
%PY% --version

echo ===== 0. Kiem tra moi truong =====
%PY% src\check_env.py || goto :fail

echo ===== 1. Lay du lieu DNSE =====
%PY% src\fetch_dnse.py || goto :fail

echo ===== 2. Pipeline PySpark =====
%PY% src\spark_pipeline.py || goto :fail

echo ===== 3. Dashboard + insight =====
%PY% src\build_dashboard.py || goto :fail

echo ===== 4. Kiem thu =====
%PY% -m pytest -q tests

echo ===== 5. Mo Streamlit (Ctrl+C de dung) =====
%PY% -m streamlit run app\streamlit_app.py
goto :end

:fail
echo.
echo *** Co loi o buoc tren. Chup man hinh / copy toan bo noi dung cua so nay de duoc ho tro. ***
:end
pause
