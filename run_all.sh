#!/usr/bin/env bash
# Chạy toàn bộ luồng: kiểm tra -> DNSE -> PySpark -> Dashboard -> test -> Streamlit
cd "$(dirname "$0")"
PY=$(command -v python3 || command -v python)
[ -z "$PY" ] && { echo "[LOI] Khong tim thay Python"; exit 1; }
export PYTHONUTF8=1
step() { echo "===== $1 ====="; shift; "$@" || { echo "*** Loi o buoc tren - copy toan bo noi dung de duoc ho tro ***"; exit 1; }; }
step "0. Kiem tra moi truong" "$PY" src/check_env.py
step "1. Lay du lieu DNSE"     "$PY" src/fetch_dnse.py
step "2. Pipeline PySpark"     "$PY" src/spark_pipeline.py
step "3. Dashboard + insight"  "$PY" src/build_dashboard.py
"$PY" -m pytest -q tests
"$PY" -m streamlit run app/streamlit_app.py
