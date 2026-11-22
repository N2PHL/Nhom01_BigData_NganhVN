"""
KIỂM TRA MÔI TRƯỜNG trước khi chạy pipeline — in ra lỗi cụ thể và cách sửa.
Chạy:  python src/check_env.py            (đầy đủ, ~30 giây)
       python src/check_env.py --no-spark (bỏ qua bước khởi động thử Spark)
Gửi toàn bộ kết quả in ra cho người hỗ trợ nếu vẫn lỗi.
"""
from __future__ import annotations

import importlib
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402  (đồng thời đặt stdout UTF-8)

PROBLEMS: list[str] = []


def ok(msg):
    print(f"  [OK]   {msg}")


def bad(msg, fix):
    print(f"  [LOI]  {msg}\n         -> Cach sua: {fix}")
    PROBLEMS.append(msg)


def warn(msg):
    print(f"  [CHU Y] {msg}")


def check_python():
    print("\n1) Python")
    v = sys.version_info
    print(f"  {sys.executable}  (Python {platform.python_version()}, {platform.system()} {platform.machine()})")
    if v < (3, 9):
        bad("Python qua cu", "Cai Python 3.10-3.12 tu python.org")
    elif v >= (3, 14):
        warn("Python 3.14+ moi, PySpark co the chua ho tro; neu loi hay dung Python 3.11/3.12")
    else:
        ok("Phien ban Python phu hop")


def check_packages():
    print("\n2) Thu vien Python")
    need = ["pyspark", "pandas", "numpy", "pyarrow", "requests", "streamlit", "plotly"]
    missing = []
    for name in need:
        try:
            m = importlib.import_module(name)
            print(f"  {name:<10} {getattr(m, '__version__', '?')}")
        except Exception as exc:  # noqa: BLE001
            missing.append(name)
            print(f"  {name:<10} THIEU ({type(exc).__name__})")
    if missing:
        bad("Thieu thu vien: " + ", ".join(missing),
            f'"{sys.executable}" -m pip install -r requirements.txt')
    else:
        ok("Du thu vien")
    try:
        import pyspark
        if pyspark.__version__.startswith("3.") and sys.version_info >= (3, 12):
            try:
                import distutils  # noqa: F401
            except Exception:  # noqa: BLE001
                bad("PySpark 3.x tren Python 3.12+ can 'distutils'",
                    f'"{sys.executable}" -m pip install setuptools')
    except Exception:  # noqa: BLE001
        pass


def check_java():
    print("\n3) Java (bat buoc cho PySpark)")
    java_home = os.environ.get("JAVA_HOME")
    java = shutil.which("java")
    if java_home:
        cand = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if cand.exists():
            java = str(cand)
        else:
            bad(f"JAVA_HOME={java_home} nhung khong co {cand}", "Sua JAVA_HOME tro toi thu muc cai JDK (khong phai thu muc bin)")
    print(f"  JAVA_HOME = {java_home or '(chua dat)'}")
    if not java:
        bad("Khong tim thay Java",
            "Cai JDK 17 (https://adoptium.net), dat JAVA_HOME va them %JAVA_HOME%\\bin vao PATH, mo lai cua so lenh")
        return
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=20)
        txt = (out.stderr or out.stdout).strip().splitlines()
        line = next((x for x in txt if "version" in x), txt[0] if txt else "")
        print(f"  {java}\n  {line}")
        m = re.search(r'version "(\d+)(?:\.(\d+))?', line)
        major = int(m.group(1)) if m else 0
        if major == 1 and m.group(2):
            major = int(m.group(2))
        try:
            import pyspark
            spark_major = int(pyspark.__version__.split(".")[0])
        except Exception:  # noqa: BLE001
            spark_major = 0
        if spark_major >= 4 and major < 17:
            bad(f"PySpark {spark_major}.x can Java >= 17 (dang co Java {major})",
                'Cai JDK 17, hoac ha PySpark: pip install "pyspark==3.5.*" setuptools')
        elif spark_major == 3 and major not in (8, 11, 17, 21):
            warn(f"PySpark 3.5 chinh thuc ho tro Java 8/11/17 (dang co {major})")
        else:
            ok(f"Java {major} tuong thich")
    except Exception as exc:  # noqa: BLE001
        bad(f"Khong chay duoc java -version: {exc}", "Cai lai JDK 17")


def check_windows_hadoop():
    if os.name != "nt":
        return
    print("\n4) Windows: winutils / HADOOP_HOME (de Spark ghi Parquet)")
    hh = os.environ.get("HADOOP_HOME")
    print(f"  HADOOP_HOME = {hh or '(chua dat)'}")
    if not hh or not (Path(hh) / "bin" / "winutils.exe").exists():
        bad("Thieu winutils.exe",
            "Tai winutils.exe + hadoop.dll ban Hadoop 3.3 vao C:\\hadoop\\bin, dat HADOOP_HOME=C:\\hadoop, "
            "them C:\\hadoop\\bin vao PATH (hoac chay bang WSL2 / Google Colab)")
    else:
        ok("Co winutils.exe")
        if not (Path(hh) / "bin" / "hadoop.dll").exists():
            warn("Thieu hadoop.dll trong HADOOP_HOME\\bin (co the gay loi UnsatisfiedLinkError)")


def check_network():
    print("\n5) Ket noi API DNSE")
    try:
        import time
        import requests
        now = int(time.time())
        url = "https://services.entrade.com.vn/chart-api/v2/ohlcs/stock"
        r = requests.get(url, params={"symbol": "VCB", "resolution": "1D", "from": now - 30 * 86400, "to": now},
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        print(f"  HTTP {r.status_code}  {r.url}")
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        n = len(data.get("t", []) or []) if isinstance(data, dict) else 0
        if r.status_code == 200 and n > 0:
            ok(f"API tra ve {n} phien cua VCB, gia dong cua gan nhat = {data['c'][-1]}")
        else:
            bad(f"API tra ve trang thai {r.status_code}, {n} phien. Noi dung: {r.text[:200]}",
                "Kiem tra mang/VPN/tuong lua; neu API DNSE doi dia chi, bao lai de cap nhat fetch_dnse.py")
    except Exception as exc:  # noqa: BLE001
        bad(f"Khong goi duoc API DNSE: {type(exc).__name__}: {exc}",
            "Kiem tra Internet; tat VPN/proxy cong ty; may o nuoc ngoai/Colab co the bi chan -> chay fetch tren may o VN")


def check_spark():
    print("\n6) Khoi dong thu Spark + ghi Parquet")
    try:
        import spark_pipeline as sp
        spark = sp.build_spark("check_env")
        df = spark.createDataFrame([(1, "a"), (2, "b")], "x int, y string")
        with tempfile.TemporaryDirectory() as d:
            df.write.mode("overwrite").parquet(os.path.join(d, "t.parquet"))
            n = spark.read.parquet(os.path.join(d, "t.parquet")).count()
        spark.stop()
        ok(f"Spark {spark.version} chay duoc, doc/ghi Parquet OK ({n} dong)")
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).splitlines()[0][:300]
        fix = "Xem loi Java/winutils o tren"
        if "Python worker" in str(exc) or "PYSPARK_PYTHON" in str(exc):
            fix = f'Dat bien moi truong PYSPARK_PYTHON="{sys.executable}"'
        elif "Java gateway" in str(exc) or "JAVA_HOME" in str(exc):
            fix = "Cai JDK 17 va dat JAVA_HOME (muc 3)"
        elif "winutils" in str(exc).lower() or "HADOOP_HOME" in str(exc) or "UnsatisfiedLink" in str(exc):
            fix = "Cai winutils (muc 4) hoac dung WSL2/Colab"
        bad(f"Spark loi: {type(exc).__name__}: {msg}", fix)


def check_data():
    print("\n7) Du lieu da co")
    n_json = len(list(config.RAW_STOCK_DIR.rglob("*.json"))) if config.RAW_STOCK_DIR.exists() else 0
    n_gold = len(list(config.GOLD_CSV_DIR.glob("*.csv"))) if config.GOLD_CSV_DIR.exists() else 0
    print(f"  File JSON tho: {n_json} | Bang Gold CSV: {n_gold}")
    if n_json == 0:
        warn("Chua co du lieu tho -> buoc 1 (fetch_dnse.py) chua chay thanh cong")


def main() -> int:
    print("=== KIEM TRA MOI TRUONG — project 11 nganh (Nhom 01) ===")
    print(f"Thu muc project: {config.ROOT}")
    check_python()
    check_packages()
    check_java()
    check_windows_hadoop()
    check_network()
    if "--no-spark" not in sys.argv:
        check_spark()
    check_data()
    print("\n=== KET LUAN ===")
    if PROBLEMS:
        print(f"Co {len(PROBLEMS)} van de can sua:")
        for i, p in enumerate(PROBLEMS, 1):
            print(f"  {i}. {p}")
        return 1
    print("Moi thu san sang. Chay tiep: run_all.bat (Windows) hoac ./run_all.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
