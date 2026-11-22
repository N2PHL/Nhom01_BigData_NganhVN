"""
Cấu hình tập trung cho toàn bộ project (đường dẫn, giai đoạn, tham số phân tích).

Mọi script (fetch, pipeline Spark, dashboard, Streamlit) đều đọc từ file này,
nên muốn đổi giai đoạn / thư mục chỉ cần sửa ở đây hoặc đặt biến môi trường.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

# In tiếng Việt an toàn trên cửa sổ lệnh Windows (tránh UnicodeEncodeError khi console không phải UTF-8)
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        pass

# ---------------------------------------------------------------------------
# Đường dẫn (tương đối theo thư mục gốc project -> chạy được trên mọi máy)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT / "data"))

UNIVERSE_CSV = ROOT / "config" / "universe.csv"          # danh mục 11 ngành ICB cấp 2

RAW_DIR = DATA_DIR / "raw" / "dnse"                       # Bronze: JSON gốc từ API DNSE
RAW_STOCK_DIR = RAW_DIR / "stock"
RAW_INDEX_DIR = RAW_DIR / "index"
FETCH_LOG_CSV = RAW_DIR / "fetch_log.csv"

SILVER_DIR = DATA_DIR / "silver"                          # Silver: bảng giao dịch đã làm sạch
SILVER_PANEL = SILVER_DIR / "daily_panel"                 # Parquet partitionBy(sector_code, year)

GOLD_DIR = DATA_DIR / "gold"                              # Gold: bảng KPI phục vụ BI / Streamlit
GOLD_PARQUET_DIR = GOLD_DIR / "parquet"
GOLD_CSV_DIR = GOLD_DIR / "csv"                           # CSV UTF-8 BOM để Power BI / Excel đọc đúng tiếng Việt

REPORTS_DIR = ROOT / "reports"
DASHBOARD_HTML = ROOT / "dashboard" / "BI_Dashboard_Nganh_VN.html"

# ---------------------------------------------------------------------------
# Giai đoạn dữ liệu (10 năm). Có thể ghi đè bằng biến môi trường START / END.
# ---------------------------------------------------------------------------
START_DATE = date.fromisoformat(os.environ.get("START", "2017-01-01"))
END_DATE = date.fromisoformat(os.environ.get("END", date.today().isoformat()))
# Chỉ số được lấy sớm hơn ~400 ngày để MA200 của VN-Index có đủ dữ liệu ngay từ đầu giai đoạn
INDEX_LOOKBACK_DAYS = 400
INDEX_SYMBOLS = ["VNINDEX", "VN30"]
MARKET_INDEX = "VNINDEX"

# ---------------------------------------------------------------------------
# Tham số phân tích
# ---------------------------------------------------------------------------
TRADING_DAYS = 252
RF_ANNUAL = float(os.environ.get("RF_ANNUAL", "0.03"))   # lãi suất phi rủi ro giả định 3%/năm (sửa theo báo cáo)
# Biên độ dao động giá theo sàn hiện hành (HOSE ±7%, HNX ±10%, UPCoM ±15%) + dung sai làm tròn bước giá
PRICE_BAND = {"HOSE": 0.07, "HNX": 0.10, "UPCOM": 0.15}
BAND_TOLERANCE = 0.005
# Giá DNSE đã điều chỉnh theo sự kiện quyền, và nhiều mã từng niêm yết ở UPCoM/HNX (biên độ rộng hơn sàn hiện tại).
# => Chỉ LOẠI lợi suất vượt biên độ rộng nhất của phiên thường (UPCoM 15%) + dung sai; vượt biên độ sàn hiện tại
#    chỉ GẮN CỜ để giải thích (thường là giai đoạn trước khi chuyển sàn, hoặc phiên chào sàn).
MAX_DAILY_MOVE = PRICE_BAND["UPCOM"] + BAND_TOLERANCE
VOLUME_Z_THRESHOLD = 4.0          # |z| của log(khối lượng) so với 60 phiên trước -> phiên đột biến thanh khoản
BETA_WINDOW = 250                 # beta trượt ~1 năm
MA_LONG = 200                     # VN-Index trên/dưới MA200 -> chế độ thị trường Tăng/Giảm
