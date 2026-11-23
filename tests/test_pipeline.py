"""
Kiểm thử đơn vị cho các phép biến đổi PySpark (chạy: pytest -q).
Dữ liệu ở đây là vài dòng tự tạo NGAY TRONG TEST theo đúng định dạng JSON của DNSE,
chỉ để kiểm tra logic — không dùng cho phân tích.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
pyspark = pytest.importorskip("pyspark")

import spark_pipeline as sp  # noqa: E402

VN = timezone(timedelta(hours=7))


def _epoch(d: str) -> int:
    return int(datetime.fromisoformat(d).replace(tzinfo=VN).timestamp())


def _write(path: Path, symbol: str, dates: list[str], closes: list[float], fetched="2026-11-01T10:00:00+07:00"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "symbol": symbol, "kind": "stock", "fetched_at": fetched,
        "t": [_epoch(d) for d in dates], "o": closes, "h": [c * 1.01 for c in closes],
        "l": [c * 0.99 for c in closes], "c": closes, "v": [1000.0] * len(closes)}))


@pytest.fixture(scope="module")
def spark():
    s = sp.build_spark("tests")
    yield s
    s.stop()


def test_explode_and_vietnam_date(spark, tmp_path):
    _write(tmp_path / "AAA" / "2024.json", "aaa", ["2024-01-02", "2024-01-03"], [10.0, 10.5])
    bars = sp.explode_bars(sp.read_raw_json(spark, tmp_path))
    rows = bars.orderBy("date").collect()
    assert len(rows) == 2
    assert rows[0].ticker == "AAA"                       # chuẩn hóa chữ hoa
    assert str(rows[0].date) == "2024-01-02"             # epoch -> ngày theo giờ Việt Nam
    assert rows[1].close == 10.5


def test_dedup_keeps_latest_download(spark, tmp_path):
    _write(tmp_path / "AAA" / "a.json", "AAA", ["2024-01-02"], [10.0], fetched="2026-01-01T00:00:00+07:00")
    _write(tmp_path / "AAA" / "b.json", "AAA", ["2024-01-02"], [11.0], fetched="2026-02-01T00:00:00+07:00")
    bars = sp.explode_bars(sp.read_raw_json(spark, tmp_path))
    out = sp.dedup_latest(bars).collect()
    assert len(out) == 1 and out[0].close == 11.0


def test_abnormal_return_is_flagged_not_rewritten(spark, tmp_path, monkeypatch):
    from datetime import date
    monkeypatch.setattr(sp.config, "START_DATE", date(2024, 1, 1))
    monkeypatch.setattr(sp.config, "END_DATE", date(2024, 12, 31))
    # 2 mã thuộc cùng 1 ngành; AAA có cú "chia tách" -50% chưa điều chỉnh
    dates = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    _write(tmp_path / "stock" / "AAA" / "2024.json", "AAA", dates, [20.0, 20.4, 10.2, 10.3])
    _write(tmp_path / "stock" / "BBB" / "2024.json", "BBB", dates, [30.0, 30.3, 30.0, 33.6])   # +12% (biên độ cũ HNX/UPCoM)
    univ = spark.createDataFrame(
        [("AAA", "A", "HOSE", "1", "S1", "Ngành 1"), ("BBB", "B", "HOSE", "1", "S1", "Ngành 1")],
        sp.UNIVERSE_SCHEMA)
    market = spark.createDataFrame([(datetime.fromisoformat(d).date(),) for d in dates], "date date")
    bars = sp.explode_bars(sp.read_raw_json(spark, tmp_path / "stock"))
    clean, dq, _ = sp.quality_and_clean(bars, univ, market)
    r = {(x.ticker, str(x.date)): x for x in clean.collect()}
    ev = r[("AAA", "2024-01-04")]
    assert ev.is_abnormal_ret is True
    assert ev.ret_clean is None                         # loại khỏi thống kê
    assert ev.close == pytest.approx(10.2)              # nhưng KHÔNG bịa lại giá
    assert r[("BBB", "2024-01-04")].is_abnormal_ret is False
    bb = r[("BBB", "2024-01-05")]                       # vượt biên độ HOSE 7% nhưng < 15%: gắn cờ, vẫn tính
    assert bb.is_band_breach is True and bb.is_abnormal_ret is False
    assert bb.ret_clean == pytest.approx(0.12)
    assert next(d for d in dq if d["check_id"] == "DQ-09b")["so_vi_pham"] == 1


def test_underwater_days_and_cvar_95(spark):
    from datetime import date
    # Chuỗi drawdown: đỉnh -> 3 phiên chìm -> đỉnh mới -> 1 phiên chìm => max_underwater_days = 3
    dd_rows = [
        ("S1", date(2024, 1, 2), 0.0),
        ("S1", date(2024, 1, 3), -0.05),
        ("S1", date(2024, 1, 4), -0.10),
        ("S1", date(2024, 1, 5), -0.02),
        ("S1", date(2024, 1, 8), 0.0),
        ("S1", date(2024, 1, 9), -0.03),
    ]
    df_dd = spark.createDataFrame(dd_rows, "sector_code string, date date, drawdown double")
    uw = sp._max_underwater(df_dd, "sector_code", "drawdown").collect()
    assert len(uw) == 1 and uw[0].max_underwater_days == 3

    # 20 phiên lợi suất từ -0.10 đến +0.09 -> 5% xấu nhất là -0.10
    ret_rows = [("S1", -0.10 + 0.01 * i) for i in range(20)]
    df_ret = spark.createDataFrame(ret_rows, "sector_code string, ret double")
    cv = sp._cvar_95(df_ret, "sector_code", "ret").collect()
    assert len(cv) == 1 and cv[0].cvar_95 <= -0.09


def test_microstructure_features_amihud_and_limit_hit(spark, tmp_path, monkeypatch):
    from datetime import date
    monkeypatch.setattr(sp.config, "START_DATE", date(2024, 1, 1))
    monkeypatch.setattr(sp.config, "END_DATE", date(2024, 12, 31))
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    # Phiên 2 tăng đúng trần HOSE +7% (20.0 -> 21.4)
    _write(tmp_path / "stock" / "AAA" / "2024.json", "AAA", dates, [20.0, 21.4, 21.6])
    univ = spark.createDataFrame([("AAA", "A", "HOSE", "1", "S1", "Ngành 1")], sp.UNIVERSE_SCHEMA)
    market = spark.createDataFrame(
        [(datetime.fromisoformat(d).date(), 1200.0, 0.005, 1150.0, "Tăng (trên MA200)", 1200.0, 0.005) for d in dates],
        "date date, mkt_close double, mkt_ret double, mkt_ma200 double, regime string, vn30_close double, vn30_ret double",
    )
    bars = sp.explode_bars(sp.read_raw_json(spark, tmp_path / "stock"))
    clean, _, _ = sp.quality_and_clean(bars, univ, market)
    feat = sp.add_features(clean, market).orderBy("date").collect()
    assert feat[1].is_limit_hit is True
    assert feat[2].is_limit_hit is False
    assert feat[1].amihud > 0
    # avg_val_20d_lag ở phiên 2 phải chỉ dùng phiên 1 (không look-ahead)
    assert feat[1].avg_val_20d_lag == pytest.approx(feat[0].traded_value_bn)
