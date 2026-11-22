"""
BƯỚC 1 — THU THẬP DỮ LIỆU TỪ API DNSE (Entrade chart-api v2)
=============================================================
Lấy dữ liệu giá–khối lượng theo ngày (OHLCV, resolution=1D) của các mã trong
`config/universe.csv` (11 ngành ICB cấp 2) và các chỉ số VNINDEX, VN30.

Thiết kế lớp Bronze (dữ liệu thô, không chỉnh sửa):
  - Mỗi mã được gọi theo từng năm -> mỗi lần gọi lưu thành 1 file JSON riêng:
        data/raw/dnse/stock/<TICKER>/<YEAR>.json
        data/raw/dnse/index/<INDEX>/<YEAR>.json
    Nội dung file = nguyên văn phản hồi API (mảng t/o/h/l/c/v) + metadata
    (symbol, loại, thời điểm lấy, URL). Spark ở bước 2 đọc toàn bộ thư mục này.
  - `fetch_log.csv` ghi lại từng lần gọi (mã, năm, số nến, trạng thái) để kiểm chứng nguồn.

Chạy (cần Internet tới services.entrade.com.vn):
    python src/fetch_dnse.py                       # mặc định 2017-01-01 -> hôm nay
    python src/fetch_dnse.py --start 2016-01-01 --workers 4
    python src/fetch_dnse.py --tickers VCB,FPT      # thử nhanh vài mã

Ghi chú:
  - Giá cổ phiếu DNSE trả về theo đơn vị NGHÌN ĐỒNG; khối lượng theo cổ phiếu.
  - Script KHÔNG sửa hay điền dữ liệu; mọi xử lý chất lượng nằm ở pipeline PySpark.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

BASE_URL = "https://services.entrade.com.vn/chart-api/v2/ohlcs"
HEADERS = {"User-Agent": "Mozilla/5.0 (Nhom01 BigData coursework)", "Accept": "application/json"}
VN_TZ = timezone(timedelta(hours=7))


def _to_epoch(d: date, end_of_day: bool = False) -> int:
    t = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=VN_TZ) if end_of_day else \
        datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=VN_TZ)
    return int(t.timestamp())


def year_chunks(start: date, end: date):
    """Chia [start, end] thành các đoạn theo năm dương lịch."""
    y = start.year
    while y <= end.year:
        s = max(start, date(y, 1, 1))
        e = min(end, date(y, 12, 31))
        if s <= e:
            yield y, s, e
        y += 1


def fetch_one(session: requests.Session, kind: str, symbol: str, s: date, e: date,
              retries: int = 3, timeout: int = 20) -> tuple[dict | None, str, str]:
    """Gọi API 1 lần cho (symbol, đoạn thời gian). Trả (json, trạng thái, url)."""
    params = {"symbol": symbol, "resolution": "1D",
              "from": _to_epoch(s), "to": _to_epoch(e, end_of_day=True)}
    url = f"{BASE_URL}/{kind}"
    full_url = requests.Request("GET", url, params=params).prepare().url
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 429:                      # bị giới hạn tần suất -> chờ lâu hơn
                time.sleep(3 * attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict) or not data.get("t"):
                return None, "empty", full_url
            return data, "ok", full_url
        except Exception as exc:                          # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
            time.sleep(1.5 * attempt)
    return None, f"error ({last_err})", full_url


def fetch_symbol(kind: str, symbol: str, start: date, end: date, out_root: Path,
                 overwrite: bool, pause: float) -> list[dict]:
    """Lấy toàn bộ các năm của 1 mã, ghi mỗi năm 1 file JSON. Trả về các dòng log."""
    logs = []
    session = requests.Session()
    sym_dir = out_root / symbol
    sym_dir.mkdir(parents=True, exist_ok=True)
    for year, s, e in year_chunks(start, end):
        fpath = sym_dir / f"{year}.json"
        is_current_year = year == date.today().year
        if fpath.exists() and not overwrite and not is_current_year:
            n = len(json.loads(fpath.read_text(encoding="utf-8")).get("t", []))
            logs.append(dict(kind=kind, symbol=symbol, year=year, n_bars=n, status="cached", url=""))
            continue
        data, status, url = fetch_one(session, kind, symbol, s, e)
        n = 0
        if data is not None:
            n = len(data["t"])
            record = {
                "symbol": symbol,
                "kind": kind,
                "resolution": "1D",
                "source": "DNSE/Entrade chart-api v2",
                "request_url": url,
                "fetched_at": datetime.now(VN_TZ).isoformat(timespec="seconds"),
                "t": data.get("t"), "o": data.get("o"), "h": data.get("h"),
                "l": data.get("l"), "c": data.get("c"), "v": data.get("v"),
            }
            # 1 dòng JSON / file (JSON Lines) -> Spark đọc song song không cần multiLine
            fpath.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
        logs.append(dict(kind=kind, symbol=symbol, year=year, n_bars=n, status=status, url=url))
        time.sleep(pause)
    return logs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Tải OHLCV ngày từ API DNSE cho danh mục 11 ngành")
    ap.add_argument("--start", default=config.START_DATE.isoformat())
    ap.add_argument("--end", default=config.END_DATE.isoformat())
    ap.add_argument("--universe", default=str(config.UNIVERSE_CSV))
    ap.add_argument("--out", default=str(config.RAW_DIR))
    ap.add_argument("--tickers", default="", help="Chỉ lấy các mã này (phân tách bằng dấu phẩy)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--pause", type=float, default=0.3, help="Nghỉ giữa 2 lần gọi của 1 luồng (giây)")
    ap.add_argument("--overwrite", action="store_true", help="Tải lại cả các năm đã có file")
    args = ap.parse_args(argv)

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    out = Path(args.out)
    with open(args.universe, encoding="utf-8") as f:
        tickers = [r["ticker"].strip().upper() for r in csv.DictReader(f)]
    if args.tickers:
        wanted = {t.strip().upper() for t in args.tickers.split(",")}
        missing = wanted - set(tickers)
        if missing:
            print(f"[fetch] ⚠ Bỏ qua mã không có trong universe.csv: {', '.join(sorted(missing))}")
        tickers = [t for t in tickers if t in wanted]

    jobs = [("stock", t, start, end, out / "stock") for t in tickers]
    idx_start = start - timedelta(days=config.INDEX_LOOKBACK_DAYS)
    jobs += [("index", i, idx_start, end, out / "index") for i in config.INDEX_SYMBOLS]

    print(f"[fetch] {len(tickers)} mã + {len(config.INDEX_SYMBOLS)} chỉ số | {start} -> {end} | {args.workers} luồng")
    all_logs: list[dict] = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch_symbol, k, s, a, b, o, args.overwrite, args.pause): s for k, s, a, b, o in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            sym = futs[fut]
            logs = fut.result()
            all_logs.extend(logs)
            n = sum(x["n_bars"] for x in logs)
            bad = [x for x in logs if x["status"].startswith("error")]
            print(f"  [{i:>3}/{len(jobs)}] {sym:<8} {n:>6} phiên" + (f"  ⚠ {len(bad)} năm lỗi" if bad else ""))

    out.mkdir(parents=True, exist_ok=True)
    with open(out / "fetch_log.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "symbol", "year", "n_bars", "status", "url"])
        w.writeheader()
        w.writerows(sorted(all_logs, key=lambda x: (x["kind"], x["symbol"], x["year"])))

    n_stock = sum(x["n_bars"] for x in all_logs if x["kind"] == "stock")
    n_err = sum(1 for x in all_logs if x["status"].startswith("error"))
    empty = sorted({x["symbol"] for x in all_logs if x["kind"] == "stock"} -
                   {x["symbol"] for x in all_logs if x["kind"] == "stock" and x["n_bars"] > 0})
    print(f"\n[fetch] Xong trong {time.perf_counter() - t0:.0f}s: {n_stock:,} dòng cổ phiếu "
          f"| {n_err} lần gọi lỗi | log: {out / 'fetch_log.csv'}")
    if empty:
        print(f"[fetch] ⚠ Không có dữ liệu cho: {', '.join(empty)} -> kiểm tra mã hoặc thay mã khác trong universe.csv")
    if n_stock < 100_000:
        print("[fetch] ⚠ Chưa đủ 100.000 dòng: mở rộng --start hoặc thêm mã vào config/universe.csv")
    if n_err:
        print("[fetch] Chạy lại lệnh để tải bù các năm lỗi (các năm đã có file sẽ được bỏ qua).")
    return 0 if n_stock > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
