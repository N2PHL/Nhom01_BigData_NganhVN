"""
BƯỚC 2 — PIPELINE PYSPARK: Bronze (JSON DNSE) -> Silver (giao dịch sạch) -> Gold (KPI)
======================================================================================
Bài toán: So sánh hiệu quả đầu tư, rủi ro, dòng tiền và mức độ đa dạng hóa của
11 ngành ICB cấp 2 trên TTCK Việt Nam trong ~10 năm (dữ liệu thật từ API DNSE).

Các bước (mỗi bước ghi số dòng vào reports/pipeline_metrics.json):
  1. Đọc dữ liệu đa nguồn: JSON cổ phiếu (nhiều file/năm/mã), JSON chỉ số, CSV phân ngành
     -> schema khai báo tường minh (StructType), explode mảng t/o/h/l/c/v thành dòng.
  2. Kiểm tra schema & chất lượng dữ liệu (DQ-01..DQ-13) -> data_quality_report.
  3. Làm sạch: khử trùng lặp, loại giá không hợp lệ, sửa OHLC lệch, xử lý khối lượng thiếu,
     gắn cờ biến động bất thường (vượt biên độ sàn / sau tạm ngừng giao dịch) thay vì bịa giá.
  4. Tích hợp: join (broadcast) bảng phân ngành, join chỉ số VN-Index theo ngày,
     xác định chế độ thị trường (VN-Index trên/dưới MA200).
  5. Biến đổi theo thời gian bằng Window: lợi suất, chỉ số tài sản, drawdown, biến động 20 phiên,
     beta trượt 250 phiên, z-score thanh khoản.
  6. Lưu Silver: Parquet phân vùng partitionBy(sector_code, year), đọc lại từ Parquet.
  7. Gold: groupBy/aggregation + Spark SQL + self-join -> 10 bảng KPI (Parquet + CSV).

Chạy:  python src/spark_pipeline.py            (cần Java 17+ cho PySpark 4.x, hoặc Java 8/11/17 cho 3.5)
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType, DoubleType, LongType, StringType, StructField, StructType,
)

# ---------------------------------------------------------------------------
# Schema tường minh cho dữ liệu nguồn (không dùng inferSchema để tránh sai kiểu)
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("kind", StringType()),
    StructField("resolution", StringType()),
    StructField("source", StringType()),
    StructField("request_url", StringType()),
    StructField("fetched_at", StringType()),
    StructField("t", ArrayType(LongType())),
    StructField("o", ArrayType(DoubleType())),
    StructField("h", ArrayType(DoubleType())),
    StructField("l", ArrayType(DoubleType())),
    StructField("c", ArrayType(DoubleType())),
    StructField("v", ArrayType(DoubleType())),
])

UNIVERSE_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("company_name", StringType()),
    StructField("exchange", StringType()),
    StructField("icb2_code", StringType()),
    StructField("sector_code", StringType()),
    StructField("sector_name", StringType()),
])

MIN_SECTOR_CONSTITUENTS = 3      # ngày nào ngành có < 3 mã có lợi suất hợp lệ thì không tính lợi suất ngành
LONG_GAP_DAYS = 10               # > 10 ngày lịch giữa 2 phiên liên tiếp => coi như tạm ngừng giao dịch

METRICS: dict = {"stages": {}}


def log(msg: str) -> None:
    print(f"[spark] {msg}", flush=True)


def stage(name: str, **values) -> None:
    METRICS["stages"][name] = values
    log(f"{name}: " + ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}={v}" for k, v in values.items()))


# ===========================================================================
# 0. SparkSession
# ===========================================================================
def build_spark(app_name: str = "Nhom01_BigData_11Nganh_VN") -> SparkSession:
    # Windows hay lỗi "Python worker failed to connect back" vì Spark gọi nhầm python khác -> chỉ định rõ
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")   # epoch DNSE -> ngày giao dịch giờ VN
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Spark 4 bật ANSI mặc định (chia 0 -> lỗi), Spark 3.5 thì tắt (chia 0 -> NULL).
        # Cố định = false để kết quả như nhau trên 2 phiên bản; các phép chia quan trọng đã chặn mẫu số 0.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ===========================================================================
# 1. ĐỌC DỮ LIỆU ĐA NGUỒN
# ===========================================================================
def read_raw_json(spark: SparkSession, path: Path) -> DataFrame:
    """Đọc đệ quy toàn bộ file JSON <SYMBOL>/<YEAR>.json bằng schema cố định."""
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .json(str(path))
        .withColumn("source_file", F.input_file_name())
    )


def explode_bars(raw: DataFrame) -> DataFrame:
    """Mỗi file chứa mảng song song t/o/h/l/c/v -> zip & explode thành 1 dòng / phiên."""
    zipped = F.arrays_zip(F.col("t"), F.col("o"), F.col("h"), F.col("l"), F.col("c"), F.col("v"))
    return (
        raw.select(
            F.upper(F.trim("symbol")).alias("ticker"),
            F.to_timestamp("fetched_at").alias("fetched_at"),
            "source_file",
            F.explode(zipped).alias("b"),
        )
        .select(
            "ticker", "fetched_at", "source_file",
            F.col("b.t").alias("ts"),
            F.col("b.o").alias("open"),
            F.col("b.h").alias("high"),
            F.col("b.l").alias("low"),
            F.col("b.c").alias("close"),
            F.col("b.v").alias("volume"),
        )
        # epoch giây (UTC) -> ngày giao dịch theo múi giờ phiên Spark (Asia/Ho_Chi_Minh)
        .withColumn("date", F.to_date(F.timestamp_seconds("ts")))
    )


def read_universe(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", "true").option("encoding", "UTF-8")
        .schema(UNIVERSE_SCHEMA).csv(str(config.UNIVERSE_CSV))
        .withColumn("ticker", F.upper(F.trim("ticker")))
        .withColumn("exchange", F.upper(F.trim("exchange")))
        .dropDuplicates(["ticker"])
    )


def dedup_latest(df: DataFrame) -> DataFrame:
    """Giữ 1 bản ghi / (ticker, date): bản lấy về muộn nhất (file mới nhất)."""
    w = Window.partitionBy("ticker", "date").orderBy(F.col("fetched_at").desc_nulls_last(),
                                                     F.col("source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ===========================================================================
# 2. CHỈ SỐ THỊ TRƯỜNG (VN-INDEX, VN30) + CHẾ ĐỘ THỊ TRƯỜNG
# ===========================================================================
def build_market(index_bars: DataFrame) -> DataFrame:
    idx = dedup_latest(index_bars).filter(F.col("close") > 0)
    w = Window.partitionBy("ticker").orderBy("date")
    w200 = w.rowsBetween(-(config.MA_LONG - 1), 0)
    idx = (
        idx.withColumn("ret", F.col("close") / F.lag("close").over(w) - 1)
        .withColumn("ma200", F.avg("close").over(w200))
        .withColumn("n200", F.count("close").over(w200))
    )
    vni = idx.filter(F.col("ticker") == config.MARKET_INDEX).select(
        "date",
        F.col("close").alias("mkt_close"),
        F.col("ret").alias("mkt_ret"),
        F.col("ma200").alias("mkt_ma200"),
        F.when(F.col("n200") < config.MA_LONG, F.lit(None))
         .when(F.col("close") >= F.col("ma200"), F.lit("Tăng (trên MA200)"))
         .otherwise(F.lit("Giảm (dưới MA200)")).alias("regime"),
    )
    vn30 = idx.filter(F.col("ticker") == "VN30").select("date", F.col("close").alias("vn30_close"),
                                                       F.col("ret").alias("vn30_ret"))
    return vni.join(vn30, "date", "left")


# ===========================================================================
# 3. KIỂM TRA CHẤT LƯỢNG + LÀM SẠCH
# ===========================================================================
def quality_and_clean(bars: DataFrame, universe: DataFrame, market: DataFrame):
    """Trả về (bảng sạch đã có cờ chất lượng, danh sách kết quả DQ)."""
    dq: list[dict] = []

    def add(check_id, group, rule, n, base, action):
        dq.append(dict(check_id=check_id, nhom=group, quy_tac=rule, so_vi_pham=int(n),
                       tren_tong=int(base), ty_le_pct=round(100.0 * n / base, 4) if base else 0.0,
                       xu_ly=action))

    prof = bars.agg(F.count("*").alias("n"), F.countDistinct("source_file").alias("files"),
                    F.countDistinct("ticker").alias("tickers"),
                    F.min("date").alias("d0"), F.max("date").alias("d1")).first()
    n_raw = prof["n"]
    stage("1_read_bronze", rows=n_raw, files=prof["files"], tickers=prof["tickers"],
          date_range=f"{prof['d0']}..{prof['d1']}")

    # DQ-01: giá trị thiếu ở các trường bắt buộc
    null_any = (F.col("ts").isNull() | F.col("close").isNull() | F.col("open").isNull()
                | F.col("high").isNull() | F.col("low").isNull())
    add("DQ-01", "Missing", "Thiếu thời gian hoặc giá OHLC", bars.filter(null_any).count(), n_raw,
        "Loại bỏ dòng (không nội suy giá vì sẽ tạo giao dịch không có thật)")
    add("DQ-02", "Missing", "Thiếu khối lượng", bars.filter(F.col("volume").isNull()).count(), n_raw,
        "Điền 0 và gắn cờ volume_missing")

    # DQ-03: trùng lặp (ticker, date) — phát sinh khi các file năm chồng biên hoặc tải lại
    n_distinct = bars.select("ticker", "date").distinct().count()
    n_conflict = (bars.groupBy("ticker", "date")
                  .agg(F.countDistinct("close").alias("k")).filter("k > 1").count())
    add("DQ-03", "Duplicate", "Trùng khóa (ticker, date)", n_raw - n_distinct, n_raw,
        f"Giữ bản lấy muộn nhất (row_number); {n_conflict} khóa có giá đóng cửa khác nhau giữa các bản")
    df = dedup_latest(bars.filter(~null_any))

    # DQ-04: phạm vi thời gian
    in_window = F.col("date").between(F.lit(config.START_DATE), F.lit(config.END_DATE))
    n_before = df.count()
    add("DQ-04", "Validity", f"Ngoài giai đoạn {config.START_DATE}..{config.END_DATE}",
        df.filter(~in_window).count(), n_before, "Lọc bỏ (filter)")
    df = df.filter(in_window)

    # DQ-05: mã ngoài danh mục / mã trong danh mục không có dữ liệu
    tickers_raw = df.select("ticker").distinct()
    not_in_univ = tickers_raw.join(universe.select("ticker"), "ticker", "left_anti")
    no_data = universe.select("ticker").join(tickers_raw, "ticker", "left_anti")
    no_data_list = [r.ticker for r in no_data.collect()]
    add("DQ-05", "Integration", "Mã không khớp danh mục ngành (thừa / thiếu dữ liệu)",
        not_in_univ.count() + len(no_data_list), universe.count(),
        "Inner join với universe; thiếu dữ liệu: " + (", ".join(no_data_list) or "không có"))

    # Đơn vị giá: DNSE trả nghìn đồng. Nếu trung vị > 500 thì là đồng -> quy về nghìn đồng.
    med = df.groupBy("ticker").agg(F.percentile_approx("close", 0.5).alias("med_close"))
    df = (df.join(med, "ticker")
          .withColumn("_scale", F.when(F.col("med_close") > 500, F.lit(0.001)).otherwise(F.lit(1.0))))
    for c in ("open", "high", "low", "close"):
        df = df.withColumn(c, F.col(c) * F.col("_scale"))
    df = df.drop("med_close", "_scale")

    # Tích hợp phân ngành (broadcast join: bảng nhỏ 81 dòng)
    df = df.join(F.broadcast(universe), "ticker", "inner").cache()
    n_base = df.count()
    stage("2_dedup_window_universe", rows=n_base)
    if n_base == 0:
        raise RuntimeError("Không còn dòng nào sau khi lọc giai đoạn/danh mục: kiểm tra START/END "
                           "trong src/config.py và mã trong config/universe.csv")

    # DQ-06: giá <= 0 hoặc high < low -> không hợp lệ
    bad_px = (F.least("open", "high", "low", "close") <= 0) | (F.col("high") < F.col("low"))
    add("DQ-06", "Validity", "Giá <= 0 hoặc High < Low", df.filter(bad_px).count(), n_base, "Loại bỏ dòng")
    df = df.filter(~bad_px)

    # DQ-07: Open/Close nằm ngoài [Low, High] -> sửa biên High/Low (lệch làm tròn của nguồn)
    out_rng = (F.greatest("open", "close") > F.col("high")) | (F.least("open", "close") < F.col("low"))
    add("DQ-07", "Consistency", "Open/Close nằm ngoài [Low, High]", df.filter(out_rng).count(), n_base,
        "High = max(O,H,C), Low = min(O,L,C)")
    df = (df.withColumn("high", F.greatest("open", "high", "close"))
            .withColumn("low", F.least("open", "low", "close")))

    # DQ-02 xử lý + DQ-08: khối lượng âm / bằng 0
    df = (df.withColumn("volume_missing", F.col("volume").isNull())
            .withColumn("volume", F.when(F.col("volume").isNull() | (F.col("volume") < 0), F.lit(0.0))
                        .otherwise(F.col("volume")))
            .withColumn("is_no_trade", F.col("volume") == 0))
    add("DQ-08", "Validity", "Phiên khối lượng = 0 (không khớp lệnh)", df.filter("is_no_trade").count(), n_base,
        "Giữ dòng, gắn cờ is_no_trade (không dùng tính thanh khoản trung bình)")

    # Lợi suất ngày + khoảng cách giữa 2 phiên liên tiếp
    w = Window.partitionBy("ticker").orderBy("date")
    band = F.when(F.col("exchange") == "HOSE", F.lit(config.PRICE_BAND["HOSE"])) \
            .when(F.col("exchange") == "HNX", F.lit(config.PRICE_BAND["HNX"])) \
            .otherwise(F.lit(config.PRICE_BAND["UPCOM"]))
    df = (df.withColumn("prev_close", F.lag("close").over(w))
            .withColumn("prev_date", F.lag("date").over(w))
            .withColumn("gap_days", F.datediff("date", "prev_date"))
            .withColumn("ret_1d", F.when(F.col("prev_close") > 0, F.col("close") / F.col("prev_close") - 1))
            .withColumn("price_band", band))

    # DQ-09a: vượt biên độ của sàn HIỆN TẠI -> chỉ gắn cờ. Phần lớn là giai đoạn mã còn ở UPCoM/HNX
    #         (biên độ ±15%/±10%) trước khi chuyển sàn -> biến động hợp lệ, vẫn tính vào lợi suất.
    # DQ-09b: vượt biên độ rộng nhất của phiên thường (UPCoM ±15%) -> phiên chào sàn/chuyển sàn (biên độ
    #         đặc biệt) hoặc lỗi điều chỉnh giá -> KHÔNG sửa giá, chỉ loại khỏi thống kê lợi suất/rủi ro.
    band_breach = F.abs(F.col("ret_1d")) > (F.col("price_band") + config.BAND_TOLERANCE)
    abnormal = F.abs(F.col("ret_1d")) > config.MAX_DAILY_MOVE
    long_gap = F.col("gap_days") > LONG_GAP_DAYS
    df = (df.withColumn("is_band_breach", F.coalesce(band_breach, F.lit(False)))
            .withColumn("is_abnormal_ret", F.coalesce(abnormal, F.lit(False)))
            .withColumn("is_long_gap", F.coalesce(long_gap, F.lit(False)))
            .withColumn("ret_clean",
                        F.when(F.col("is_abnormal_ret") | F.col("is_long_gap"), F.lit(None))
                         .otherwise(F.col("ret_1d"))))
    add("DQ-09a", "Outlier", "|Lợi suất ngày| vượt biên độ sàn hiện tại (HOSE 7%, HNX 10%) + 0,5%",
        df.filter("is_band_breach").count(), n_base,
        "Chỉ gắn cờ is_band_breach, VẪN tính lợi suất (thường là giai đoạn mã còn niêm yết UPCoM/HNX)")
    add("DQ-09b", "Outlier", f"|Lợi suất ngày| > {config.MAX_DAILY_MOVE:.1%} (vượt biên độ rộng nhất phiên thường)",
        df.filter("is_abnormal_ret").count(), n_base,
        "Giữ giá gốc, ret_clean = NULL (phiên chào sàn/chuyển sàn hoặc lỗi giá); liệt kê trong abnormal_events")
    add("DQ-10", "Time", f"Khoảng trống > {LONG_GAP_DAYS} ngày giữa 2 phiên (tạm ngừng giao dịch)",
        df.filter("is_long_gap").count(), n_base, "ret_clean = NULL cho phiên mở lại")

    # DQ-11: đột biến thanh khoản (z-score log khối lượng so với 60 phiên TRƯỚC đó — không nhìn tương lai)
    w60 = w.rowsBetween(-60, -1)
    lv = F.log1p("volume")
    df = (df.withColumn("_lv", lv)
            .withColumn("_mu", F.avg(F.when(~F.col("is_no_trade"), F.col("_lv"))).over(w60))
            .withColumn("_sd", F.stddev(F.when(~F.col("is_no_trade"), F.col("_lv"))).over(w60))
            .withColumn("volume_z60", F.when(F.col("_sd") > 0, (F.col("_lv") - F.col("_mu")) / F.col("_sd")))
            .withColumn("is_volume_spike", F.coalesce(F.abs("volume_z60") > config.VOLUME_Z_THRESHOLD,
                                                      F.lit(False)))
            .drop("_lv", "_mu", "_sd")).cache()
    add("DQ-11", "Outlier", f"Khối lượng đột biến |z60| > {config.VOLUME_Z_THRESHOLD}",
        df.filter("is_volume_spike").count(), n_base, "Giữ dòng, gắn cờ (thông tin cho EDA dòng tiền)")

    # DQ-12: lịch giao dịch dựng từ chính dữ liệu cổ phiếu (ngày có >= 10 mã giao dịch) -> độ phủ từng mã
    cal = df.groupBy("date").agg(F.countDistinct("ticker").alias("_n")).filter("_n >= 10").select("date")
    span = df.groupBy("ticker").agg(F.min("date").alias("d0"), F.max("date").alias("d1"),
                                    F.count("*").alias("n_have"))
    expected = (span.join(cal, (cal.date >= span.d0) & (cal.date <= span.d1))
                .groupBy("ticker").agg(F.count("*").alias("n_expected")))
    coverage = span.join(expected, "ticker", "left")
    n_missing_sessions = coverage.select(F.sum(F.greatest(F.col("n_expected") - F.col("n_have"), F.lit(0)))).first()[0] or 0
    n_off_calendar = df.join(cal, "date", "left_anti").count()
    add("DQ-12", "Time", "Phiên thiếu của từng mã so với lịch giao dịch chung (trong khoảng niêm yết của mã)",
        n_missing_sessions, n_base,
        f"Không tạo dòng giả; lợi suất tính giữa 2 phiên có thật. {n_off_calendar} dòng ở ngày có < 10 mã giao dịch")
    # DQ-13: nguồn chỉ số thiếu ngày so với lịch giao dịch
    n_cal = cal.count()
    idx_missing = cal.join(market.select("date"), "date", "left_anti").count()
    add("DQ-13", "Integration", "Ngày giao dịch không có dữ liệu VN-Index từ nguồn DNSE", idx_missing, n_cal,
        "Không nội suy chỉ số; các ngày này không tính beta/capture/chế độ thị trường; CAGR tính theo năm lịch")

    return df, dq, coverage


# ===========================================================================
# 4–5. TÍCH HỢP THỊ TRƯỜNG + ĐẶC TRƯNG THEO WINDOW
# ===========================================================================
def add_features(df: DataFrame, market: DataFrame) -> DataFrame:
    df = df.join(market, "date", "left")
    w = Window.partitionBy("ticker").orderBy("date")
    w_cum = w.rowsBetween(Window.unboundedPreceding, 0)
    w20 = w.rowsBetween(-19, 0)
    w200 = w.rowsBetween(-199, 0)
    wb = w.rowsBetween(-(config.BETA_WINDOW - 1), 0)
    both = F.col("ret_clean").isNotNull() & F.col("mkt_ret").isNotNull()
    x, y = F.when(both, F.col("ret_clean")), F.when(both, F.col("mkt_ret"))
    return (
        df.withColumn("traded_value_bn", F.col("close") * F.col("volume") / 1e6)   # nghìn đồng × CP -> tỷ đồng
        # Chỉ số tài sản dựng từ ret_clean (miễn nhiễm với bước nhảy giá do sự kiện quyền)
        .withColumn("wealth", F.exp(F.sum(F.log1p(F.coalesce("ret_clean", F.lit(0.0)))).over(w_cum)))
        .withColumn("drawdown", F.col("wealth") / F.max("wealth").over(w_cum) - 1)
        .withColumn("vol_20d", F.stddev("ret_clean").over(w20) * math.sqrt(config.TRADING_DAYS))
        .withColumn("trend_vs_ma200", F.col("wealth") / F.avg("wealth").over(w200) - 1)
        .withColumn("excess_ret", F.col("ret_clean") - F.col("mkt_ret"))
        .withColumn("_n", F.count(x).over(wb))
        .withColumn("_ex", F.avg(x).over(wb)).withColumn("_ey", F.avg(y).over(wb))
        .withColumn("_exy", F.avg(x * y).over(wb)).withColumn("_eyy", F.avg(y * y).over(wb))
        .withColumn("beta_250d", F.when(F.col("_n") >= 120,
                    (F.col("_exy") - F.col("_ex") * F.col("_ey")) / F.nullif(F.col("_eyy") - F.col("_ey") ** 2, F.lit(0.0))))
        .drop("_n", "_ex", "_ey", "_exy", "_eyy")
        .withColumn("year", F.year("date"))
        .withColumn("month", F.date_trunc("month", "date").cast("date"))
        .withColumn("quarter", F.concat(F.year("date").cast("string"), F.lit("-Q"), F.quarter("date").cast("string")))
    )


# ===========================================================================
# 7. GOLD — BẢNG KPI PHỤC VỤ PHÂN TÍCH / BI
# ===========================================================================
def build_gold(spark: SparkSession, silver: DataFrame, market: DataFrame, coverage: DataFrame) -> dict[str, DataFrame]:
    rf, td = config.RF_ANNUAL, config.TRADING_DAYS
    silver.createOrReplaceTempView("silver")
    market.filter(F.col("date").between(F.lit(config.START_DATE), F.lit(config.END_DATE))) \
          .createOrReplaceTempView("market")
    gold: dict[str, DataFrame] = {}

    # (A) KPI từng mã — Spark SQL
    gold["ticker_kpi"] = spark.sql(f"""
        SELECT ticker, company_name, exchange, sector_code, sector_name,
               MIN(date) AS first_date, MAX(date) AS last_date, COUNT(*) AS n_sessions,
               COUNT(ret_clean) AS n_valid_returns,
               EXP(SUM(LN(1 + ret_clean))) - 1 AS total_return,
               POWER(EXP(SUM(LN(1 + ret_clean))), 365.25 / DATEDIFF(MAX(date), MIN(date))) - 1 AS cagr,
               STDDEV_SAMP(ret_clean) * SQRT({td}) AS ann_vol,
               (AVG(ret_clean) * {td} - {rf}) / (STDDEV_SAMP(ret_clean) * SQRT({td})) AS sharpe,
               MIN(drawdown) AS max_drawdown,
               COVAR_SAMP(ret_clean, mkt_ret) / VAR_SAMP(CASE WHEN ret_clean IS NOT NULL THEN mkt_ret END) AS beta,
               CORR(ret_clean, mkt_ret) AS corr_vnindex,
               AVG(CASE WHEN NOT is_no_trade THEN traded_value_bn END) AS avg_value_bn,
               PERCENTILE_APPROX(CASE WHEN NOT is_no_trade THEN traded_value_bn END, 0.5) AS median_value_bn,
               MAX_BY(close, date) AS last_close_kvnd,
               SUM(CAST(is_abnormal_ret AS INT)) AS n_abnormal_ret,
               SUM(CAST(is_band_breach AS INT)) AS n_band_breach,
               SUM(CAST(is_no_trade AS INT)) AS n_no_trade,
               SUM(CAST(is_volume_spike AS INT)) AS n_volume_spike
        FROM silver
        GROUP BY ticker, company_name, exchange, sector_code, sector_name
    """).join(coverage.select("ticker", "n_expected"), "ticker", "left") \
        .withColumn("coverage_pct", F.col("n_sessions") / F.col("n_expected"))

    # (B) Chỉ số ngành bình quân đều (equal-weight) theo ngày
    sector_daily = spark.sql(f"""
        SELECT sector_code, sector_name, date,
               CASE WHEN COUNT(ret_clean) >= {MIN_SECTOR_CONSTITUENTS} THEN AVG(ret_clean) END AS sector_ret,
               COUNT(ret_clean) AS n_constituents,
               SUM(traded_value_bn) AS value_bn
        FROM silver GROUP BY sector_code, sector_name, date
    """).join(market.select("date", "mkt_ret", "mkt_close", "regime"), "date", "left")
    ws = Window.partitionBy("sector_code").orderBy("date")
    ws_cum = ws.rowsBetween(Window.unboundedPreceding, 0)
    sector_daily = (sector_daily
                    .withColumn("sector_index", 100 * F.exp(F.sum(F.log1p(F.coalesce("sector_ret", F.lit(0.0)))).over(ws_cum)))
                    .withColumn("drawdown", F.col("sector_index") / F.max("sector_index").over(ws_cum) - 1)
                    .withColumn("year", F.year("date")))
    gold["sector_daily"] = sector_daily
    sector_daily.createOrReplaceTempView("sector_daily")

    # (C) Chuỗi VN-Index chuẩn hóa để so sánh (benchmark)
    wm = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    market_daily = (spark.table("market")
                    .withColumn("vnindex_rebased", 100 * F.exp(F.sum(F.log1p(F.coalesce("mkt_ret", F.lit(0.0)))).over(wm)))
                    .withColumn("drawdown", F.col("vnindex_rebased") / F.max("vnindex_rebased").over(wm) - 1))
    gold["market_daily"] = market_daily
    market_daily.createOrReplaceTempView("market_daily")

    # (D) KPI ngành (Q1, Q2) + dòng benchmark VN-Index
    sector_kpi = spark.sql(f"""
        SELECT sector_code, sector_name,
               COUNT(sector_ret) AS n_days,
               POWER(EXP(SUM(LN(1 + sector_ret))), 365.25 / DATEDIFF(MAX(date), MIN(date))) - 1 AS cagr,
               STDDEV_SAMP(sector_ret) * SQRT({td}) AS ann_vol,
               (AVG(sector_ret) * {td} - {rf}) / (STDDEV_SAMP(sector_ret) * SQRT({td})) AS sharpe,
               (AVG(sector_ret) * {td} - {rf}) /
                   (SQRT(AVG(CASE WHEN sector_ret < 0 THEN sector_ret * sector_ret ELSE 0 END)) * SQRT({td})) AS sortino,
               MIN(drawdown) AS max_drawdown,
               COVAR_SAMP(sector_ret, mkt_ret) / VAR_SAMP(CASE WHEN sector_ret IS NOT NULL THEN mkt_ret END) AS beta,
               CORR(sector_ret, mkt_ret) AS corr_vnindex,
               AVG(CASE WHEN mkt_ret > 0 THEN sector_ret END) / AVG(CASE WHEN mkt_ret > 0 AND sector_ret IS NOT NULL THEN mkt_ret END) AS upside_capture,
               AVG(CASE WHEN mkt_ret < 0 THEN sector_ret END) / AVG(CASE WHEN mkt_ret < 0 AND sector_ret IS NOT NULL THEN mkt_ret END) AS downside_capture,
               AVG(CASE WHEN sector_ret > mkt_ret THEN 1.0 ELSE 0.0 END) AS pct_days_beat_vnindex,
               AVG(value_bn) AS avg_daily_value_bn,
               SUM(value_bn) AS total_value_bn
        FROM sector_daily GROUP BY sector_code, sector_name
    """)
    total_val = sector_kpi.agg(F.sum("total_value_bn")).first()[0] or 1.0
    sector_kpi = sector_kpi.withColumn("value_share", F.col("total_value_bn") / F.lit(total_val))
    bench = spark.sql(f"""
        SELECT 'VNINDEX' AS sector_code, 'VN-Index (benchmark)' AS sector_name, COUNT(mkt_ret) AS n_days,
               POWER(EXP(SUM(LN(1 + mkt_ret))), 365.25 / DATEDIFF(MAX(date), MIN(date))) - 1 AS cagr,
               STDDEV_SAMP(mkt_ret) * SQRT({td}) AS ann_vol,
               (AVG(mkt_ret) * {td} - {rf}) / (STDDEV_SAMP(mkt_ret) * SQRT({td})) AS sharpe,
               (AVG(mkt_ret) * {td} - {rf}) /
                   (SQRT(AVG(CASE WHEN mkt_ret < 0 THEN mkt_ret * mkt_ret ELSE 0 END)) * SQRT({td})) AS sortino,
               MIN(drawdown) AS max_drawdown, CAST(1 AS DOUBLE) AS beta, CAST(1 AS DOUBLE) AS corr_vnindex,
               CAST(1 AS DOUBLE) AS upside_capture, CAST(1 AS DOUBLE) AS downside_capture, CAST(NULL AS DOUBLE) AS pct_days_beat_vnindex,
               CAST(NULL AS DOUBLE) AS avg_daily_value_bn, CAST(NULL AS DOUBLE) AS total_value_bn,
               CAST(NULL AS DOUBLE) AS value_share
        FROM market_daily
    """)
    sector_kpi = sector_kpi.unionByName(bench)
    # xếp hạng theo Sharpe (window không partition -> dữ liệu 12 dòng nên an toàn)
    gold["sector_kpi"] = sector_kpi.withColumn(
        "rank_sharpe", F.when(F.col("sector_code") != "VNINDEX",
                              F.dense_rank().over(Window.orderBy(F.col("sharpe").desc()))))

    # (E) Lợi suất theo năm + thứ hạng ngành mỗi năm (Q1: tính ổn định)
    yearly = spark.sql(f"""
        WITH s AS (
            SELECT sector_code, sector_name, year,
                   EXP(SUM(LN(1 + sector_ret))) - 1 AS annual_return,
                   STDDEV_SAMP(sector_ret) * SQRT({td}) AS ann_vol, COUNT(sector_ret) AS n_days
            FROM sector_daily GROUP BY sector_code, sector_name, year),
        m AS (
            SELECT YEAR(date) AS year, EXP(SUM(LN(1 + mkt_ret))) - 1 AS vnindex_return
            FROM market GROUP BY YEAR(date))
        SELECT s.*, m.vnindex_return, s.annual_return - m.vnindex_return AS excess_vs_vnindex,
               RANK() OVER (PARTITION BY s.year ORDER BY s.annual_return DESC) AS rank_in_year,
               (s.year = {config.END_DATE.year} AND {int(config.END_DATE.month < 12)} = 1) OR s.n_days < 120 AS is_partial_year
        FROM s JOIN m ON s.year = m.year
    """)
    gold["sector_yearly"] = yearly

    # (F) Hiệu quả theo chế độ thị trường (Q2: ngành phòng thủ / tấn công)
    gold["sector_regime"] = spark.sql(f"""
        SELECT sector_code, sector_name, regime, COUNT(sector_ret) AS n_days,
               AVG(sector_ret) * {td} AS ann_mean_return,
               AVG(mkt_ret) * {td} AS vnindex_ann_mean_return,
               (AVG(sector_ret) - AVG(mkt_ret)) * {td} AS ann_excess_vs_vnindex,
               STDDEV_SAMP(sector_ret) * SQRT({td}) AS ann_vol,
               AVG(CASE WHEN sector_ret > 0 THEN 1.0 ELSE 0.0 END) AS pct_up_days
        FROM sector_daily
        WHERE regime IS NOT NULL AND sector_ret IS NOT NULL
        GROUP BY sector_code, sector_name, regime
    """)

    # (G) Dòng tiền: giá trị giao dịch theo quý và tỷ trọng ngành (Q3)
    liq = (silver.groupBy("sector_code", "sector_name", "quarter")
           .agg(F.sum("traded_value_bn").alias("value_bn"),
                F.countDistinct("date").alias("n_sessions"),
                F.countDistinct("ticker").alias("n_tickers")))
    wq = Window.partitionBy("quarter")
    wsq = Window.partitionBy("sector_code").orderBy("quarter")
    gold["liquidity_quarterly"] = (
        liq.withColumn("avg_daily_value_bn", F.col("value_bn") / F.col("n_sessions"))
        .withColumn("value_share", F.col("value_bn") / F.sum("value_bn").over(wq))
        .withColumn("share_change_qoq", F.col("value_share") - F.lag("value_share").over(wsq))
        .withColumn("rank_in_quarter", F.rank().over(wq.orderBy(F.col("value_bn").desc())))
    )

    # (H) Ma trận tương quan lợi suất ngày giữa các ngành — self-join theo ngày (Q4)
    a = sector_daily.select("date", F.col("sector_code").alias("sector_a"), F.col("sector_ret").alias("ra"))
    b = sector_daily.select("date", F.col("sector_code").alias("sector_b"), F.col("sector_ret").alias("rb"))
    gold["sector_corr"] = (a.join(b, "date").groupBy("sector_a", "sector_b")
                           .agg(F.corr("ra", "rb").alias("corr"), F.count(F.col("ra") * F.col("rb")).alias("n_days")))

    # (I) Lợi suất tháng từng mã (drill-down)
    gold["ticker_monthly"] = (silver.groupBy("ticker", "sector_code", "sector_name", "month")
                              .agg((F.exp(F.sum(F.log1p("ret_clean"))) - 1).alias("monthly_return"),
                                   F.sum("traded_value_bn").alias("value_bn"),
                                   F.max_by("close", "date").alias("close_kvnd"),
                                   F.count("*").alias("n_sessions")))

    # (J) Danh sách phiên bất thường để giải thích trong báo cáo (EDA)
    gold["abnormal_events"] = (silver.filter("is_abnormal_ret OR is_long_gap OR is_band_breach")
                               .select("ticker", "sector_name", "exchange", "date", "prev_date", "gap_days",
                                       "prev_close", "close", "ret_1d", "price_band", "is_band_breach",
                                       "is_abnormal_ret", "is_long_gap")
                               .orderBy(F.abs("ret_1d").desc()))
    return gold


# ===========================================================================
# GHI DỮ LIỆU
# ===========================================================================
def write_gold(gold: dict[str, DataFrame]) -> dict[str, int]:
    config.GOLD_CSV_DIR.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, df in gold.items():
        df = df.cache()
        df.coalesce(1).write.mode("overwrite").parquet(str(config.GOLD_PARQUET_DIR / name))
        # Bảng Gold đã tổng hợp (nhỏ) -> xuất CSV 1 file có BOM để Power BI/Excel đọc tiếng Việt
        pdf = df.toPandas()
        pdf.to_csv(config.GOLD_CSV_DIR / f"{name}.csv", index=False, encoding="utf-8-sig")
        counts[name] = len(pdf)
        df.unpersist()
    return counts


def main() -> int:
    t0 = time.perf_counter()
    if not any(config.RAW_STOCK_DIR.rglob("*.json")):
        log(f"Không tìm thấy dữ liệu thô trong {config.RAW_STOCK_DIR}. Chạy trước: python src/fetch_dnse.py")
        return 1
    spark = build_spark()
    METRICS.update(spark_version=spark.version, period=f"{config.START_DATE}..{config.END_DATE}",
                   rf_annual=config.RF_ANNUAL)
    log(f"SparkSession {spark.version} | master={spark.sparkContext.master}")

    # 1. Đọc dữ liệu đa nguồn
    universe = read_universe(spark)
    stock_bars = explode_bars(read_raw_json(spark, config.RAW_STOCK_DIR))
    index_bars = explode_bars(read_raw_json(spark, config.RAW_INDEX_DIR))
    market = build_market(index_bars).cache()
    stage("1b_market_index", rows=market.count())
    log("Schema bronze sau explode:")
    stock_bars.printSchema()

    # 2–3. Chất lượng + làm sạch
    clean, dq, coverage = quality_and_clean(stock_bars, universe, market)

    # 4–5. Tích hợp & đặc trưng
    panel = add_features(clean, market)

    # 6. Silver: Parquet phân vùng theo ngành, năm
    cols = ["date", "year", "month", "quarter", "ticker", "company_name", "exchange", "icb2_code",
            "sector_code", "sector_name", "open", "high", "low", "close", "volume", "traded_value_bn",
            "prev_close", "prev_date", "gap_days", "ret_1d", "ret_clean", "price_band",
            "is_band_breach", "is_abnormal_ret", "is_long_gap", "is_no_trade", "volume_missing", "volume_z60", "is_volume_spike",
            "wealth", "drawdown", "vol_20d", "trend_vs_ma200", "beta_250d", "excess_ret",
            "mkt_close", "mkt_ret", "regime", "vn30_close", "vn30_ret"]
    (panel.select(*cols).repartition("sector_code", "year")
     .write.mode("overwrite").partitionBy("sector_code", "year").parquet(str(config.SILVER_PANEL)))
    silver = spark.read.parquet(str(config.SILVER_PANEL)).cache()
    n_silver = silver.count()
    stage("3_silver_panel", rows=n_silver, tickers=silver.select("ticker").distinct().count(),
          sectors=silver.select("sector_code").distinct().count(),
          rows_valid_return=silver.filter(F.col("ret_clean").isNotNull()).count())

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 7. Gold (+ ghi physical plan để minh họa broadcast join / window / exchange trong báo cáo)
    gold = build_gold(spark, silver, market, coverage)
    try:
        plan_txt = gold["sector_kpi"]._jdf.queryExecution().toString()
        (config.REPORTS_DIR / "spark_plan_sector_kpi.txt").write_text(plan_txt, encoding="utf-8")
    except Exception:  # noqa: BLE001 — chỉ là tài liệu minh họa, không ảnh hưởng kết quả
        pass
    counts = write_gold(gold)
    dq_df = spark.createDataFrame(dq)
    dq_df.toPandas().to_csv(config.GOLD_CSV_DIR / "data_quality_report.csv", index=False, encoding="utf-8-sig")
    counts["data_quality_report"] = len(dq)
    stage("4_gold_tables", **counts)

    METRICS["data_quality"] = dq
    METRICS["elapsed_seconds"] = round(time.perf_counter() - t0, 1)
    (config.REPORTS_DIR / "pipeline_metrics.json").write_text(
        json.dumps(METRICS, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log(f"Hoàn tất trong {METRICS['elapsed_seconds']}s. Silver: {config.SILVER_PANEL} | Gold: {config.GOLD_DIR}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
