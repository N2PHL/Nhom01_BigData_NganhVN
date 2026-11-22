"""
Sinh insight dạng văn bản TỪ SỐ LIỆU GOLD (không viết tay con số) cho 5 câu hỏi phân tích.
Dùng chung cho: Streamlit app, dashboard HTML và file reports/insights_auto.md.

Câu hỏi phân tích (Chương 1 báo cáo):
  Q1. Ngành nào có hiệu quả điều chỉnh rủi ro tốt nhất so với VN-Index và thứ hạng có ổn định qua các năm?
  Q2. Ngành nào phòng thủ / tấn công: beta, upside/downside capture, hiệu quả khi VN-Index dưới MA200?
  Q3. Dòng tiền (giá trị giao dịch) dịch chuyển giữa các ngành thế nào; ngành nào đang hút / thoát tiền?
  Q4. Tương quan giữa các ngành — cặp ngành nào giúp đa dạng hóa tốt nhất?
  Q5. Trong từng ngành, mã nào dẫn dắt và mức độ phân hóa nội ngành?
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

QUESTIONS = {
    "Q1": "Ngành nào có lợi suất điều chỉnh rủi ro tốt nhất so với VN-Index, và thứ hạng có ổn định qua từng năm?",
    "Q2": "Ngành nào phòng thủ, ngành nào tấn công (beta, upside/downside capture, hiệu quả khi VN-Index dưới MA200)?",
    "Q3": "Dòng tiền (giá trị giao dịch) dịch chuyển giữa các ngành ra sao; ngành nào đang hút hoặc thoát tiền?",
    "Q4": "Các ngành tương quan với nhau đến đâu; cặp ngành nào giúp đa dạng hóa danh mục tốt nhất?",
    "Q5": "Trong từng ngành, mã nào dẫn dắt và mức độ phân hóa giữa các mã cùng ngành lớn đến đâu?",
}

GOLD_TABLES = ["ticker_kpi", "sector_kpi", "sector_daily", "market_daily", "sector_yearly", "sector_regime",
               "liquidity_quarterly", "sector_corr", "ticker_monthly", "abnormal_events", "data_quality_report"]


def load_gold(csv_dir: Path) -> dict[str, pd.DataFrame]:
    out = {}
    for name in GOLD_TABLES:
        p = Path(csv_dir) / f"{name}.csv"
        if p.exists():
            df = pd.read_csv(p, encoding="utf-8-sig")
            for c in ("date", "month", "first_date", "last_date", "prev_date"):
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c])
            out[name] = df
    return out


def pct(x: float, d: int = 1) -> str:
    return "n/a" if pd.isna(x) else f"{x * 100:.{d}f}%".replace(".", ",")


def pp(x: float) -> str:
    """Chênh lệch tỷ trọng theo điểm phần trăm, có dấu."""
    return "n/a" if pd.isna(x) else f"{x * 100:+.1f} điểm %".replace(".", ",")


def num(x: float, d: int = 2) -> str:
    return "n/a" if pd.isna(x) else f"{x:.{d}f}".replace(".", ",")


def generate(g: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    res: dict[str, list[str]] = {}
    kpi = g["sector_kpi"]
    bench = kpi[kpi.sector_code == "VNINDEX"].iloc[0]
    sec = kpi[kpi.sector_code != "VNINDEX"].sort_values("sharpe", ascending=False)

    # ---------------- Q1 ----------------
    top, bot = sec.iloc[0], sec.iloc[-1]
    beat = sec[sec.cagr > bench.cagr]
    yearly = g["sector_yearly"]
    full = yearly[~yearly.is_partial_year.astype(bool)]
    top3_freq = (full[full.rank_in_year <= 3].groupby("sector_name").size()
                 .reindex(sec.sector_name).fillna(0).astype(int))
    n_years = full.year.nunique()
    rank_sd = full.groupby("sector_name").rank_in_year.std().sort_values()
    res["Q1"] = [
        f"VN-Index cả giai đoạn: CAGR {pct(bench.cagr)}, biến động {pct(bench.ann_vol)}/năm, Sharpe {num(bench.sharpe)}, "
        f"sụt giảm tối đa {pct(bench.max_drawdown)}.",
        f"Dẫn đầu theo Sharpe là **{top.sector_name}** (CAGR {pct(top.cagr)}, Sharpe {num(top.sharpe)}, MaxDD {pct(top.max_drawdown)}); "
        f"thấp nhất là **{bot.sector_name}** (CAGR {pct(bot.cagr)}, Sharpe {num(bot.sharpe)}).",
        f"{len(beat)}/{len(sec)} ngành (rổ bình quân đều) có CAGR cao hơn VN-Index"
        + (f": {', '.join(beat.sector_name)}." if len(beat) else "."),
        f"Tính ổn định ({n_years} năm đủ dữ liệu): ngành lọt top 3 nhiều năm nhất là **{top3_freq.idxmax()}** "
        f"({top3_freq.max()}/{n_years} năm); thứ hạng biến động ít nhất là **{rank_sd.index[0]}**, "
        f"nhiều nhất là **{rank_sd.index[-1]}** → hiệu quả ngành có tính chu kỳ, không nên ngoại suy 1–2 năm gần nhất.",
    ]

    # ---------------- Q2 ----------------
    by_beta = sec.sort_values("beta")
    reg = g["sector_regime"]
    bear = reg[reg.regime.str.startswith("Giảm")].sort_values("ann_excess_vs_vnindex", ascending=False)
    bull = reg[reg.regime.str.startswith("Tăng")].sort_values("ann_excess_vs_vnindex", ascending=False)
    defensive = sec[(sec.downside_capture < 1) & (sec.beta < 1)]
    res["Q2"] = [
        f"Beta thấp nhất: **{by_beta.iloc[0].sector_name}** ({num(by_beta.iloc[0].beta)}); cao nhất: "
        f"**{by_beta.iloc[-1].sector_name}** ({num(by_beta.iloc[-1].beta)}).",
        "Ngành có đặc tính phòng thủ (beta < 1 và downside capture < 1): "
        + (", ".join(f"{r.sector_name} (down {num(r.downside_capture)})" for r in defensive.itertuples())
           if len(defensive) else "không có ngành nào thỏa cả hai điều kiện") + ".",
        (f"Khi VN-Index dưới MA200, ngành giữ giá tốt nhất so với thị trường: **{bear.iloc[0].sector_name}** "
         f"(vượt {pct(bear.iloc[0].ann_excess_vs_vnindex)}/năm); kém nhất: **{bear.iloc[-1].sector_name}** "
         f"({pct(bear.iloc[-1].ann_excess_vs_vnindex)}/năm).") if len(bear) else "Chưa đủ dữ liệu chế độ Giảm.",
        (f"Khi VN-Index trên MA200, ngành vượt trội nhất: **{bull.iloc[0].sector_name}** "
         f"(vượt {pct(bull.iloc[0].ann_excess_vs_vnindex)}/năm).") if len(bull) else "Chưa đủ dữ liệu chế độ Tăng.",
    ]

    # ---------------- Q3 ----------------
    liq = g["liquidity_quarterly"].sort_values("quarter")
    qs = sorted(liq.quarter.unique())
    last_q, first_q = qs[-1], qs[0]
    last4 = qs[-4:]
    prev4 = qs[-8:-4] if len(qs) >= 8 else qs[:4]
    s_last = liq[liq.quarter.isin(last4)].groupby("sector_name").value_bn.sum()
    s_prev = liq[liq.quarter.isin(prev4)].groupby("sector_name").value_bn.sum()
    share_last, share_prev = s_last / s_last.sum(), s_prev / s_prev.sum()
    delta = (share_last - share_prev).sort_values()
    lead = liq[liq.quarter == last_q].sort_values("value_bn", ascending=False).iloc[0]
    tot_first = liq[liq.quarter == first_q].avg_daily_value_bn.sum()
    tot_last = liq[liq.quarter == last_q].avg_daily_value_bn.sum()
    res["Q3"] = [
        f"Giá trị giao dịch bình quân/phiên của {g['ticker_kpi'].ticker.nunique()} mã thay đổi từ {num(tot_first, 0)} tỷ đồng ({first_q}) lên "
        f"{num(tot_last, 0)} tỷ đồng ({last_q}).",
        f"Quý gần nhất ({last_q}) ngành chiếm tỷ trọng thanh khoản lớn nhất: **{lead.sector_name}** ({pct(lead.value_share)}).",
        f"So 4 quý gần nhất với 4 quý trước đó: hút tiền mạnh nhất **{delta.index[-1]}** "
        f"({pp(delta.iloc[-1])} tỷ trọng), thoát tiền mạnh nhất **{delta.index[0]}** "
        f"({pp(delta.iloc[0])}).",
    ]

    # ---------------- Q4 ----------------
    corr = g["sector_corr"]
    names = dict(zip(sec.sector_code, sec.sector_name))
    off = corr[corr.sector_a < corr.sector_b].copy()
    off["pair"] = off.sector_a.map(names) + " – " + off.sector_b.map(names)
    off = off.sort_values("corr")
    avg_corr = off["corr"].mean()
    res["Q4"] = [
        f"Tương quan lợi suất ngày trung bình giữa các cặp ngành: {num(avg_corr)} → các ngành cùng chịu nhân tố thị trường chung, "
        "đa dạng hóa chỉ theo ngành có tác dụng giới hạn.",
        f"Cặp ít tương quan nhất: **{off.iloc[0].pair}** ({num(off.iloc[0]['corr'])}); "
        f"{off.iloc[1].pair} ({num(off.iloc[1]['corr'])}).",
        f"Cặp tương quan cao nhất: **{off.iloc[-1].pair}** ({num(off.iloc[-1]['corr'])}) → nắm cả hai gần như không giảm rủi ro.",
    ]

    # ---------------- Q5 ----------------
    tk = g["ticker_kpi"]
    leaders = tk.sort_values("sharpe", ascending=False).groupby("sector_name").head(1).sort_values("sharpe", ascending=False)
    disp = tk.groupby("sector_name").cagr.agg(lambda s: s.max() - s.min()).sort_values()
    res["Q5"] = [
        "Mã dẫn dắt theo Sharpe ở mỗi ngành: " + "; ".join(
            f"{r.sector_name}: **{r.ticker}** ({num(r.sharpe)})" for r in leaders.head(11).itertuples()) + ".",
        f"Phân hóa nội ngành (chênh lệch CAGR mã tốt nhất – kém nhất) lớn nhất ở **{disp.index[-1]}** "
        f"({pct(disp.iloc[-1])}), nhỏ nhất ở **{disp.index[0]}** ({pct(disp.iloc[0])}) → ngành phân hóa mạnh "
        "cần chọn mã; ngành đồng pha có thể tiếp cận theo rổ/ETF ngành.",
    ]
    return res


def recommendations(g: dict[str, pd.DataFrame]) -> list[str]:
    kpi = g["sector_kpi"]
    sec = kpi[kpi.sector_code != "VNINDEX"]
    core = sec[(sec.sharpe >= sec.sharpe.median()) & (sec.downside_capture <= sec.downside_capture.median())]
    reg = g["sector_regime"]
    bull = reg[reg.regime.str.startswith("Tăng")].set_index("sector_name").ann_excess_vs_vnindex
    bear = reg[reg.regime.str.startswith("Giảm")].set_index("sector_name").ann_excess_vs_vnindex
    rec = []
    if len(core):
        rec.append("Danh mục lõi dài hạn có thể ưu tiên các ngành vừa có Sharpe trên trung vị vừa có downside capture dưới trung vị: "
                   + ", ".join(core.sector_name) + ".")
    # Phân loại theo hiệu quả thực tế ở từng chế độ thị trường (không chỉ theo beta)
    cyc = [n for n in bull.index if bull[n] > 0 and bear.get(n, 0) < 0]
    shelter = [n for n in bear.sort_values(ascending=False).index if bear[n] > 0 and bull.get(n, 0) < 0]
    both = [n for n in bull.index if bull[n] > 0.1 and bear.get(n, 0) > 0.1]
    if cyc:
        rec.append("Chỉ nên tăng tỷ trọng khi VN-Index trên MA200, giảm khi VN-Index dưới MA200 (vượt thị trường lúc tăng, "
                   "kém thị trường lúc giảm): " + ", ".join(cyc) + ".")
    if shelter:
        rec.append("Nơi trú ẩn khi VN-Index dưới MA200 (vượt thị trường lúc giảm, kém lúc tăng): " + ", ".join(shelter) + ".")
    if both:
        rec.append("Vượt VN-Index hơn 10%/năm ở cả hai chế độ: " + ", ".join(both) + ".")
    rec.append("Do tương quan giữa các ngành cao, cần kết hợp thêm quản trị tỷ trọng tiền mặt theo chế độ thị trường thay vì chỉ "
               "đa dạng hóa theo ngành.")
    rec.append("Đây là phân tích dữ liệu lịch sử cho mục đích học thuật, chưa tính chi phí giao dịch/thuế và không phải khuyến nghị mua bán.")
    return rec


def to_markdown(g: dict[str, pd.DataFrame]) -> str:
    ins = generate(g)
    lines = ["# Insight tự động từ bảng Gold (sinh bởi src/insights.py)", ""]
    for q, text in QUESTIONS.items():
        lines += [f"## {q}. {text}", ""] + [f"- {s}" for s in ins[q]] + [""]
    lines += ["## Khuyến nghị dựa trên dữ liệu", ""] + [f"- {s}" for s in recommendations(g)]
    return "\n".join(lines) + "\n"
