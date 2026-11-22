"""
BƯỚC 4 — ỨNG DỤNG STREAMLIT: trình diễn kết quả phân tích 11 ngành
===================================================================
App chỉ ĐỌC các bảng Gold do pipeline PySpark xuất ra (data/gold/csv) — không chạy Spark khi demo.
Chạy:  streamlit run app/streamlit_app.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import config  # noqa: E402
import insights  # noqa: E402

st.set_page_config(page_title="Phân tích 11 ngành TTCK Việt Nam", page_icon="📊", layout="wide")

# Màu theo vai trò (bảng màu tham chiếu đã kiểm định mù màu)
BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#0b0b0b", "#b9b8b2", "#ecebe7"
DIVERGING = [[0, "#d03b3b"], [0.5, "#f0efec"], [1, "#2a78d6"]]     # đỏ (âm) – xám – xanh (dương)
SEQUENTIAL = ["#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#104281"]


def style(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(template="plotly_white", height=height, margin=dict(l=10, r=10, t=40, b=10),
                      font=dict(size=13), hoverlabel=dict(bgcolor="white"),
                      legend=dict(orientation="h", y=1.08, x=0))
    fig.update_xaxes(gridcolor=GRID, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


@st.cache_data(show_spinner="Đang đọc bảng Gold…")
def load() -> dict[str, pd.DataFrame]:
    return insights.load_gold(config.GOLD_CSV_DIR)


g = load()
if "sector_kpi" not in g:
    st.error(f"Chưa có dữ liệu Gold tại `{config.GOLD_CSV_DIR}`. Chạy lần lượt:\n\n"
             "`python src/fetch_dnse.py` → `python src/spark_pipeline.py`")
    st.stop()

metrics_path = config.REPORTS_DIR / "pipeline_metrics.json"
metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.exists() else {}
sd, md, tk = g["sector_daily"], g["market_daily"], g["ticker_kpi"]
SECTORS = (g["sector_kpi"].query("sector_code != 'VNINDEX'").sort_values("sector_name").sector_name.tolist())

# ---------------------------------------------------------------- Sidebar (bộ lọc)
st.sidebar.header("Bộ lọc")
dmin, dmax = sd.date.min().date(), sd.date.max().date()
d0, d1 = st.sidebar.slider("Giai đoạn phân tích", min_value=dmin, max_value=dmax, value=(dmin, dmax),
                           format="MM/YYYY")
sel_sectors = st.sidebar.multiselect("Ngành (ICB cấp 2)", SECTORS, default=SECTORS)
highlight = st.sidebar.selectbox("Ngành làm nổi bật trên biểu đồ", SECTORS,
                                 index=SECTORS.index(sel_sectors[0]) if sel_sectors else 0)
st.sidebar.caption(f"Lãi suất phi rủi ro giả định: {config.RF_ANNUAL:.0%}/năm. "
                   "Lợi suất ngành = bình quân đều các mã trong ngành.")
if not sel_sectors:
    st.warning("Chọn ít nhất 1 ngành.")
    st.stop()


def kpi_in_range(daily: pd.DataFrame, ret_col: str) -> dict:
    """Tính lại KPI trong giai đoạn người dùng chọn (lớp hiển thị, dữ liệu đã tổng hợp từ Spark)."""
    r = daily[ret_col].dropna()
    if len(r) < 20:
        return dict(cagr=np.nan, vol=np.nan, sharpe=np.nan, mdd=np.nan)
    w = (1 + r).cumprod()
    years = (daily.date.max() - daily.date.min()).days / 365.25      # CAGR theo năm lịch (như pipeline Spark)
    return dict(cagr=w.iloc[-1] ** (1 / years) - 1,
                vol=r.std() * np.sqrt(config.TRADING_DAYS),
                sharpe=(r.mean() * config.TRADING_DAYS - config.RF_ANNUAL) / (r.std() * np.sqrt(config.TRADING_DAYS)),
                mdd=(w / w.cummax() - 1).min())


mask = (sd.date.dt.date >= d0) & (sd.date.dt.date <= d1)
sd_r = sd[mask & sd.sector_name.isin(sel_sectors)]
md_r = md[(md.date.dt.date >= d0) & (md.date.dt.date <= d1)]
if sd_r.empty:
    st.warning("Không có dữ liệu giao dịch trong giai đoạn đã chọn. Vui lòng mở rộng khoảng thời gian.")
    st.stop()
rows = []
for name, part in sd_r.groupby("sector_name"):
    k = kpi_in_range(part, "sector_ret")
    rows.append(dict(sector_name=name, **k))
kr = pd.DataFrame(rows).sort_values("sharpe", ascending=False)
km = kpi_in_range(md_r, "mkt_ret")

st.title("📊 Hiệu quả, rủi ro & dòng tiền của 11 ngành trên TTCK Việt Nam")
st.caption(f"Nguồn: API DNSE (OHLCV ngày) · {tk.ticker.nunique()} mã · {len(SECTORS)} ngành ICB cấp 2 · "
           f"giai đoạn đang xem {d0:%d/%m/%Y} – {d1:%d/%m/%Y} · pipeline PySpark {metrics.get('spark_version', '')}")

tabs = st.tabs(["Tổng quan", "Q1 · Hiệu quả ngành", "Q2 · Phòng thủ / tấn công", "Q3 · Dòng tiền",
                "Q4 · Đa dạng hóa", "Q5 · Tra cứu mã", "Dữ liệu & pipeline"])
INS = insights.generate(g)


def insight_box(q: str) -> None:
    with st.container(border=True):
        st.markdown(f"**{q}. {insights.QUESTIONS[q]}**")
        st.caption("Insight cả giai đoạn (tính tự động từ bảng Gold):")
        for s in INS[q]:
            st.markdown(f"- {s}")


# ---------------------------------------------------------------- Tổng quan
with tabs[0]:
    c = st.columns(5)
    stg = metrics.get("stages", {})
    c[0].metric("Số dòng giao dịch sạch", f"{stg.get('3_silver_panel', {}).get('rows', len(tk)):,}".replace(",", "."))
    c[1].metric("Số mã / ngành", f"{tk.ticker.nunique()} / {len(SECTORS)}")
    c[2].metric("CAGR VN-Index", insights.pct(km["cagr"]))
    best = kr.iloc[0]
    c[3].metric("Ngành Sharpe cao nhất", best.sector_name, f"Sharpe {insights.num(best.sharpe)}", delta_color="off")
    c[4].metric("Số ngành có CAGR > VN-Index", f"{int((kr.cagr > km['cagr']).sum())}/{len(kr)}")

    st.subheader("Chỉ số ngành (bình quân đều, gốc = 100 tại đầu giai đoạn)")
    fig = go.Figure()
    for name, part in sd_r.groupby("sector_name"):
        idx = 100 * (1 + part.sector_ret.fillna(0)).cumprod()
        is_h = name == highlight
        fig.add_scatter(x=part.date, y=idx, name=name, mode="lines",
                        line=dict(color=BLUE if is_h else MUTED, width=2.5 if is_h else 1),
                        opacity=1 if is_h else 0.6, showlegend=is_h,
                        hovertemplate=f"{name}<br>%{{x|%d/%m/%Y}}: %{{y:.1f}}<extra></extra>")
    fig.add_scatter(x=md_r.date, y=100 * (1 + md_r.mkt_ret.fillna(0)).cumprod(), name="VN-Index",
                    line=dict(color=INK, width=2, dash="dash"),
                    hovertemplate="VN-Index<br>%{x|%d/%m/%Y}: %{y:.1f}<extra></extra>")
    fig.update_layout(hovermode="closest", yaxis_title="Chỉ số (gốc 100)")
    st.plotly_chart(style(fig, 430), width="stretch")
    st.caption(f"Đường xanh: {highlight} (đổi ở thanh bên). Đường xám: các ngành còn lại. Nét đứt: VN-Index.")

    st.subheader("Khuyến nghị rút ra từ dữ liệu")
    for s in insights.recommendations(g):
        st.markdown(f"- {s}")

# ---------------------------------------------------------------- Q1
with tabs[1]:
    insight_box("Q1")
    a, b = st.columns(2)
    bars = pd.concat([kr, pd.DataFrame([dict(sector_name="VN-Index", **km)])])
    for col, (metric, title, fmt) in zip((a, b), [("cagr", "CAGR trong giai đoạn chọn", ".1%"),
                                                    ("sharpe", "Sharpe trong giai đoạn chọn", ".2f")]):
        d = bars.sort_values(metric)
        fig = go.Figure(go.Bar(x=d[metric], y=d.sector_name, orientation="h",
                               marker_color=[INK if n == "VN-Index" else (BLUE if n == highlight else "#86b6ef")
                                             for n in d.sector_name],
                               text=d[metric], texttemplate=f"%{{text:{fmt}}}", textposition="outside",
                               hovertemplate="%{y}: %{x:" + fmt + "}<extra></extra>"))
        fig.update_layout(title=title, xaxis_tickformat=fmt)
        col.plotly_chart(style(fig, 460), width="stretch")

    st.subheader("Lợi suất từng năm theo ngành (xanh: tăng, đỏ: giảm)")
    y = g["sector_yearly"][g["sector_yearly"].sector_name.isin(sel_sectors)]
    piv = y.pivot(index="sector_name", columns="year", values="annual_return")
    piv.columns = [f"{c}{' (YTD)' if y[y.year == c].is_partial_year.any() else ''}" for c in piv.columns]
    lim = float(np.nanmax(np.abs(piv.values)))
    fig = px.imshow(piv, color_continuous_scale=DIVERGING, zmin=-lim, zmax=lim, aspect="auto",
                    text_auto=".0%", labels=dict(color="Lợi suất"))
    fig.update_traces(hovertemplate="%{y} · %{x}: %{z:.1%}<extra></extra>")
    st.plotly_chart(style(fig, 460), width="stretch")
    with st.expander("Bảng KPI ngành (cả giai đoạn, từ Spark)"):
        st.dataframe(g["sector_kpi"].drop(columns=["sector_code"]).round(4), width="stretch", hide_index=True)

# ---------------------------------------------------------------- Q2
with tabs[2]:
    insight_box("Q2")
    k = g["sector_kpi"].query("sector_code != 'VNINDEX'")
    k = k[k.sector_name.isin(sel_sectors)]
    a, b = st.columns(2)
    fig = go.Figure(go.Scatter(x=k.downside_capture, y=k.upside_capture, mode="markers+text", text=k.sector_name,
                               textposition="top center", marker=dict(size=12, color=BLUE, line=dict(color="white", width=2)),
                               customdata=k[["beta"]],
                               hovertemplate="%{text}<br>Downside capture %{x:.2f}<br>Upside capture %{y:.2f}"
                                             "<br>Beta %{customdata[0]:.2f}<extra></extra>"))
    fig.add_vline(x=1, line=dict(color=MUTED, dash="dot")); fig.add_hline(y=1, line=dict(color=MUTED, dash="dot"))
    fig.update_layout(title="Upside vs downside capture so với VN-Index",
                      xaxis_title="Downside capture (< 1: giảm ít hơn thị trường)",
                      yaxis_title="Upside capture (> 1: tăng mạnh hơn thị trường)")
    a.plotly_chart(style(fig, 470), width="stretch")

    reg = g["sector_regime"][g["sector_regime"].sector_name.isin(sel_sectors)]
    order = reg[reg.regime.str.startswith("Giảm")].sort_values("ann_excess_vs_vnindex").sector_name
    fig = go.Figure()
    for regime, color in [("Tăng (trên MA200)", BLUE), ("Giảm (dưới MA200)", ORANGE)]:
        r = reg[reg.regime == regime].set_index("sector_name").reindex(order)
        fig.add_bar(y=r.index, x=r.ann_excess_vs_vnindex, name=f"VN-Index {regime.lower()}", orientation="h",
                    marker_color=color, hovertemplate="%{y}: %{x:+.1%}/năm<extra>" + regime + "</extra>")
    fig.update_layout(title="Lợi suất vượt VN-Index (năm hóa) theo chế độ thị trường", barmode="group",
                      xaxis_tickformat="+.0%", bargap=0.25)
    b.plotly_chart(style(fig, 470), width="stretch")
    st.caption("Chế độ thị trường: VN-Index đóng cửa trên/dưới đường trung bình 200 phiên (tính trong Spark).")

# ---------------------------------------------------------------- Q3
with tabs[3]:
    insight_box("Q3")
    liq = g["liquidity_quarterly"]
    liq = liq[liq.sector_name.isin(sel_sectors)]
    piv = liq.pivot(index="sector_name", columns="quarter", values="value_share")
    fig = px.imshow(piv, color_continuous_scale=SEQUENTIAL, aspect="auto", labels=dict(color="Tỷ trọng"))
    fig.update_traces(hovertemplate="%{y} · %{x}: %{z:.1%} giá trị giao dịch<extra></extra>")
    fig.update_layout(title="Tỷ trọng giá trị giao dịch theo quý (đậm = hút nhiều tiền)", coloraxis_colorbar_tickformat=".0%")
    st.plotly_chart(style(fig, 460), width="stretch")
    tot = liq.groupby("quarter").avg_daily_value_bn.sum().reset_index()
    fig = go.Figure(go.Bar(x=tot.quarter, y=tot.avg_daily_value_bn, marker_color=BLUE,
                           hovertemplate="%{x}: %{y:,.0f} tỷ đồng/phiên<extra></extra>"))
    fig.update_layout(title="Giá trị giao dịch bình quân/phiên của các ngành đã chọn (tỷ đồng)")
    st.plotly_chart(style(fig, 330), width="stretch")

# ---------------------------------------------------------------- Q4
with tabs[4]:
    insight_box("Q4")
    c = g["sector_corr"]
    names = dict(zip(g["sector_kpi"].sector_code, g["sector_kpi"].sector_name))
    c = c.assign(a=c.sector_a.map(names), b=c.sector_b.map(names))
    c = c[c.a.isin(sel_sectors) & c.b.isin(sel_sectors)]
    piv = c.pivot(index="a", columns="b", values="corr")
    fig = px.imshow(piv, color_continuous_scale=SEQUENTIAL, zmin=max(0, float(np.nanmin(piv.values))), zmax=1,
                    text_auto=".2f", aspect="auto", labels=dict(color="Tương quan"))
    fig.update_traces(hovertemplate="%{y} × %{x}: %{z:.2f}<extra></extra>")
    fig.update_layout(title="Ma trận tương quan lợi suất ngày giữa các ngành", xaxis_title="", yaxis_title="")
    st.plotly_chart(style(fig, 560), width="stretch")

# ---------------------------------------------------------------- Q5
with tabs[5]:
    insight_box("Q5")
    t = tk[tk.sector_name.isin(sel_sectors)].copy()
    fig = go.Figure()
    for is_h, part in t.groupby(t.sector_name == highlight):
        fig.add_scatter(x=part.ann_vol, y=part.cagr, mode="markers+text" if is_h else "markers",
                        text=part.ticker, textposition="top center", name=highlight if is_h else "Ngành khác",
                        marker=dict(size=11 if is_h else 8, color=BLUE if is_h else MUTED,
                                    line=dict(color="white", width=2)),
                        customdata=part[["sector_name", "sharpe", "max_drawdown"]],
                        hovertemplate="%{text} · %{customdata[0]}<br>CAGR %{y:.1%} · biến động %{x:.1%}"
                                      "<br>Sharpe %{customdata[1]:.2f} · MaxDD %{customdata[2]:.1%}<extra></extra>")
    fig.update_layout(title="Rủi ro – lợi suất từng mã (cả giai đoạn)", xaxis_title="Biến động năm hóa",
                      yaxis_title="CAGR", xaxis_tickformat=".0%", yaxis_tickformat=".0%")
    st.plotly_chart(style(fig, 470), width="stretch")

    tick = st.selectbox("Chọn mã để xem chi tiết", sorted(t.ticker))
    row = t[t.ticker == tick].iloc[0]
    cc = st.columns(5)
    cc[0].metric("CAGR", insights.pct(row.cagr)); cc[1].metric("Biến động", insights.pct(row.ann_vol))
    cc[2].metric("Sharpe", insights.num(row.sharpe)); cc[3].metric("Max drawdown", insights.pct(row.max_drawdown))
    cc[4].metric("Beta", insights.num(row.beta))
    m = g["ticker_monthly"].query("ticker == @tick").sort_values("month")
    m = m[(m.month.dt.date >= d0) & (m.month.dt.date <= d1)]
    fig = go.Figure(go.Bar(x=m.month, y=m.monthly_return,
                           marker_color=np.where(m.monthly_return >= 0, BLUE, "#d03b3b"),
                           hovertemplate="%{x|%m/%Y}: %{y:+.1%}<extra></extra>"))
    fig.update_layout(title=f"Lợi suất theo tháng — {tick} ({row.company_name})", yaxis_tickformat=".0%")
    st.plotly_chart(style(fig, 320), width="stretch")
    t_display = t.sort_values("sharpe", ascending=False)[
        ["ticker", "company_name", "sector_name", "exchange", "first_date", "n_sessions", "cagr", "ann_vol", "sharpe",
         "max_drawdown", "beta", "avg_value_bn", "coverage_pct", "n_abnormal_ret"]].copy()
    t_display["first_date"] = t_display["first_date"].dt.strftime("%Y-%m-%d")
    num_cols = t_display.select_dtypes(include="number").columns
    t_display[num_cols] = t_display[num_cols].round(4)
    st.dataframe(t_display, width="stretch", hide_index=True)

# ---------------------------------------------------------------- Dữ liệu & pipeline
with tabs[6]:
    st.subheader("Luồng dữ liệu")
    st.code("API DNSE (JSON/mã/năm) + universe.csv (phân ngành) + chỉ số VNINDEX/VN30\n"
            "   → PySpark: schema → DQ-01..12 → làm sạch → broadcast join + join theo ngày → Window\n"
            "   → Silver Parquet partitionBy(sector_code, year)\n"
            "   → Gold (Spark SQL / groupBy / self-join): 10 bảng KPI Parquet + CSV\n"
            "   → Dashboard BI  +  Streamlit (app này)  →  Insight / khuyến nghị", language="text")
    if metrics:
        st.markdown("**Số dòng qua từng bước pipeline**")
        st.dataframe(pd.DataFrame([{"Bước": k, **v} for k, v in metrics["stages"].items() if k != "4_gold_tables"]),
                     width="stretch", hide_index=True)
    st.markdown("**Báo cáo chất lượng dữ liệu**")
    st.dataframe(g["data_quality_report"], width="stretch", hide_index=True)
    st.markdown("**Các phiên biến động bất thường bị loại khỏi thống kê lợi suất** (cần giải thích trong báo cáo)")
    st.dataframe(g["abnormal_events"].head(200), width="stretch", hide_index=True)
