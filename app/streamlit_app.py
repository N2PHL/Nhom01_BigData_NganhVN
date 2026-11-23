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
has_lw = "sector_ret_lw" in sd.columns
weight_mode = st.sidebar.radio(
    "Phương pháp tính chỉ số ngành",
    ["Bình quân đều (Equal-Weight)", "Gia quyền thanh khoản (Liquidity-Weighted)"] if has_lw else ["Bình quân đều (Equal-Weight)"],
    index=0,
    help="Gia quyền thanh khoản dùng GTGD trung bình 20 phiên TRƯỚC ĐÓ (tránh look-ahead) để kiểm chứng Size Effect so với VN-Index.",
)
RET_COL = "sector_ret_lw" if (has_lw and "Gia quyền" in weight_mode) else "sector_ret"
st.sidebar.caption(f"Lãi suất phi rủi ro giả định: {config.RF_ANNUAL:.0%}/năm. "
                   f"Đang dùng: {weight_mode}.")
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
    k = kpi_in_range(part, RET_COL)
    rows.append(dict(sector_name=name, **k))
kr = pd.DataFrame(rows).sort_values("sharpe", ascending=False)
km = kpi_in_range(md_r, "mkt_ret")

st.title("📊 Hiệu quả, rủi ro & dòng tiền của 11 ngành trên TTCK Việt Nam")
st.caption(f"Nguồn: API DNSE (OHLCV ngày) · {tk.ticker.nunique()} mã · {len(SECTORS)} ngành ICB cấp 2 · "
           f"giai đoạn đang xem {d0:%d/%m/%Y} – {d1:%d/%m/%Y} · pipeline PySpark {metrics.get('spark_version', '')}")

tabs = st.tabs(["Tổng quan", "Q1 · Hiệu quả ngành", "Q2 · Phòng thủ / tấn công", "Q3 · Dòng tiền",
                "Q4 · Đa dạng hóa & RMT", "Q5 · Tra cứu mã", "Portfolio Lab (Backtest VN)", "Dữ liệu & pipeline"])
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

    st.subheader(f"Chỉ số ngành ({weight_mode}, gốc = 100 tại đầu giai đoạn)")
    fig = go.Figure()
    for name, part in sd_r.groupby("sector_name"):
        idx = 100 * (1 + part[RET_COL].fillna(0)).cumprod()
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
                               cliponaxis=False,
                               hovertemplate="%{y}: %{x:" + fmt + "}<extra></extra>"))
        fig.update_layout(title=title, xaxis_tickformat=fmt)
        fig = style(fig, 460)
        fig.update_layout(margin=dict(l=10, r=60, t=40, b=10))
        col.plotly_chart(fig, width="stretch")

    # So sánh Equal-Weight vs Liquidity-Weighted & Rủi ro đuôi (CVaR 95%, Time-to-Recovery)
    kpi_sec = g["sector_kpi"].query("sector_code != 'VNINDEX'")
    kpi_sec = kpi_sec[kpi_sec.sector_name.isin(sel_sectors)]
    if "cagr_lw" in kpi_sec.columns and "cvar_95" in kpi_sec.columns:
        st.subheader("Kiểm chứng Size Effect & Rủi ro đuôi (cả giai đoạn, từ Spark)")
        c1, c2 = st.columns(2)
        d_ew = kpi_sec.sort_values("cagr")
        fig = go.Figure()
        fig.add_bar(y=d_ew.sector_name, x=d_ew.cagr, name="Bình quân đều (EW)", orientation="h",
                    marker_color=BLUE, hovertemplate="%{y} (EW): %{x:.1%}<extra></extra>")
        fig.add_bar(y=d_ew.sector_name, x=d_ew.cagr_lw, name="Gia quyền thanh khoản (LW)", orientation="h",
                    marker_color=ORANGE, hovertemplate="%{y} (LW): %{x:.1%}<extra></extra>")
        fig.update_layout(title="CAGR: Bình quân đều vs Gia quyền thanh khoản (lag 20p)", barmode="group",
                          xaxis_tickformat=".0%", bargap=0.25)
        c1.plotly_chart(style(fig, 440), width="stretch")

        fig = go.Figure(go.Scatter(
            x=kpi_sec.max_underwater_days, y=kpi_sec.cvar_95, mode="markers+text",
            text=kpi_sec.sector_name, textposition="top center",
            marker=dict(size=12, color=BLUE, line=dict(color="white", width=2)),
            customdata=kpi_sec[["calmar", "median_amihud"]],
            hovertemplate="%{text}<br>Chìm dưới đỉnh lâu nhất: %{x:.0f} phiên<br>CVaR 95%: %{y:.2%}/phiên"
                          "<br>Calmar: %{customdata[0]:.2f} · Amihud TV: %{customdata[1]:.5f}<extra></extra>"))
        fig.update_layout(title="Rủi ro đuôi (CVaR 95%) vs Thời gian về bờ lâu nhất",
                          xaxis_title="Số phiên liên tiếp chìm dưới đỉnh (Time-to-Recovery)",
                          yaxis_title="CVaR 95% (TB 5% phiên xấu nhất)", yaxis_tickformat=".1%")
        c2.plotly_chart(style(fig, 440), width="stretch")

    st.subheader("Lợi suất từng năm theo ngành (xanh: tăng, đỏ: giảm)")
    y = g["sector_yearly"][g["sector_yearly"].sector_name.isin(sel_sectors)]
    piv = y.pivot(index="sector_name", columns="year", values="annual_return")
    piv.columns = [f"{c}{' (YTD)' if y[y.year == c].is_partial_year.any() else ''}" for c in piv.columns]
    lim = float(np.nanmax(np.abs(piv.values)))
    fig = px.imshow(piv, color_continuous_scale=DIVERGING, zmin=-lim, zmax=lim, aspect="auto",
                    text_auto=".0%", labels=dict(color="Lợi suất"))
    fig.update_traces(hovertemplate="%{y} · %{x}: %{z:.1%}<extra></extra>")
    st.plotly_chart(style(fig, 460), width="stretch")
    with st.expander("Bảng KPI ngành đầy đủ (EW, LW, CVaR 95%, Calmar, Amihud, Thời gian về bờ — từ Spark)"):
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

    # Phân cụm chế độ thị trường không giám sát bằng PySpark MLlib K-Means
    if "systemic_risk_daily" in g and "ml_regime_sector" in g:
        st.subheader("Phân cụm chế độ thị trường bằng PySpark MLlib (K-Means: Lợi suất 20p · Biến động 20p · Breadth MA50 · Absorption Ratio)")
        sr = g["systemic_risk_daily"].dropna(subset=["ml_regime"])
        sr = sr[(sr.date.dt.date >= d0) & (sr.date.dt.date <= d1)]
        c1, c2 = st.columns(2)
        if len(sr):
            cmap = {"ML: Tăng bền vững": BLUE, "ML: Tích lũy / Chuyển pha": MUTED, "ML: Hoảng loạn / Rủi ro cao": "#d03b3b"}
            fig = go.Figure()
            for lbl, col_hex in cmap.items():
                sub = sr[sr.ml_regime == lbl]
                fig.add_scatter(x=sub.date, y=sub.mkt_close, mode="markers", name=lbl,
                                marker=dict(size=4.5, color=col_hex),
                                hovertemplate=f"{lbl}<br>%{{x|%d/%m/%Y}}: VN-Index %{{y:,.1f}}<extra></extra>")
            fig.update_layout(title="VN-Index tô màu theo 3 cụm chế độ PySpark MLlib", yaxis_title="VN-Index")
            c1.plotly_chart(style(fig, 430), width="stretch")

        ml_s = g["ml_regime_sector"]
        ml_s = ml_s[ml_s.sector_name.isin(sel_sectors) & ml_s.ml_regime.str.contains("Hoảng loạn", na=False)].sort_values("ann_excess_vs_vnindex")
        if len(ml_s):
            fig = go.Figure(go.Bar(
                y=ml_s.sector_name, x=ml_s.ann_excess_vs_vnindex, orientation="h",
                marker_color=[BLUE if x >= 0 else "#d03b3b" for x in ml_s.ann_excess_vs_vnindex],
                text=ml_s.ann_excess_vs_vnindex, texttemplate="%{text:+.1%}", textposition="outside",
                cliponaxis=False,
                hovertemplate="%{y}: %{x:+.1%}/năm so với VN-Index<extra>ML Hoảng loạn</extra>"))
            fig.update_layout(title="Vượt/kém VN-Index trong cụm 'ML: Hoảng loạn / Rủi ro cao'", xaxis_tickformat="+.0%")
            fig = style(fig, 430)
            fig.update_layout(margin=dict(l=10, r=60, t=40, b=10))
            c2.plotly_chart(fig, width="stretch")

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

    # Kiểm định Lead-Lag dòng tiền (Smart Money vs Retail FOMO)
    if "flow_lead_lag" in g and len(g["flow_lead_lag"]):
        st.subheader("Kiểm định Lead–Lag Dòng tiền: Đột biến thanh khoản là Smart Money hay Retail FOMO?")
        fl = g["flow_lead_lag"]
        fl_opts = ["Toàn thị trường (11 ngành)"] + sorted(s for s in fl.sector_name.unique() if s != "Toàn thị trường (11 ngành)")
        fl_choice = st.selectbox("Chọn phạm vi kiểm định Lead–Lag", fl_opts, index=0)
        sub = fl[fl.sector_name == fl_choice].sort_values("flow_quintile")
        q_labels = {1: "Q1 (Cạn thanh khoản)", 2: "Q2 (Thấp)", 3: "Q3 (Trung bình)", 4: "Q4 (Cao)", 5: "Q5 (Đột biến / FOMO)"}
        x_lab = [q_labels.get(int(q), f"Q{q}") for q in sub.flow_quintile]
        fig = go.Figure()
        fig.add_bar(x=x_lab, y=sub.avg_fwd_20d, name="Lợi suất 20 phiên sau (~1 tháng)", marker_color=BLUE,
                    text=sub.avg_fwd_20d, texttemplate="%{text:+.1%}", textposition="outside",
                    hovertemplate="%{x}<br>20 phiên sau: %{y:+.2%}<extra></extra>")
        fig.add_bar(x=x_lab, y=sub.avg_fwd_60d, name="Lợi suất 60 phiên sau (~1 quý)", marker_color=ORANGE,
                    text=sub.avg_fwd_60d, texttemplate="%{text:+.1%}", textposition="outside",
                    hovertemplate="%{x}<br>60 phiên sau: %{y:+.2%}<extra></extra>")
        fig.update_layout(title=f"Lợi suất tương lai theo phân vị đột biến dòng tiền (flow_z60) — {fl_choice}",
                          barmode="group", yaxis_tickformat="+.0%")
        st.plotly_chart(style(fig, 380), width="stretch")
        st.caption("flow_z60 tính bằng z-score log(GTGD) so với 60 phiên TRƯỚC ĐÓ (không look-ahead). Nếu Q5 kém hơn Q1 → hiệu ứng FOMO đu đỉnh.")

# ---------------------------------------------------------------- Q4
with tabs[4]:
    insight_box("Q4")
    names = dict(zip(g["sector_kpi"].sector_code, g["sector_kpi"].sector_name))
    has_cr = "sector_corr_regime" in g and len(g["sector_corr_regime"])
    corr_mode = st.radio(
        "Chế độ ma trận tương quan (Tail Dependence — Econophysics)",
        ["Toàn giai đoạn", "Bình thường (VN-Index > -1,5%)", "Hoảng loạn (VN-Index ≤ -1,5%)", "Chênh lệch Δρ (Hoảng loạn − Bình thường)"]
        if has_cr else ["Toàn giai đoạn"],
        horizontal=True,
    )
    if corr_mode == "Toàn giai đoạn":
        c = g["sector_corr"].assign(a=lambda d: d.sector_a.map(names), b=lambda d: d.sector_b.map(names))
        val_col, cscale, zmin_v, zmax_v = "corr", SEQUENTIAL, None, 1.0
    else:
        cr = g["sector_corr_regime"].assign(a=lambda d: d.sector_a.map(names), b=lambda d: d.sector_b.map(names))
        c = cr
        if "Bình thường" in corr_mode and "Chênh lệch" not in corr_mode:
            val_col, cscale, zmin_v, zmax_v = "corr_normal", SEQUENTIAL, None, 1.0
        elif "Hoảng loạn" in corr_mode and "Chênh lệch" not in corr_mode:
            val_col, cscale, zmin_v, zmax_v = "corr_crash", SEQUENTIAL, None, 1.0
        else:
            val_col, cscale, zmin_v, zmax_v = "corr_diff", DIVERGING, -0.4, 0.4

    c = c[c.a.isin(sel_sectors) & c.b.isin(sel_sectors)]
    piv = c.pivot(index="a", columns="b", values=val_col)
    if zmin_v is None:
        zmin_v = max(0.0, float(np.nanmin(piv.values)))
    fig = px.imshow(piv, color_continuous_scale=cscale, zmin=zmin_v, zmax=zmax_v,
                    text_auto=".2f", aspect="auto", labels=dict(color="Tương quan"))
    fig.update_traces(hovertemplate="%{y} × %{x}: %{z:+.2f}<extra></extra>" if val_col == "corr_diff" else "%{y} × %{x}: %{z:.2f}<extra></extra>")
    fig.update_layout(title=f"Ma trận tương quan lợi suất ngày — {corr_mode}", xaxis_title="", yaxis_title="")
    st.plotly_chart(style(fig, 540), width="stretch")

    # Biểu đồ Rủi ro Hệ thống Absorption Ratio (Random Matrix Theory)
    if "systemic_risk_daily" in g and "absorption_ratio" in g["systemic_risk_daily"].columns:
        st.subheader("Chỉ số Rủi ro Hệ thống: Absorption Ratio (Random Matrix Theory, trượt 120 phiên)")
        sr = g["systemic_risk_daily"].dropna(subset=["absorption_ratio"])
        sr = sr[(sr.date.dt.date >= d0) & (sr.date.dt.date <= d1)]
        if len(sr):
            fig = go.Figure()
            fig.add_scatter(x=sr.date, y=sr.absorption_ratio, name="Absorption Ratio (λ₁ / Σλ)",
                            line=dict(color=ORANGE, width=2), yaxis="y",
                            hovertemplate="Absorption Ratio<br>%{x|%d/%m/%Y}: %{y:.1%}<extra></extra>")
            fig.add_scatter(x=sr.date, y=sr.mkt_close, name="VN-Index (trục phải)",
                            line=dict(color=INK, width=1.5, dash="dot"), yaxis="y2", opacity=0.7,
                            hovertemplate="VN-Index<br>%{x|%d/%m/%Y}: %{y:,.1f}<extra></extra>")
            fig.update_layout(
                title="Chuyển pha rủi ro hệ thống (Absorption Ratio tăng vọt khi toàn thị trường đồng pha hoảng loạn)",
                yaxis=dict(title="Absorption Ratio", tickformat=".0%", gridcolor=GRID),
                yaxis2=dict(title="VN-Index", overlaying="y", side="right", showgrid=False),
            )
            st.plotly_chart(style(fig, 400), width="stretch")
            st.caption("Absorption Ratio = tỷ trọng phương sai giải thích bởi trị riêng lớn nhất của ma trận tương quan 11 ngành (trượt 120 phiên).")

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

# ---------------------------------------------------------------- Portfolio Lab (Backtest VN)
with tabs[6]:
    st.subheader("🧪 Portfolio Lab — Kiểm định danh mục phân bổ ngành (có phí & thuế TTCK Việt Nam)")
    st.caption("Mô phỏng không nhìn tương lai (tín hiệu ngày t−1 áp dụng cho ngày t) trên các ngành đang chọn ở thanh bên.")

    p1, p2, p3 = st.columns(3)
    strat_opts = ["Bình quân đều (Equal-Weight)", "Risk Parity (Inverse Volatility 60p)",
                  "Regime-Switching (Giữ tiền mặt khi dưới MA200)"]
    if "systemic_risk_daily" in g and "ml_regime" in g["systemic_risk_daily"].columns:
        strat_opts.append("Regime-Switching (Tiền mặt khi MLlib Hoảng loạn)")
    strategy = p1.selectbox("Chiến lược phân bổ", strat_opts)
    rebal = p2.selectbox("Tần suất tái cân bằng", ["Hàng tháng", "Hàng quý", "Hàng ngày (lý thuyết)"])
    tc_bps = p3.slider("Phí giao dịch + thuế bán (%/chiều)", min_value=0.0, max_value=1.0, value=0.25, step=0.05,
                       help="Mặc định 0,25%/chiều ≈ 0,15% phí môi giới + 0,10% thuế bán chứng khoán tại Việt Nam.")

    piv_r = sd_r.pivot(index="date", columns="sector_name", values=RET_COL).sort_index().fillna(0.0)
    mkt_s = md_r.set_index("date")["mkt_ret"].reindex(piv_r.index).fillna(0.0)
    dates_bt = piv_r.index
    n_sec = piv_r.shape[1]

    if len(dates_bt) < 30 or n_sec == 0:
        st.warning("Cần ít nhất 30 phiên và 1 ngành để chạy kiểm định danh mục.")
    else:
        # Xác định ngày tái cân bằng
        if rebal == "Hàng tháng":
            is_rebal = dates_bt.to_series().dt.to_period("M").ne(dates_bt.to_series().dt.to_period("M").shift(1)).to_numpy(copy=True)
        elif rebal == "Hàng quý":
            is_rebal = dates_bt.to_series().dt.to_period("Q").ne(dates_bt.to_series().dt.to_period("Q").shift(1)).to_numpy(copy=True)
        else:
            is_rebal = np.ones(len(dates_bt), dtype=bool)
        is_rebal[0] = True

        # Tín hiệu phòng thủ (trễ 1 phiên để không look-ahead)
        reg_ma = md.set_index("date")["regime"].reindex(dates_bt).shift(1)
        cash_mask = np.zeros(len(dates_bt), dtype=bool)
        if "MA200" in strategy:
            cash_mask = reg_ma.astype(str).str.startswith("Giảm").values
        elif "MLlib" in strategy:
            reg_ml = g["systemic_risk_daily"].set_index("date")["ml_regime"].reindex(dates_bt).shift(1)
            cash_mask = reg_ml.astype(str).str.contains("Hoảng loạn", na=False).values

        # Độ biến động trượt 60 phiên (trễ 1 phiên) cho Risk Parity
        vol60 = piv_r.rolling(60, min_periods=20).std().shift(1)

        # Tính chuỗi trọng số mục tiêu & lợi suất sau phí (vector hóa nhẹ theo ngày)
        R = piv_r.values
        w_cur = np.zeros(n_sec, dtype=float)
        port_ret = np.zeros(len(dates_bt), dtype=float)
        total_turnover = 0.0
        rf_daily = config.RF_ANNUAL / config.TRADING_DAYS
        tc_rate = tc_bps / 100.0

        for i in range(len(dates_bt)):
            # Khi đổi trạng thái tiền mặt hoặc đến kỳ tái cân bằng -> tính lại trọng số mục tiêu
            need_rebal = is_rebal[i] or (i > 0 and cash_mask[i] != cash_mask[i - 1])
            if need_rebal:
                if cash_mask[i]:
                    w_tgt = np.zeros(n_sec, dtype=float)
                elif "Risk Parity" in strategy:
                    v = vol60.iloc[i].values
                    valid = np.isfinite(v) & (v > 1e-8)
                    if valid.any():
                        inv_v = np.where(valid, 1.0 / np.where(valid, v, 1.0), 0.0)
                        w_tgt = inv_v / inv_v.sum()
                    else:
                        w_tgt = np.full(n_sec, 1.0 / n_sec)
                else:
                    w_tgt = np.full(n_sec, 1.0 / n_sec)
                turnover = float(np.abs(w_tgt - w_cur).sum())
                cost = turnover * tc_rate
                total_turnover += turnover
                w_cur = w_tgt.copy()
            else:
                cost = 0.0

            cash_w = max(0.0, 1.0 - float(w_cur.sum()))
            gross_r = float(np.dot(w_cur, R[i])) + cash_w * rf_daily
            port_ret[i] = gross_r - cost
            # Cập nhật trôi trọng số (drift) cuối phiên
            if w_cur.sum() > 0:
                w_after = w_cur * (1.0 + R[i])
                denom = 1.0 + gross_r
                w_cur = w_after / denom if denom > 0 else w_cur

        bt_df = pd.DataFrame({"date": dates_bt, "port_ret": port_ret, "mkt_ret": mkt_s.values})
        kp = kpi_in_range(bt_df, "port_ret")
        calmar_p = kp["cagr"] / abs(kp["mdd"]) if pd.notna(kp["mdd"]) and kp["mdd"] < 0 else np.nan
        total_tc_pct = total_turnover * tc_rate

        mc = st.columns(5)
        mc[0].metric("CAGR danh mục (sau phí)", insights.pct(kp["cagr"]),
                     f"vs VN-Index {insights.pct(km['cagr'])}", delta_color="off")
        mc[1].metric("Sharpe (sau phí)", insights.num(kp["sharpe"]),
                     f"vs VN-Index {insights.num(km['sharpe'])}", delta_color="off")
        mc[2].metric("Max Drawdown", insights.pct(kp["mdd"]),
                     f"vs VN-Index {insights.pct(km['mdd'])}", delta_color="off")
        mc[3].metric("Calmar Ratio", insights.num(calmar_p))
        mc[4].metric("Tổng chi phí giao dịch", insights.pct(total_tc_pct),
                     f"Vòng quay {total_turnover:.1f}x", delta_color="off")

        w_port = 100 * (1 + bt_df.port_ret).cumprod()
        w_mkt = 100 * (1 + bt_df.mkt_ret).cumprod()
        dd_port = w_port / w_port.cummax() - 1
        dd_mkt = w_mkt / w_mkt.cummax() - 1

        fig = go.Figure()
        fig.add_scatter(x=bt_df.date, y=w_port, name=f"Danh mục: {strategy}", line=dict(color=BLUE, width=2.5),
                        hovertemplate="Danh mục<br>%{x|%d/%m/%Y}: %{y:.1f}<extra></extra>")
        fig.add_scatter(x=bt_df.date, y=w_mkt, name="VN-Index", line=dict(color=INK, width=2, dash="dash"),
                        hovertemplate="VN-Index<br>%{x|%d/%m/%Y}: %{y:.1f}<extra></extra>")
        fig.update_layout(title="Đường cong tài sản sau phí & thuế (gốc = 100)", yaxis_title="Tài sản (gốc 100)")
        st.plotly_chart(style(fig, 400), width="stretch")

        fig = go.Figure()
        fig.add_scatter(x=bt_df.date, y=dd_port, name="Drawdown danh mục", fill="tozeroy",
                        line=dict(color=BLUE, width=1.5), fillcolor="rgba(42,120,214,0.18)",
                        hovertemplate="Danh mục<br>%{x|%d/%m/%Y}: %{y:.1%}<extra></extra>")
        fig.add_scatter(x=bt_df.date, y=dd_mkt, name="Drawdown VN-Index", line=dict(color=ORANGE, width=1.5, dash="dot"),
                        hovertemplate="VN-Index<br>%{x|%d/%m/%Y}: %{y:.1%}<extra></extra>")
        fig.update_layout(title="Mức sụt giảm từ đỉnh (Drawdown)", yaxis_tickformat=".0%")
        st.plotly_chart(style(fig, 300), width="stretch")

# ---------------------------------------------------------------- Dữ liệu & pipeline
with tabs[7]:
    st.subheader("Luồng dữ liệu")
    st.code("API DNSE (JSON/mã/năm) + universe.csv (phân ngành) + chỉ số VNINDEX/VN30\n"
            "   → PySpark: schema → DQ-01..13 → làm sạch → broadcast join + join theo ngày → Window (Amihud, Limit-Hit, Beta, Drawdown)\n"
            "   → Silver Parquet partitionBy(sector_code, year)\n"
            "   → Gold (Spark SQL / groupBy / self-join / RMT Absorption Ratio / PySpark MLlib K-Means): 14 bảng KPI Parquet + CSV\n"
            "   → Dashboard BI  +  Streamlit (app này + Portfolio Lab)  →  Insight / khuyến nghị", language="text")
    if metrics:
        st.markdown("**Số dòng qua từng bước pipeline**")
        st.dataframe(pd.DataFrame([{"Bước": k, **v} for k, v in metrics["stages"].items() if k != "4_gold_tables"]),
                     width="stretch", hide_index=True)
    st.markdown("**Báo cáo chất lượng dữ liệu**")
    st.dataframe(g["data_quality_report"], width="stretch", hide_index=True)
    st.markdown("**Các phiên biến động bất thường bị loại khỏi thống kê lợi suất** (cần giải thích trong báo cáo)")
    st.dataframe(g["abnormal_events"].head(200), width="stretch", hide_index=True)
