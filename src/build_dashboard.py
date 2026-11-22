"""
BƯỚC 3 — DASHBOARD BI (HTML tương tác, tự chứa) + FILE INSIGHT
==============================================================
Đọc bảng Gold (CSV do PySpark xuất) -> sinh:
  - dashboard/BI_Dashboard_Nganh_VN.html : dashboard BI mở trực tiếp bằng trình duyệt, không cần Internet
    (KPI cards, 4 bộ lọc, 8 visual liên kết: chọn ngành / giai đoạn / click cột để làm nổi bật)
  - reports/insights_auto.md              : insight & khuyến nghị sinh từ số liệu
Dashboard này là "BI tool tương đương"; hướng dẫn dựng bản Power BI (.pbix) từ cùng dữ liệu: docs/POWER_BI.md

Chạy:  python src/build_dashboard.py
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from plotly.offline import get_plotlyjs

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402
import insights  # noqa: E402


def _clean(v):
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    return v


def records(df: pd.DataFrame, cols: list[str], digits: int = 6) -> list[list]:
    out = []
    for row in df[cols].itertuples(index=False):
        out.append([_clean(round(x, digits)) if isinstance(x, float) else
                    (x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else x) for x in row])
    return out


def md_bold(s: str) -> str:
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)


def build_payload(g: dict[str, pd.DataFrame]) -> dict:
    sd, md = g["sector_daily"], g["market_daily"]
    kpi = g["sector_kpi"]
    sectors = kpi[kpi.sector_code != "VNINDEX"].sort_values("sector_name")[["sector_code", "sector_name"]]
    dates = sorted(set(md.date) | set(sd.date))           # lịch chung (nguồn VN-Index thiếu một số ngày)
    piv = sd.pivot(index="date", columns="sector_code", values="sector_ret").reindex(dates)
    mk = md.set_index("date").reindex(dates)
    metrics_path = config.REPORTS_DIR / "pipeline_metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.exists() else {}
    ins = insights.generate(g)
    return {
        "meta": {"rf": config.RF_ANNUAL, "td": config.TRADING_DAYS,
                 "rows": metrics.get("stages", {}).get("3_silver_panel", {}).get("rows"),
                 "spark": metrics.get("spark_version"), "n_tickers": int(g["ticker_kpi"].ticker.nunique())},
        "sectors": sectors.values.tolist(),
        "dates": [d.strftime("%Y-%m-%d") for d in dates],
        "ret": {c: [_clean(round(float(x), 6)) if pd.notna(x) else None for x in piv[c]] for c in sectors.sector_code
                if c in piv},
        "mkt": [_clean(round(float(x), 6)) if pd.notna(x) else None for x in mk.mkt_ret],
        "yearly": records(g["sector_yearly"], ["sector_code", "year", "annual_return", "is_partial_year"]),
        "regime": records(g["sector_regime"], ["sector_code", "regime", "ann_excess_vs_vnindex", "ann_mean_return"]),
        "liq": records(g["liquidity_quarterly"], ["sector_code", "quarter", "value_share", "avg_daily_value_bn"]),
        "corr": records(g["sector_corr"], ["sector_a", "sector_b", "corr"]),
        "tickers": records(g["ticker_kpi"], ["ticker", "company_name", "sector_code", "exchange", "cagr", "ann_vol",
                                             "sharpe", "max_drawdown", "beta", "avg_value_bn"]),
        "questions": insights.QUESTIONS,
        "insights": {q: [md_bold(s) for s in v] for q, v in ins.items()},
        "recs": [md_bold(s) for s in insights.recommendations(g)],
    }


TEMPLATE = r"""<!DOCTYPE html>
<html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BI Dashboard — 11 ngành TTCK Việt Nam</title>
<style>
:root{--surface:#fcfcfb;--card:#ffffff;--ink:#0b0b0b;--ink2:#52514e;--muted:#b9b8b2;--line:#ecebe7;--blue:#2a78d6;--orange:#eb6834}
*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--surface);color:var(--ink)}
header{padding:18px 24px 6px}h1{font-size:22px;margin:0 0 4px}header p{margin:0;color:var(--ink2);font-size:13px}
.filters{display:flex;flex-wrap:wrap;gap:14px;align-items:flex-end;padding:12px 24px;position:sticky;top:0;background:var(--surface);z-index:5;border-bottom:1px solid var(--line)}
.filters label{font-size:12px;color:var(--ink2);display:block;margin-bottom:3px}
select,button{font:inherit;font-size:13px;padding:5px 8px;border:1px solid #d6d5cf;border-radius:6px;background:#fff}
.chips{display:flex;flex-wrap:wrap;gap:6px;max-width:760px}.chip{font-size:12px;padding:4px 9px;border-radius:14px;border:1px solid #d6d5cf;cursor:pointer;user-select:none;background:#fff}
.chip.on{background:#e3eefb;border-color:var(--blue);color:#104281}
main{padding:12px 24px 40px}.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:14px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}.kpi .l{font-size:12px;color:var(--ink2)}.kpi .v{font-size:22px;font-weight:600;margin-top:4px}.kpi .s{font-size:12px;color:var(--ink2)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
.card h3{font-size:14px;margin:2px 0 2px}.card .sub{font-size:12px;color:var(--ink2);margin-bottom:4px}.full{grid-column:1/-1}
.ins{font-size:13px;line-height:1.5}.ins h4{margin:10px 0 4px;font-size:13px}.ins ul{margin:0 0 4px 18px;padding:0}
table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:5px 6px;border-bottom:1px solid var(--line);text-align:right}th{cursor:pointer;color:var(--ink2);font-weight:600;position:sticky;top:0;background:#fff}td:nth-child(-n+3),th:nth-child(-n+3){text-align:left}
.tablewrap{max-height:420px;overflow:auto}
@media(max-width:1000px){.grid{grid-template-columns:1fr}.kpis{grid-template-columns:repeat(2,1fr)}}
</style><script>__PLOTLY__</script></head>
<body>
<header><h1>Hiệu quả, rủi ro &amp; dòng tiền của 11 ngành ICB cấp 2 — TTCK Việt Nam</h1>
<p id="subtitle"></p></header>
<div class="filters">
 <div><label>Từ năm</label><select id="y0"></select></div>
 <div><label>Đến năm</label><select id="y1"></select></div>
 <div><label>Ngành làm nổi bật (hoặc click cột ở biểu đồ xếp hạng)</label><select id="hl"></select></div>
 <div><label>Chỉ số xếp hạng</label><select id="metric"><option value="sharpe">Sharpe</option><option value="cagr">CAGR</option><option value="vol">Biến động</option><option value="mdd">Max drawdown</option></select></div>
 <div style="flex:1"><label>Ngành đưa vào phân tích</label><div class="chips" id="chips"></div></div>
 <div><button id="all">Chọn tất cả</button></div>
</div>
<main>
 <div class="kpis" id="kpis"></div>
 <div class="grid">
  <div class="card full"><h3>Chỉ số ngành bình quân đều (gốc 100 đầu giai đoạn)</h3><div class="sub">Xanh: ngành nổi bật · xám: ngành khác · nét đứt: VN-Index</div><div id="c_line" style="height:380px"></div></div>
  <div class="card"><h3 id="rank_title">Xếp hạng ngành</h3><div class="sub">Tính lại theo giai đoạn đã chọn · click 1 cột để làm nổi bật ngành</div><div id="c_rank" style="height:420px"></div></div>
  <div class="card"><h3>Lợi suất từng năm</h3><div class="sub">Xanh: tăng · đỏ: giảm · YTD = năm chưa trọn</div><div id="c_year" style="height:420px"></div></div>
  <div class="card"><h3>Vượt/kém VN-Index theo chế độ thị trường</h3><div class="sub">Năm hóa, cả giai đoạn · chế độ = VN-Index trên/dưới MA200</div><div id="c_regime" style="height:420px"></div></div>
  <div class="card"><h3>Rủi ro – lợi suất từng mã</h3><div class="sub">Cả giai đoạn · mã của ngành nổi bật được gắn nhãn</div><div id="c_scatter" style="height:420px"></div></div>
  <div class="card"><h3>Tỷ trọng giá trị giao dịch theo quý</h3><div class="sub">Dòng tiền dịch chuyển giữa các ngành (trong các ngành đã chọn)</div><div id="c_liq" style="height:420px"></div></div>
  <div class="card"><h3>Tương quan lợi suất ngày giữa các ngành</h3><div class="sub">Càng nhạt càng có lợi cho đa dạng hóa</div><div id="c_corr" style="height:420px"></div></div>
  <div class="card full ins" id="insights"></div>
  <div class="card full"><h3>Bảng KPI từng mã</h3><div class="sub">Click tiêu đề cột để sắp xếp</div><div class="tablewrap"><table id="tbl"></table></div></div>
 </div>
</main>
<script>
const D = __DATA__;
const BLUE="#2a78d6", ORANGE="#eb6834", INK="#0b0b0b", MUTED="#b9b8b2", GRID="#ecebe7";
const SEQ=[[0,"#cde2fb"],[0.25,"#86b6ef"],[0.5,"#3987e5"],[0.75,"#256abf"],[1,"#104281"]];
const DIV=[[0,"#d03b3b"],[0.5,"#f0efec"],[1,"#2a78d6"]];
const NAME=Object.fromEntries(D.sectors), CODES=D.sectors.map(s=>s[0]);
const years=[...new Set(D.dates.map(d=>+d.slice(0,4)))];
const st={y0:years[0],y1:years[years.length-1],hl:CODES[0],sel:new Set(CODES),metric:"sharpe",sortKey:"sharpe",sortDir:-1};
const cfg={displayModeBar:false,responsive:true};
const base=(o={})=>Object.assign({margin:{l:10,r:10,t:10,b:30},paper_bgcolor:"#fff",plot_bgcolor:"#fff",font:{size:12,color:INK},
  xaxis:{gridcolor:GRID,zeroline:false,automargin:true},yaxis:{gridcolor:GRID,zeroline:false,automargin:true},hoverlabel:{bgcolor:"#fff"},showlegend:false},o);
const pct=(x,d=1)=>x==null||isNaN(x)?"n/a":(x*100).toFixed(d).replace(".",",")+"%";
const num=(x,d=2)=>x==null||isNaN(x)?"n/a":x.toFixed(d).replace(".",",");

function kpi(arr,d0,d1){const r=arr.filter(x=>x!=null);if(r.length<20)return{cagr:NaN,vol:NaN,sharpe:NaN,mdd:NaN};
 const yrs=(new Date(d1)-new Date(d0))/(365.25*864e5);
 let w=1,peak=1,mdd=0,s=0,s2=0;for(const x of r){w*=1+x;peak=Math.max(peak,w);mdd=Math.min(mdd,w/peak-1);s+=x;s2+=x*x;}
 const n=r.length,mu=s/n,sd=Math.sqrt((s2-n*mu*mu)/(n-1));
 return{cagr:Math.pow(w,1/yrs)-1,vol:sd*Math.sqrt(D.meta.td),sharpe:(mu*D.meta.td-D.meta.rf)/(sd*Math.sqrt(D.meta.td)),mdd};}
function rangeIdx(){let a=D.dates.findIndex(d=>+d.slice(0,4)>=st.y0),b=D.dates.length-1;while(b>0&&+D.dates[b].slice(0,4)>st.y1)b--;return[a,b];}
function sel(){return CODES.filter(c=>st.sel.has(c));}

function render(){
 const [a,b]=rangeIdx(),dates=D.dates.slice(a,b+1),S=sel();
 const dd0=dates[0],dd1=dates[dates.length-1];
 const K=Object.fromEntries(S.map(c=>[c,kpi(D.ret[c].slice(a,b+1),dd0,dd1)])),KM=kpi(D.mkt.slice(a,b+1),dd0,dd1);
 const best=S.slice().sort((x,y)=>K[y].sharpe-K[x].sharpe)[0];
 document.getElementById("subtitle").textContent=`Nguồn: API DNSE · ${D.meta.n_tickers} mã · ${(D.meta.rows||0).toLocaleString("vi-VN")} dòng giao dịch sạch (PySpark ${D.meta.spark||""}) · đang xem ${dates[0]} → ${dates[dates.length-1]} · rf = ${pct(D.meta.rf,0)}`;
 const cards=[["Ngành đang phân tích",`${S.length}/${CODES.length}`,""],["CAGR VN-Index",pct(KM.cagr),`Sharpe ${num(KM.sharpe)} · MaxDD ${pct(KM.mdd)}`],
  ["Ngành Sharpe cao nhất",best?NAME[best]:"–",best?`Sharpe ${num(K[best].sharpe)} · CAGR ${pct(K[best].cagr)}`:""],
  ["Số ngành CAGR > VN-Index",`${S.filter(c=>K[c].cagr>KM.cagr).length}/${S.length}`,""],
  [`Ngành nổi bật: ${NAME[st.hl]}`,pct(K[st.hl]?K[st.hl].cagr:NaN),K[st.hl]?`Sharpe ${num(K[st.hl].sharpe)} · MaxDD ${pct(K[st.hl].mdd)}`:"(chưa chọn ngành này)"]];
 document.getElementById("kpis").innerHTML=cards.map(c=>`<div class="kpi"><div class="l">${c[0]}</div><div class="v">${c[1]}</div><div class="s">${c[2]}</div></div>`).join("");

 // 1. line
 const cum=arr=>{let w=100;return arr.map(x=>(w*=1+(x||0)));};
 const tr=S.map(c=>({x:dates,y:cum(D.ret[c].slice(a,b+1)),mode:"lines",name:NAME[c],line:{color:c===st.hl?BLUE:MUTED,width:c===st.hl?2.5:1},opacity:c===st.hl?1:.6,hovertemplate:NAME[c]+"<br>%{x}: %{y:.1f}<extra></extra>"}));
 tr.sort((p,q)=>(p.name===NAME[st.hl])-(q.name===NAME[st.hl]));
 tr.push({x:dates,y:cum(D.mkt.slice(a,b+1)),mode:"lines",name:"VN-Index",line:{color:INK,width:2,dash:"dash"},hovertemplate:"VN-Index<br>%{x}: %{y:.1f}<extra></extra>"});
 Plotly.react("c_line",tr,base({hovermode:"closest"}),cfg);

 // 2. rank bar (click to highlight)
 const m=st.metric,lab={sharpe:"Sharpe",cagr:"CAGR",vol:"Biến động năm hóa",mdd:"Max drawdown"}[m];
 document.getElementById("rank_title").textContent="Xếp hạng ngành theo "+lab;
 const rows=S.map(c=>[c,NAME[c],K[c][m]]).concat([["VNINDEX","VN-Index",KM[m]]]).sort((p,q)=>p[2]-q[2]);
 const isPct=m!=="sharpe";
 Plotly.react("c_rank",[{type:"bar",orientation:"h",y:rows.map(r=>r[1]),x:rows.map(r=>r[2]),customdata:rows.map(r=>r[0]),
   marker:{color:rows.map(r=>r[0]==="VNINDEX"?INK:(r[0]===st.hl?BLUE:"#86b6ef"))},text:rows.map(r=>isPct?pct(r[2]):num(r[2])),textposition:"outside",cliponaxis:false,
   hovertemplate:"%{y}: %{text}<extra></extra>"}],base({xaxis:{gridcolor:GRID,tickformat:isPct?".0%":".1f",automargin:true}}),cfg);

 // 3. yearly heatmap
 const ys=years.filter(y=>y>=st.y0&&y<=st.y1),Y={},partial={};
 D.yearly.forEach(([c,y,r,p])=>{(Y[c]=Y[c]||{})[y]=r;if(p)partial[y]=1;});
 const z=S.map(c=>ys.map(y=>Y[c]&&Y[c][y]!=null?Y[c][y]:null)),zl=Math.max(...z.flat().filter(v=>v!=null).map(Math.abs),0.01);
 Plotly.react("c_year",[{type:"heatmap",z,x:ys.map(y=>partial[y]?y+" YTD":String(y)),y:S.map(c=>NAME[c]),colorscale:DIV,zmin:-zl,zmax:zl,
   text:z.map(r=>r.map(v=>v==null?"":pct(v,0))),texttemplate:"%{text}",hovertemplate:"%{y} · %{x}: %{text}<extra></extra>",colorbar:{tickformat:".0%",thickness:10}}],
   base({xaxis:{type:"category",automargin:true},yaxis:{automargin:true}}),cfg);

 // 4. regime grouped bar
 const R={};D.regime.forEach(([c,rg,ex])=>{(R[c]=R[c]||{})[rg.startsWith("Tăng")?"up":"down"]=ex;});
 const ord=S.slice().sort((p,q)=>((R[p]||{}).down||0)-((R[q]||{}).down||0));
 Plotly.react("c_regime",[["up","VN-Index trên MA200",BLUE],["down","VN-Index dưới MA200",ORANGE]].map(([k,n,col])=>({type:"bar",orientation:"h",name:n,
   y:ord.map(c=>NAME[c]),x:ord.map(c=>(R[c]||{})[k]),marker:{color:col},hovertemplate:"%{y}: %{x:+.1%}/năm<extra>"+n+"</extra>"})),
   base({barmode:"group",showlegend:true,legend:{orientation:"h",y:1.08},xaxis:{gridcolor:GRID,tickformat:"+.0%",automargin:true},margin:{l:10,r:10,t:30,b:30}}),cfg);

 // 5. scatter tickers
 const T=D.tickers.filter(t=>st.sel.has(t[2]));
 const grp=[T.filter(t=>t[2]!==st.hl),T.filter(t=>t[2]===st.hl)];
 Plotly.react("c_scatter",grp.map((g,i)=>({type:"scatter",mode:i?"markers+text":"markers",x:g.map(t=>t[5]),y:g.map(t=>t[4]),text:g.map(t=>t[0]),textposition:"top center",
   customdata:g.map(t=>[NAME[t[2]],t[6],t[7]]),marker:{size:i?11:8,color:i?BLUE:MUTED,line:{color:"#fff",width:2}},
   hovertemplate:"%{text} · %{customdata[0]}<br>CAGR %{y:.1%} · biến động %{x:.1%}<br>Sharpe %{customdata[1]:.2f} · MaxDD %{customdata[2]:.1%}<extra></extra>"})),
   base({xaxis:{gridcolor:GRID,tickformat:".0%",title:"Biến động năm hóa",automargin:true},yaxis:{gridcolor:GRID,tickformat:".0%",title:"CAGR",automargin:true}}),cfg);

 // 6. liquidity share heatmap (recomputed within selected sectors)
 const L={},qs=[...new Set(D.liq.map(r=>r[1]))].sort().filter(q=>+q.slice(0,4)>=st.y0&&+q.slice(0,4)<=st.y1),tot={};
 D.liq.forEach(([c,q,,v])=>{if(st.sel.has(c)){(L[c]=L[c]||{})[q]=v;tot[q]=(tot[q]||0)+v;}});
 const lz=S.map(c=>qs.map(q=>L[c]&&L[c][q]!=null?L[c][q]/tot[q]:null));
 Plotly.react("c_liq",[{type:"heatmap",z:lz,x:qs,y:S.map(c=>NAME[c]),colorscale:SEQ,hovertemplate:"%{y} · %{x}: %{z:.1%}<extra></extra>",colorbar:{tickformat:".0%",thickness:10}}],
   base({xaxis:{type:"category",automargin:true,nticks:12},yaxis:{automargin:true}}),cfg);

 // 7. correlation
 const C={};D.corr.forEach(([p,q,v])=>{(C[p]=C[p]||{})[q]=v;});
 const cz=S.map(p=>S.map(q=>C[p]&&C[p][q]!=null?C[p][q]:null)),cmin=Math.min(...cz.flat().filter(v=>v!=null));
 Plotly.react("c_corr",[{type:"heatmap",z:cz,x:S.map(c=>NAME[c]),y:S.map(c=>NAME[c]),colorscale:SEQ,zmin:Math.max(0,cmin),zmax:1,
   text:cz.map(r=>r.map(v=>v==null?"":num(v))),texttemplate:S.length<=8?"%{text}":"",hovertemplate:"%{y} × %{x}: %{text}<extra></extra>",colorbar:{thickness:10}}],
   base({xaxis:{automargin:true,tickangle:-40},yaxis:{automargin:true}}),cfg);

 // table
 const cols=[["ticker","Mã"],["company_name","Doanh nghiệp"],["sector","Ngành"],["exchange","Sàn"],["cagr","CAGR"],["ann_vol","Biến động"],["sharpe","Sharpe"],["max_drawdown","MaxDD"],["beta","Beta"],["avg_value_bn","GTGD TB (tỷ)"]];
 const idx={ticker:0,company_name:1,sector:2,exchange:3,cagr:4,ann_vol:5,sharpe:6,max_drawdown:7,beta:8,avg_value_bn:9};
 const rowsT=T.slice().sort((p,q)=>{const i=idx[st.sortKey],x=p[i],y=q[i];return (x>y?1:x<y?-1:0)*st.sortDir;});
 const f=(k,v)=>["cagr","ann_vol","max_drawdown"].includes(k)?pct(v):(k==="sharpe"||k==="beta")?num(v):k==="avg_value_bn"?num(v,1):k==="sector"?NAME[v]:v;
 document.getElementById("tbl").innerHTML="<thead><tr>"+cols.map(([k,n])=>`<th data-k="${k}">${n}${st.sortKey===k?(st.sortDir>0?" ▲":" ▼"):""}</th>`).join("")+"</tr></thead><tbody>"+
   rowsT.map(t=>"<tr>"+cols.map(([k])=>`<td>${f(k,t[idx[k]])}</td>`).join("")+"</tr>").join("")+"</tbody>";
 document.querySelectorAll("#tbl th").forEach(th=>th.onclick=()=>{const k=th.dataset.k;st.sortDir=st.sortKey===k?-st.sortDir:-1;st.sortKey=k;render();});
 document.querySelectorAll(".chip").forEach(ch=>ch.classList.toggle("on",st.sel.has(ch.dataset.c)));
}

// controls
const y0=document.getElementById("y0"),y1=document.getElementById("y1"),hl=document.getElementById("hl");
years.forEach(y=>{y0.add(new Option(y,y));y1.add(new Option(y,y));});y1.value=st.y1;
CODES.forEach(c=>hl.add(new Option(NAME[c],c)));
y0.onchange=()=>{st.y0=+y0.value;if(st.y0>st.y1){st.y1=st.y0;y1.value=st.y1;}render();};
y1.onchange=()=>{st.y1=+y1.value;if(st.y1<st.y0){st.y0=st.y1;y0.value=st.y0;}render();};
hl.onchange=()=>{st.hl=hl.value;st.sel.add(st.hl);render();};
document.getElementById("metric").onchange=e=>{st.metric=e.target.value;render();};
document.getElementById("chips").innerHTML=CODES.map(c=>`<span class="chip on" data-c="${c}">${NAME[c]}</span>`).join("");
document.querySelectorAll(".chip").forEach(ch=>ch.onclick=()=>{const c=ch.dataset.c;if(st.sel.has(c)&&st.sel.size>1)st.sel.delete(c);else st.sel.add(c);
  if(!st.sel.has(st.hl)){st.hl=sel()[0];hl.value=st.hl;}render();});
document.getElementById("all").onclick=()=>{CODES.forEach(c=>st.sel.add(c));render();};
document.getElementById("insights").innerHTML="<h3>Insight &amp; khuyến nghị (cả giai đoạn, sinh tự động từ bảng Gold)</h3>"+
  Object.entries(D.questions).map(([q,t])=>`<h4>${q}. ${t}</h4><ul>${D.insights[q].map(s=>`<li>${s}</li>`).join("")}</ul>`).join("")+
  `<h4>Khuyến nghị</h4><ul>${D.recs.map(s=>`<li>${s}</li>`).join("")}</ul>`;
render();
document.getElementById("c_rank").on("plotly_click",ev=>{const c=ev.points[0].customdata;if(c&&c!=="VNINDEX"){st.hl=c;hl.value=c;render();}});
</script></body></html>
"""


def main() -> int:
    g = insights.load_gold(config.GOLD_CSV_DIR)
    if "sector_kpi" not in g:
        print(f"[dashboard] Chưa có bảng Gold trong {config.GOLD_CSV_DIR}. Chạy src/spark_pipeline.py trước.")
        return 1
    payload = json.dumps(build_payload(g), ensure_ascii=False, separators=(",", ":"))
    html = TEMPLATE.replace("__PLOTLY__", get_plotlyjs()).replace("__DATA__", payload)
    config.DASHBOARD_HTML.parent.mkdir(parents=True, exist_ok=True)
    config.DASHBOARD_HTML.write_text(html, encoding="utf-8")
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (config.REPORTS_DIR / "insights_auto.md").write_text(insights.to_markdown(g), encoding="utf-8")
    print(f"[dashboard] {config.DASHBOARD_HTML} ({config.DASHBOARD_HTML.stat().st_size / 1e6:.1f} MB)")
    print(f"[dashboard] {config.REPORTS_DIR / 'insights_auto.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
