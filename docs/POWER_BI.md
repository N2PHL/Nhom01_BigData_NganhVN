# Dựng dashboard Power BI (.pbix) từ bảng Gold

Rubric ưu tiên Power BI/Tableau. Dashboard HTML trong `dashboard/` là bản "BI tương đương"; nếu nhóm có
Power BI Desktop (Windows), làm thêm file `.pbix` theo các bước dưới (~45 phút) và nộp kèm.

## 1. Nạp dữ liệu
`Get Data → Text/CSV` lần lượt các file trong `data/gold/csv/` (UTF-8 BOM nên tiếng Việt hiển thị đúng):

| Bảng | Vai trò | Ghi chú kiểu dữ liệu |
|---|---|---|
| `sector_kpi` | dimension ngành + KPI cả giai đoạn | các cột tỷ lệ → Percentage |
| `sector_daily` | fact theo ngày × ngành | `date` → Date |
| `market_daily` | VN-Index theo ngày | `date` → Date |
| `sector_yearly`, `sector_regime`, `liquidity_quarterly`, `sector_corr` | fact tổng hợp | |
| `ticker_kpi`, `ticker_monthly` | mã cổ phiếu | `month` → Date |
| `data_quality_report` | trang chất lượng dữ liệu | |

## 2. Mô hình quan hệ (Model view)
- Tạo bảng ngày: `Dim_Date = CALENDAR(MIN(sector_daily[date]), MAX(sector_daily[date]))`, thêm cột `Year = YEAR([Date])`.
- `Dim_Date[Date]` 1 → * `sector_daily[date]`, 1 → * `market_daily[date]`.
- `sector_kpi[sector_code]` 1 → * `sector_daily`, `sector_yearly`, `sector_regime`, `liquidity_quarterly`, `ticker_kpi` (cùng cột `sector_code`).
- Lọc bỏ dòng `VNINDEX` khỏi slicer ngành (filter visual: `sector_code <> "VNINDEX"`).

## 3. Measure DAX (tính lại theo bộ lọc thời gian)
```DAX
Số ngày = COUNTROWS ( FILTER ( sector_daily, NOT ISBLANK ( sector_daily[sector_ret] ) ) )

CAGR ngành =
VAR n = [Số ngày]
VAR w = PRODUCTX ( FILTER ( sector_daily, NOT ISBLANK ( sector_daily[sector_ret] ) ), 1 + sector_daily[sector_ret] )
RETURN IF ( n > 20, POWER ( w, 252 / n ) - 1 )

Biến động năm = STDEVX.S ( FILTER ( sector_daily, NOT ISBLANK ( sector_daily[sector_ret] ) ), sector_daily[sector_ret] ) * SQRT ( 252 )

Sharpe = DIVIDE ( AVERAGE ( sector_daily[sector_ret] ) * 252 - 0.03, [Biến động năm] )

CAGR VN-Index =
VAR n = COUNTROWS ( FILTER ( market_daily, NOT ISBLANK ( market_daily[mkt_ret] ) ) )
VAR w = PRODUCTX ( FILTER ( market_daily, NOT ISBLANK ( market_daily[mkt_ret] ) ), 1 + market_daily[mkt_ret] )
RETURN IF ( n > 20, POWER ( w, 252 / n ) - 1 )

Vượt VN-Index = [CAGR ngành] - [CAGR VN-Index]
```
(Lãi suất phi rủi ro 0.03 phải khớp `RF_ANNUAL` trong `src/config.py`.)

## 4. Bố cục gợi ý (1 trang chính + 1 trang chất lượng dữ liệu)
- **Slicer:** khoảng thời gian (`Dim_Date[Date]`), ngành (`sector_kpi[sector_name]`), sàn (`ticker_kpi[exchange]`).
- **Thẻ KPI:** CAGR VN-Index · Sharpe ngành cao nhất · số ngành vượt VN-Index · tổng giá trị giao dịch.
- **Visual:**
  1. Line chart `sector_daily[sector_index]` theo `date` (legend = ngành; dùng *Highlight* thay vì 11 màu).
  2. Bar chart ngang `Sharpe` / `CAGR ngành` theo ngành (Q1).
  3. Matrix `sector_yearly`: hàng = ngành, cột = năm, giá trị `annual_return`, conditional formatting đỏ–xám–xanh (Q1).
  4. Scatter `sector_kpi`: x = `downside_capture`, y = `upside_capture` (Q2) + clustered bar `sector_regime` (Q2).
  5. Matrix/heatmap `liquidity_quarterly[value_share]` hàng = ngành, cột = quý (Q3).
  6. Matrix `sector_corr` (Q4).
  7. Scatter `ticker_kpi`: x = `ann_vol`, y = `cagr`, tooltip ticker (Q5).
- Bật *Edit interactions* để click 1 ngành ở bar chart lọc các visual còn lại.
- Chụp ảnh màn hình đưa vào Chương 5 báo cáo.
