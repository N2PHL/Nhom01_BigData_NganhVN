# Phản biện bài nộp cũ "L.U.C.A Quant v2.0 — BigData Submission" và cách đã sửa

Bản rà soát đối chiếu bài cũ với đề "Phân tích dữ liệu lớn trong đầu tư và kinh doanh – Đề tài cuối kỳ".
Mức độ: **🔴 Nghiêm trọng** (có thể bị đánh giá không đạt yêu cầu tối thiểu hoặc vi phạm trung thực học thuật) ·
**🟠 Lỗi kỹ thuật** (sai kết quả/khái niệm) · **🟡 Lệch trọng tâm** (tốn công nhưng không ăn điểm rubric).

---

## A. 🔴 Lỗi nghiêm trọng

| # | Vấn đề | Bằng chứng trong bài cũ | Đã sửa |
|---|---|---|---|
| A1 | **Dữ liệu "156.660 dòng của 60 mã HOSE/HNX" là dữ liệu giả lập**, nhưng tóm tắt, Chương 1, slide, dashboard trình bày như dữ liệu thị trường thật. | `ensure_raw_ohlcv_60tickers()` sinh giá bằng `SyntheticProvider(mode="momentum", seed=42)`; `sector_metadata.csv` có cột `annual_drift`, `annual_vol`, `signal_strength` do nhóm tự đặt; VCB ngày 31/08/2016 có giá tròn 20.000đ; `ticker_summary_kpis.csv` có cột `signal_strength_true`. Phụ lục A gắn kết quả giả lập với tên doanh nghiệp thật (FPT, DGC, REE…). | Toàn bộ dữ liệu lấy thật từ API DNSE (`src/fetch_dnse.py`), lưu nguyên văn phản hồi API + URL + thời điểm lấy (`fetch_log.csv`) để kiểm chứng. Không còn dòng giả lập nào trong bài. |
| A2 | **Lỗi dữ liệu được "tiêm" có chủ đích rồi pipeline "phát hiện" đúng con số đã tiêm** (120 trùng, 90 thiếu close, 150 thiếu volume, 109 vượt biên). Kiểm định chất lượng như vậy không chứng minh được gì. | Đoạn "Tiêm đúng các lỗi dữ liệu chuẩn mực…" trong `pyspark_luca_quant_pipeline.py`. | 12 quy tắc DQ chạy trên dữ liệu thật; con số vi phạm là kết quả đo được, kèm danh sách phiên bất thường (`abnormal_events.csv`) để giải thích. |
| A3 | **Nguồn macro và BCTC không rõ ràng, không có mã thu thập** (lãi suất liên ngân hàng, USD/VND, 2.460 dòng BCTC quý, ngày công bố "trễ 28–45 ngày"). Vốn hóa VCB quý III/2016 = 25.703 tỷ rồi quý sau còn 12.334 tỷ: phi thực tế. Lịch giao dịch dùng ngày làm việc (2.609 phiên) gồm cả ngày nghỉ Tết. | `corporate_fundamentals_quarterly.parquet`, `macro_vnindex_daily.parquet`. | Bỏ các nguồn không kiểm chứng được. Chỉ dùng 3 nguồn có thể truy vết: API DNSE cổ phiếu, API DNSE chỉ số, file phân ngành ICB. Lịch giao dịch lấy từ chính VN-Index. |
| A4 | **Spark không thực sự tạo ra kết quả trong báo cáo.** Khi máy không có Java, script tự chuyển sang `run_verified_analytical_verification()`: đếm bằng Pandas, kiểm tra file CSV có tồn tại rồi in "thành công". Mục *Giới hạn 4* của báo cáo thừa nhận chạy bằng "bộ máy tương thích PyArrow/Pandas". Các bảng KPI hiển thị (`ticker_summary_kpis`, `sector_regime_kpis`, `cost_sensitivity_matrix`…) không do Spark tạo. Vi phạm mục 12 đề bài: *"không chỉ đọc file bằng Spark rồi chuyển toàn bộ sang Pandas"*. | `main()` → `if has_pyspark(): … else: run_verified_analytical_verification()`. | Pipeline chỉ có nhánh PySpark, không có fallback. Mọi bảng Gold mà dashboard/Streamlit dùng đều do Spark ghi ra; số dòng mỗi bước ghi vào `reports/pipeline_metrics.json`, kế hoạch thực thi vào `reports/spark_plan_sector_kpi.txt`. |
| A5 | **Nhóm 5 thành viên, đề cho tối đa 4.** Bảng phân công ghi mỗi người "100%". | Trang bìa, Phụ lục C. | ⚠ Nhóm cần tự xử lý với giảng viên (xin phép hoặc tách nhóm). Bảng phân công mẫu trong `docs/DAN_Y_BAO_CAO.md` ghi phần việc cụ thể, kiểm chứng được. |
| A6 | Kết quả "DNSE thật" (VIC, VND) được **gõ cứng** trong trang Streamlit số 8 và CSV, không có mã tái lập. | `pages/8_…py` dòng 138: `{"Ticker": "VND (DNSE Real)", …}`. | Mọi con số trên dashboard/app/insight sinh tự động từ bảng Gold (`src/insights.py`). |

## B. 🟠 Lỗi kỹ thuật trong pipeline

| # | Vấn đề | Đã sửa |
|---|---|---|
| B1 | DQ-04 "sửa" phiên vượt biên độ bằng `close = open × (1 ± 6,5%)` → **bịa ra giá không có thật**. Với dữ liệu thật, bước nhảy lớn thường do phiên chào sàn/chuyển sàn, giai đoạn mã còn ở UPCoM/HNX hoặc mở lại sau tạm ngừng. | Giữ nguyên giá gốc. DQ-09a: vượt biên độ sàn hiện tại → chỉ gắn cờ, vẫn tính. DQ-09b: lợi suất > 15,5% → loại khỏi thống kê. Tất cả liệt kê trong `abnormal_events`. |
| B2 | Mỗi lần chạy, script **đọc rồi ghi đè chính các file Parquet đầu vào** (đổi kiểu timestamp) → phá nguyên tắc dữ liệu thô bất biến. | Kiến trúc Bronze → Silver → Gold; lớp Bronze (JSON DNSE) chỉ đọc. |
| B3 | Điền giá thiếu bằng forward-fill rồi mới tính lợi suất → tạo lợi suất 0% giả. | Không nội suy giá; lợi suất tính giữa 2 phiên có thật, phiên mở lại sau > 10 ngày bị loại. |
| B4 | `gross_sharpe` theo ngành = trung bình/độ lệch chuẩn của **toàn bộ lợi suất các mã gộp lại** — không phải biến động của danh mục ngành; `AVG(pe_ratio)` trung bình trực tiếp P/E. | Lợi suất ngành = bình quân đều các mã **theo từng ngày** (`sector_daily`), rồi mới tính CAGR, độ biến động, Sharpe, Sortino, MaxDD, beta. |
| B5 | Docstring ghi RSI Wilder nhưng code Spark dùng trung bình đơn giản; 33 "đặc trưng" tạo ra nhưng không câu hỏi nào dùng. | Chỉ tạo đặc trưng phục vụ câu hỏi (lợi suất, chỉ số tài sản, drawdown, biến động 20 phiên, beta 250 phiên, z-score thanh khoản, chế độ thị trường). |
| B6 | Chế độ thị trường tính bằng window `partitionBy("ticker")` trên cột VN-Index (lặp 60 lần, sai ở mã niêm yết muộn). | Tính 1 lần trên bảng chỉ số, rồi join theo ngày. |
| B7 | `percent_rank` với giá trị NULL bị xếp đầu → méo thứ hạng. Báo cáo ghi điền volume bằng "median 20 phiên" nhưng code dùng `avg`. | Bỏ phần xếp hạng không dùng; mô tả trong tài liệu khớp code. |
| B8 | Chưa xử lý khác biệt Spark 3.5 / 4.x (Spark 4 bật ANSI: chia cho 0 làm sập job). | Cố định `spark.sql.ansi.enabled=false` và chặn mẫu số 0 ở các phép chia; có `pytest` kiểm thử. |

## C. 🟡 Lệch trọng tâm đề bài

| # | Vấn đề | Đã sửa |
|---|---|---|
| C1 | Phần lớn công sức (34 mô hình ML/DL, DSR, cổng 9 bước, ablation 63 tổ hợp, torch) thuộc đồ án cũ L.U.C.A v1 phân tích **1 mã**, không nằm trong luồng Data → PySpark → Analytics. Đề ghi ML là *tùy chọn*; rubric dồn 45/100 điểm vào pipeline Spark + phân tích. | Bỏ toàn bộ gói `luca_quant/` và 7 trang Streamlit cũ. Project chỉ còn ~1.500 dòng code, mỗi file phục vụ 1 bước của luồng bắt buộc. |
| C2 | RQ1–RQ5 là câu hỏi phương pháp luận về mô hình; H1/H3 kiểm định trên dữ liệu giả lập mà chính nhóm cài tín hiệu → vòng lặp tự chứng minh. | 5 câu hỏi đầu tư cụ thể, trả lời trực tiếp bằng bảng Gold (xem README). |
| C3 | Insight không gắn với output Spark (VD: nhận định về nâng hạng FTSE không có số liệu nào trong pipeline chứng minh). | Insight sinh từ số liệu (`reports/insights_auto.md`); nhận định vĩ mô nếu đưa vào báo cáo phải có trích dẫn. |
| C4 | Báo cáo tự nhận "Power BI" nhưng chỉ có HTML; không có `.pbix`. | Dashboard HTML tương tác (BI tương đương) sinh từ Gold + hướng dẫn dựng `.pbix` từ cùng CSV (`docs/POWER_BI.md`). Nên nộp thêm `.pbix`. |
| C5 | Streamlit trang 1–7 gọi API DNSE và chạy mô hình lúc demo (dễ lỗi mạng, nặng torch); chỉ trang 8 đọc output pipeline. | 1 app, 7 tab, chỉ đọc bảng Gold; bộ lọc giai đoạn/ngành tính lại KPI ngay. |
| C6 | Văn phong quảng cáo, tự chấm "đáp ứng 100% rubric", so sánh "Jane Street, Citadel"; 15 trang nhưng ít trang trả lời câu hỏi bằng dữ liệu. | Dàn ý báo cáo mới bám đúng 6 chương của đề (`docs/DAN_Y_BAO_CAO.md`). |
| C7 | Tên file nộp không theo quy định `Nhom??_DeTaiCK_HoTen_MaSV`. | Ghi chú trong README: đổi tên file zip trước khi nộp. |
| C8 | Khai báo GenAI chỉ nêu Gemini. | Bản sửa này có dùng Claude (Anthropic) → **phải khai báo** theo mục 4 đề bài (mẫu trong dàn ý). |

## D. Lỗi của chính bản sửa, phát hiện khi chạy trên dữ liệu thật (đã sửa)

| # | Vấn đề | Cách phát hiện | Đã sửa |
|---|---|---|---|
| D1 | Lần chạy đầu loại 268 phiên hợp lệ vì so lợi suất với biên độ của sàn **hiện tại**, trong khi TDM, BSR, VTP, SCS… từng niêm yết UPCoM (±15%). CAGR ngành Hàng & DV công nghiệp bị kéo thấp ~2,6 điểm %/năm, Điện nước ~1,9 điểm %. | Đối chiếu danh sách `abnormal_events` và ước lượng ảnh hưởng lên lợi suất ngành. | DQ-09a (gắn cờ) / DQ-09b (> 15,5% mới loại). Giá DNSE đã điều chỉnh theo sự kiện quyền nên an toàn. |
| D2 | CAGR tính theo số phiên × 252; nguồn VN-Index thiếu 35 ngày nên CAGR VN-Index bị đẩy lên 10,1% thay vì 9,8%. | So CAGR theo phiên với CAGR theo năm lịch từ giá đầu/cuối. | CAGR theo năm lịch (`365,25 / DATEDIFF`); thêm DQ-13 báo ngày thiếu VN-Index. |
| D3 | Khuyến nghị tự động xếp Ngân hàng vào nhóm "hạ tỷ trọng khi thị trường giảm" chỉ vì beta > 1, trong khi Ngân hàng vẫn vượt VN-Index 12,4%/năm khi VN-Index dưới MA200. | Đối chiếu khuyến nghị với bảng `sector_regime`. | Phân nhóm theo hiệu quả thực tế ở từng chế độ thị trường (`src/insights.py`). |
| D4 | Spark 4 bật ANSI mặc định: chia cho 0 làm sập job (Spark 3.5 trả NULL). | `pytest`. | `spark.sql.ansi.enabled=false` + chặn mẫu số 0. |
