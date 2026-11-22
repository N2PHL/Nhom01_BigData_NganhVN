# Dàn ý báo cáo (đúng 6 chương theo đề) và slide

Độ dài gợi ý 12–15 trang. Mọi con số lấy từ file sinh ra sau khi chạy pipeline — **không gõ tay số liệu**:
`reports/pipeline_metrics.json` (Ch.3), `data/gold/csv/*.csv` và `reports/insights_auto.md` (Ch.4, 6).

## Chương 1. Tổng quan bài toán (1–1,5 trang)
- Bối cảnh: nhà đầu tư phân bổ vốn theo ngành trên TTCK Việt Nam; vì sao cần so sánh ngành theo hiệu quả *đã điều chỉnh rủi ro* thay vì chỉ nhìn lợi suất.
- Mục tiêu và 5 câu hỏi Q1–Q5 (bảng trong README).
- Giá trị: gợi ý tỷ trọng ngành lõi / chu kỳ, theo dõi dòng tiền, đa dạng hóa.
- Phạm vi: 81 mã, 11 ngành ICB cấp 2, 01/2017 → thời điểm chạy, dữ liệu ngày.

## Chương 2. Dữ liệu và công nghệ (1,5–2 trang)
- Nguồn: API DNSE (URL endpoint, tham số `symbol/from/to/resolution`), `universe.csv`. Mức độ tin cậy: dữ liệu khớp lệnh từ CTCK; hạn chế: chưa rõ giá đã điều chỉnh quyền hay chưa → xử lý ở DQ-09.
- Schema thô (JSON t/o/h/l/c/v) → schema Silver (bảng cột trong `spark_pipeline.py`).
- Đặc trưng Big Data: số file, số dòng (từ `pipeline_metrics.json`), nhiều nguồn, chuỗi thời gian; lý do dùng Spark.
- Công nghệ: PySpark, Parquet, Streamlit, Plotly, dashboard BI (HTML/Power BI).
- Sơ đồ kiến trúc (sơ đồ mermaid trong README → xuất ảnh).

## Chương 3. Xử lý dữ liệu với PySpark (3 trang)
- 3.1 Đọc dữ liệu: `StructType`, `recursiveFileLookup`, `arrays_zip` + `explode`, chuyển epoch → ngày giờ VN.
- 3.2 Chất lượng dữ liệu: **bảng `data_quality_report.csv`** (12 quy tắc, số vi phạm, cách xử lý). Giải thích 3–5 ví dụ trong `abnormal_events.csv` (tra lịch sự kiện quyền/chuyển sàn của mã đó).
- 3.3 Làm sạch & tích hợp: dedup `row_number`, broadcast join ngành, join VN-Index theo ngày, chế độ thị trường MA200.
- 3.4 Biến đổi bằng Window: công thức lợi suất, chỉ số tài sản, drawdown, beta 250 phiên, z-score thanh khoản.
- 3.5 Gold: liệt kê 10 bảng, ví dụ 1 câu Spark SQL, self-join tương quan; trích 5–10 dòng `spark_plan_sector_kpi.txt` chỉ ra BroadcastHashJoin / Window / Exchange.
- 3.6 Bảng số dòng qua từng bước (Bronze → Silver → Gold).

## Chương 4. Phân tích và kết quả (4 trang) — mỗi câu hỏi: 1 biểu đồ + 1 bảng + 2–3 insight
- EDA tổng quan: số mã theo ngành, phân phối lợi suất ngày, thanh khoản theo năm.
- Q1: bar Sharpe/CAGR + heatmap lợi suất theo năm.
- Q2: scatter upside/downside capture + bar theo chế độ thị trường.
- Q3: heatmap tỷ trọng giá trị giao dịch theo quý.
- Q4: ma trận tương quan.
- Q5: scatter rủi ro–lợi suất từng mã + top mã mỗi ngành.

## Chương 5. Dashboard và ứng dụng Streamlit (2 trang)
- Dashboard: bố cục, KPI, bộ lọc, các visual liên kết; ảnh chụp; cách dùng.
- Streamlit: 7 tab, bộ lọc giai đoạn/ngành, ảnh chụp, lệnh chạy.

## Chương 6. Kết luận (1 trang)
- Trả lời ngắn Q1–Q5 (từ `insights_auto.md`).
- Khuyến nghị dựa trên dữ liệu.
- Hạn chế: lợi suất ngành bình quân đều (chưa theo vốn hóa); chưa tính phí/thuế giao dịch, cổ tức tiền mặt;
  sàn niêm yết lấy theo hiện tại (mã chuyển sàn có thể bị gắn cờ biên độ sai); 6–8 mã/ngành chưa đại diện toàn ngành;
  chế độ thị trường dựa trên 1 quy tắc MA200.
- Hướng phát triển: mở rộng toàn thị trường / dữ liệu phút, tỷ trọng theo vốn hóa, dữ liệu BCTC có nguồn rõ ràng, chạy trên cụm.

## Phụ lục
- **A. Phân công** (tối đa 4 người; ghi phần việc kiểm chứng được — ví dụ):

| Thành viên | MSSV | Phần việc | File chính |
|---|---|---|---|
| … | … | Thu thập dữ liệu DNSE, danh mục ngành, Ch.1–2 | `fetch_dnse.py`, `universe.csv` |
| … | … | Pipeline PySpark, DQ, Ch.3 | `spark_pipeline.py`, `tests/` |
| … | … | Phân tích Q1–Q5, insight, Ch.4 & 6 | `insights.py`, `reports/` |
| … | … | Dashboard BI + Streamlit, slide, Ch.5 | `build_dashboard.py`, `app/`, `.pbix` |

- **B. Mô tả file & hướng dẫn chạy:** copy mục 4–5 README.
- **C. Khai báo GenAI (mục 4 đề bài) — mẫu:**
  > Nhóm sử dụng Claude (Anthropic) để: rà soát lỗi phương pháp của phiên bản trước, viết lại khung mã nguồn
  > pipeline PySpark, ứng dụng Streamlit và dashboard. Nhóm đã chạy lại toàn bộ pipeline trên dữ liệu DNSE thật,
  > kiểm tra kết quả, viết phần phân tích, và chịu trách nhiệm giải thích mọi bước. [Ghi thêm công cụ khác nếu có.]

## Slide (10–12 trang, 10–12 phút)
1. Tên đề tài, nhóm · 2. Bài toán & 5 câu hỏi · 3. Dữ liệu & lý do dùng Spark · 4. Kiến trúc pipeline ·
5. PySpark: DQ (bảng 12 quy tắc) · 6. PySpark: window/join/Spark SQL + số dòng qua từng bước ·
7. Q1 · 8. Q2 · 9. Q3 + Q4 · 10. Q5 + khuyến nghị · 11. Demo dashboard & Streamlit · 12. Hạn chế, hướng phát triển.
