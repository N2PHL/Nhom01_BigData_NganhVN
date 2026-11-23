# Báo cáo cập nhật phiên bản nâng cao — Đề tài Nhóm 01

**Dự án:** Phân tích hiệu quả, rủi ro & dòng tiền 11 ngành trên TTCK Việt Nam bằng PySpark  
**Streamlit Live:** https://nhom01bigdatanganhvn.streamlit.app/  
**GitHub Repo:** https://github.com/N2PHL/Nhom01_BigData_NganhVN  
**Ngày cập nhật:** 23/11/2026

---

## 1. Tóm tắt lý do nâng cấp (bám sát Rubric 100 điểm)

Phiên bản trước đã hoàn chỉnh toàn bộ luồng cơ bản (`DNSE → PySpark → 10 bảng Gold → Dashboard HTML → Streamlit 7 tab`). Tuy nhiên, để tối ưu điểm tuyệt đối ở **Tiêu chí 2 (PySpark — 25đ)**, **Tiêu chí 3 (Phân tích & Insight — 20đ)**, **Tiêu chí 5 (Streamlit — 15đ)** và **Tiêu chí 7 (Tái lập & Sáng tạo — 5đ)** dưới góc độ định lượng thực chiến tại thị trường Việt Nam, bản cập nhật này bổ sung:

1. **Kiểm chứng Size Effect:** So sánh lợi suất ngành *Bình quân đều (Equal-Weight)* vs *Gia quyền thanh khoản (Liquidity-Weighted, lag 20 phiên)*.
2. **Rủi ro đuôi & Cấu trúc vi mô TTCK Việt Nam:** CVaR 95% (Expected Shortfall), Calmar Ratio, Skewness/Kurtosis, Thời gian về bờ lâu nhất (*Time-to-Recovery*), Chỉ số kém thanh khoản *Amihud* và tần suất chạm biên độ trần/sàn.
3. **Econophysics (Q4):** Tương quan có điều kiện (*Conditional / Tail Correlation*: Bình thường vs Hoảng loạn `VN-Index ≤ -1,5%`) và chỉ số rủi ro hệ thống **Absorption Ratio** từ *Random Matrix Theory* (trượt 120 phiên).
4. **Kiểm định Lead–Lag Dòng tiền (Q3):** Phân tách 5 phân vị đột biến thanh khoản (`flow_z60`) để kiểm định dòng tiền là *Smart Money dẫn dắt* hay *Retail FOMO đu đỉnh*.
5. **PySpark MLlib (Tiêu chí 7 — điểm cộng ML):** Phân cụm không giám sát **K-Means (3 chế độ thị trường)** trên 4 đặc trưng (*Lợi suất 20p, Biến động 20p, Market Breadth MA50, Absorption Ratio*).
6. **Portfolio Lab (Tab thứ 8 trên Streamlit):** Backtest 4 chiến lược phân bổ ngành có tính **phí giao dịch + thuế bán TTCK Việt Nam (mặc định 0,25%/chiều)** và tần suất tái cân bằng (không look-ahead bias).

---

## 2. Chi tiết thay đổi theo từng file mã nguồn

| File | Bản cũ | Bản mới (đã kiểm thử) |
|---|---|---|
| `src/spark_pipeline.py` | 10 bảng Gold + `data_quality_report`; chỉ dùng Equal-Weight, MA200, Pearson tĩnh | **14 bảng Gold** + `data_quality_report`; thêm đặc trưng Silver (`amihud`, `is_limit_hit`, `avg_val_20d_lag`, `above_ma50`), thêm `cagr_lw`, `sharpe_lw`, `cvar_95`, `calmar`, `skewness`, `excess_kurtosis`, `max_underwater_days`, **PySpark MLlib K-Means**, **Absorption Ratio (RMT)**, và 4 bảng mới (`sector_corr_regime`, `systemic_risk_daily`, `ml_regime_sector`, `flow_lead_lag`). |
| `src/insights.py` & `reports/insights_auto.md` | Insight cơ bản cho Q1–Q5 | Tự động sinh thêm các insight định lượng mới cho Q1 (Size Effect, CVaR, Time-to-Recovery), Q2 (MLlib K-Means, chạm trần/sàn), Q3 (Lead–Lag dòng tiền), Q4 (Tail Correlation, Absorption Ratio). |
| `app/streamlit_app.py` | 7 tab; lỗi `KeyError` khi chọn khoảng ngày cuối tuần; nhãn cột Q1 hơi sát lề phải | **8 tab** (thêm `Portfolio Lab (Backtest VN)`); thêm công tắc **EW vs LW** ở Sidebar; nâng cấp biểu đồ Q1, Q2 (MLlib), Q3 (Lead–Lag), Q4 (4 chế độ tương quan + RMT); đã vá lỗi khoảng ngày trống & cảnh báo Pandas 3.x. |
| `tests/test_pipeline.py` | 3 unit tests | **5 unit tests (5/5 PASS)** — bổ sung kiểm thử `max_underwater_days`, `cvar_95`, `amihud`, `is_limit_hit`, `avg_val_20d_lag`. |
| `.gitignore` | Bỏ `data/raw/`, `data/silver/`, `data/gold/parquet/` | Bổ sung bỏ `.venv/`, `.Rhistory`, `.DS_Store`, `*.zip` (vẫn giữ `data/gold/csv/` để deploy Cloud). |

---

## 3. Các phát hiện (Insight) mới nổi bật để cập nhật vào Báo cáo PDF & Slide

Nhóm nên đưa **5 kết quả mới** dưới đây vào **Chương 4 (Phân tích & Kết quả)** và **Slide thuyết trình** (số liệu lấy trực tiếp từ `reports/insights_auto.md`):

1. **Hiệu ứng quy mô (Size Effect — Q1):** Khi chuyển từ *Bình quân đều (EW)* sang *Gia quyền thanh khoản (LW, lag 20p)*, số ngành vượt VN-Index giảm từ **9/11 xuống 7/11**. Chênh lệch lớn nhất ở **Thực phẩm & Đồ uống** (**EW 12,0% vs LW chỉ 2,0%**) — cho thấy tăng trưởng rổ EW ngành này chủ yếu đến từ cổ phiếu nhỏ/vừa, không phải nhóm đầu ngành thanh khoản lớn.
2. **Rủi ro đuôi & Thời gian về bờ (Q1):** **Dầu khí** có CVaR 95% sâu nhất (**-6,0%/phiên**); **Tài nguyên cơ bản** có thời gian chìm dưới đỉnh lâu nhất (**1.262 phiên liên tiếp ≈ 5 năm**).
3. **Phân cụm PySpark MLlib K-Means & Hiệu ứng trần/sàn (Q2):** Trong cụm *ML: Hoảng loạn / Rủi ro cao*, **Hàng & Dịch vụ công nghiệp** phòng thủ tốt nhất (**+30,8%/năm** so với VN-Index), còn **Dịch vụ tài chính** giảm mạnh nhất (**-22,6%/năm**) và có tần suất chạm biên độ trần/sàn cao nhất (**4,90%** số phiên).
4. **Kiểm định Lead–Lag Dòng tiền (Q3):** Khi dòng tiền ngành bùng nổ (nhóm Q5 `flow_z60`), lợi suất bình quân **20 phiên sau đạt +2,3%** và **60 phiên sau đạt +5,9%**, cao gấp đôi nhóm cạn thanh khoản Q1 (**+1,0%** và **+3,0%**) → chứng minh dòng tiền cấp ngành có tính **Smart Money dẫn dắt theo sóng (Sector Momentum)**.
5. **Tail Dependence & Rủi ro hệ thống RMT (Q4):** Khi VN-Index giảm sốc (`≤ -1,5%`), tương quan trung bình giữa các ngành nhảy từ **0,46 lên 0,60**. Cặp tăng tương quan mạnh nhất khi sập là **Hóa chất – Hàng & Dịch vụ công nghiệp** (`0,53 → 0,78`); cặp giữ đa dạng hóa tốt nhất khi sập là **Ngân hàng – Hóa chất** (`0,36`). Chỉ số **Absorption Ratio (120p)** đạt đỉnh **81,6%** ngày **07/08/2020** (sốc COVID-19).

---

## 4. Checklist việc cần làm cho các thành viên trước khi nộp bài

- [ ] **Thành viên phụ trách Báo cáo PDF:** Cập nhật thêm mục PySpark MLlib (Chương 3), 5 insight mới trên (Chương 4), ảnh chụp màn hình Tab `Portfolio Lab` và biểu đồ `Absorption Ratio` (Chương 5), và dán link live https://nhom01bigdatanganhvn.streamlit.app/ + link GitHub vào Phụ lục.
- [ ] **Thành viên phụ trách Slide & Demo:** Chuẩn bị demo trực tiếp trên https://nhom01bigdatanganhvn.streamlit.app/ — nhấn mạnh 3 điểm ăn điểm sáng tạo: công tắc **EW vs LW**, ma trận **Tail Correlation + Absorption Ratio**, và tab **Portfolio Lab (Backtest có phí/thuế VN)**.
- [ ] **Thành viên phụ trách Khai báo GenAI & Nộp bài:** Cập nhật mục khai báo sử dụng GenAI trong báo cáo; đổi tên file nén `.zip` đúng quy định `Nhom01_DeTaiCK_HoTen_MaSV.zip` và gộp thêm **Báo cáo PDF + Slide thuyết trình** vào trước khi nộp.
