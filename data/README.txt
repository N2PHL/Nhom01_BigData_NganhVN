Dữ liệu kèm theo:
- raw/dnse/: 803 file JSON gốc từ API DNSE (lấy ngày 22/11/2026 trên Google Colab) + fetch_log.csv
- gold/: 10 bảng KPI + data_quality_report do src/spark_pipeline.py tạo (CSV + Parquet)
Silver (Parquet, ~22 MB) không kèm: chạy lại python src/spark_pipeline.py để tạo.
