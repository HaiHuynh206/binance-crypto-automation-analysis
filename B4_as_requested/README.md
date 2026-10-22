# B4_as_requested — MFF4030 đề B4 (market quality)

Phân tích chất lượng thị trường (Amihud, Roll, OFI, VR) dùng **lake Parquet** do pipeline Airflow trong repo này publish lên R2.

Không đụng code ingest core (`src/crypto_ingest/`). Folder này độc lập: metrics + notebook + báo cáo.

## Chạy nhanh

```bash
cd B4_as_requested
cp .env.example .env   # điền AIRFLOW_* và AWS_* (R2 read-only)
pip install -r requirements.txt
python -m src.r2_lake --dataset klines_1m --limit 10
python -m src.compute_from_r2 --symbols BTCUSDT,ETHUSDT --date-from 2026-10-19 --date-to 2026-10-21
```

Notebook: `notebooks/B4_phan_tich_chat_luong_tt.ipynb`  
Báo cáo: `docs/B4_bao_cao.md`

## Quan hệ với platform

| Platform (repo gốc) | B4_as_requested |
|---------------------|-----------------|
| DAG `crypto_market_ingest` → R2 lake | Đọc lake → metrics B4 |
| Không có microstructure metrics | Có Roll/Amihud/OFI/VR + phản biện VN |
