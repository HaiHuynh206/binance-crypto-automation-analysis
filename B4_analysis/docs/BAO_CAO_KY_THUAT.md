# Báo cáo tổng quan kỹ thuật, hạ tầng và công nghệ

**Đề tài:** B4 — Đo chất lượng thị trường từ dữ liệu giao dịch tần suất cao
**Hệ thống:** Binance Crypto Data Platform (repo `binance-crypto-automation-analysis`)
**Số liệu chốt ngày:** 25/10/2026. Mọi con số trong báo cáo được đo trực tiếp từ lake local, R2 hoặc VPS vào ngày này,
trừ phần chi phí (lấy từ báo cáo quản trị `docs/reports/bao-cao-hieu-qua-phong-du-lieu-2026-10.md`).

---

## 1. Tóm tắt

Hệ thống tự động thu thập dữ liệu thị trường tần suất cao từ Binance (BTCUSDT, ETHUSDT), lưu thành Parquet lake
ba tầng **Bronze → Silver → Gold**, kiểm tra chất lượng bằng cổng DQ trước khi dựng tầng phân tích, rồi xuất bản
lên Cloudflare R2 để nhóm và người nhận truy vấn bằng DuckDB hoặc Spark.

| Hạng mục | Giá trị |
|---|---|
| Lịch chạy | Airflow `crypto_market_ingest`, mỗi 2 giờ (`0 */2 * * *` UTC), 12 lần/ngày |
| Hạ tầng | 1 VPS trên GCP (2 vCPU / 4 GB RAM), Docker Compose, Airflow + Postgres |
| Kho chia sẻ | Cloudflare R2, bucket `crypto-lake`, prefix `crypto/` |
| Kỳ dữ liệu backfill | 01/07/2026 – 24/10/2026 (BTCUSDT, ETHUSDT) |
| Quy mô Bronze | khoảng 192 triệu dòng, 3,2 GB trên đĩa local |
| Chi phí hạ tầng | khoảng 8 USD/tháng (theo báo cáo quản trị) |
| Kiểm thử | 37 test đạt, 2 test bỏ qua; `ruff` không có lỗi |

Lần chạy 04:00 UTC ngày 25/10 trên VPS hoàn thành cả 10 task trong khoảng 2 phút 38 giây, trong đó cổng DQ kiểm
280 giờ dữ liệu và cả 280 giờ đều đạt.

---

## 2. Kiến trúc tổng thể

```
Binance REST API
 (klines 1s/1m, aggTrades, funding rate, open interest)
        │
        ▼
 crypto_ingest  ── rate limit + retry/backoff + phân trang
        │
        ▼
 BRONZE   data/lake/exchange=binance/dataset=…/symbol=…/date=…/hour=…/part-00000.parquet
        │            (dữ liệu thô, 1 file / giờ, ghi atomic)
        ▼
 SILVER   data/silver/dataset=…/symbol=…/date=…/part-00000.parquet
        │            (dedupe, đổi tên cột, loại dòng sai, thêm cột market)
        ▼
 DQ gate  dq_hourly: ok / missing / partial / id_gaps  ── fail thì dừng, không dựng Gold
        │
        ▼
 GOLD     data/gold/mart=…/symbol=…/date=…/part-00000.parquet
        │            (ohlcv, market_hourly, derivatives_hourly, data_quality_daily)
        ▼
 DuckDB catalog  ─►  Cloudflare R2 (Bronze + Silver + Gold + _manifest.json)
                           │
                           └─►  notebook phân tích (DuckDB / Spark) và báo cáo B4
```

Sơ đồ dạng hình nằm ở `docs/assets/architecture.png` của repo gốc.

Nguyên tắc thiết kế:

- **Mỗi giờ là một file Parquet.** Ghi atomic (file tạm rồi `os.replace`), nên retry hoặc backfill chỉ ghi đè đúng giờ
  đó, không sinh dữ liệu trùng hay file dở dang.
- **Marker hoàn thành theo từng partition** (`data/_state/completed`). Chạy lại cùng cửa sổ gần như không tốn gì; task
  lỗi thì lần retry chỉ tải phần còn thiếu.
- **Rate limit và retry nằm trong code**, không phụ thuộc Airflow.
- **Không có service query riêng.** DuckDB đọc trực tiếp Parquet, nên Silver và Gold chỉ là SQL chạy trên lake.
- **Upload incremental** dựa trên ETag/MD5, chỉ đẩy file mới hoặc thay đổi.

---

## 3. Nguồn và tập dữ liệu

| Dataset (Bronze) | Endpoint Binance | Tần suất | Ghi chú |
|---|---|---|---|
| `klines_1s` | `GET /api/v3/klines` | 1 giây | khoảng 3.600 dòng/giờ/symbol |
| `klines_1m` | `GET /api/v3/klines` | 1 phút | khoảng 60 dòng/giờ/symbol |
| `agg_trades` | `GET /api/v3/aggTrades` | từng giao dịch gộp | nặng nhất, phân trang theo `fromId` |
| `funding_rate` | `GET /fapi/v1/fundingRate` | 8 giờ/lần | futures |
| `open_interest` | `GET /futures/data/openInterestHist` | 5 phút | API chỉ cho 30 ngày gần nhất; lake có từ 25/09/2026 |

Dữ liệu là dữ liệu thật từ API công khai của Binance, không có dòng mô phỏng. Về giấy phép, đề bài ghi dữ liệu Binance
public data thuộc CC BY-NC-SA 4.0, chỉ dùng cho nghiên cứu và giáo dục.

### Quy mô thực tế (đo trên lake local, kỳ 01/07 – 24/10/2026)

| Tầng | Bảng | Số dòng |
|---|---|---:|
| Bronze | `klines_1s` | 19.951.200 |
| Bronze | `klines_1m` | 332.520 |
| Bronze | `agg_trades` | 171.770.585 |
| Bronze | `funding_rate` | 694 |
| Bronze | `open_interest` | 17.112 |
| Silver | `bars_1s` / `bars_1m` | 19.951.200 / 332.520 |
| Silver | `trades` | 171.770.585 |
| Silver | `funding` / `open_interest` | 694 / 16.392 |
| Silver | `dq_hourly` | 23.594 |
| Gold | `ohlcv` | 72.278 |
| Gold | `market_hourly` | 5.542 |
| Gold | `derivatives_hourly` | 1.344 |
| Gold | `data_quality_daily` | 976 |

Dung lượng trên đĩa: Bronze 3,2 GB (18.748 file Parquet), Silver 2,9 GB, Gold 14 MB. Hai dataset lớn nhất là
`agg_trades` (2,4 GB ở Bronze, 2,2 GB ở Silver) và `klines_1s` (719 MB).

Silver `open_interest` ít hơn Bronze 720 dòng (16.392 so với 17.112). Báo cáo này chưa đối chiếu nguyên nhân chi tiết
(dedupe hoặc dòng bị loại sang quarantine), nên không kết luận.

---

## 4. Tầng thu thập (ingestion)

Gói Python `crypto_ingest` (khoảng 2.700 dòng), cài thành lệnh `crypto-ingest`:

| Module | Vai trò |
|---|---|
| `binance.py` | gọi các endpoint Binance, phân trang |
| `http.py` | HTTP client: giới hạn trọng số theo phút (`X-MBX-USED-WEIGHT-1M`), retry với exponential backoff cho lỗi 5xx, timeout, 429/418 (tôn trọng `Retry-After`); lỗi 4xx khác không retry |
| `pipeline.py` | điều phối ingest theo cửa sổ thời gian, backfill |
| `storage.py` | ghi Parquet (nén **zstd, level 3**), ghi atomic |
| `state.py` | marker hoàn thành theo partition (idempotency) |
| `schemas.py` | schema PyArrow, ngưỡng số dòng tối thiểu mỗi giờ (`MIN_ROWS_PER_HOUR`) |
| `medallion.py` | Silver, DQ, Gold bằng DuckDB SQL |
| `catalog.py` | tạo view DuckDB `bronze_*`, `silver_*`, `gold_*` |
| `s3.py` | đồng bộ lên R2/S3/MinIO, sinh `_manifest.json` |
| `cli.py` | các lệnh `ingest`, `backfill`, `status`, `catalog`, `silver`, `dq`, `gold`, `sync-s3` |

Cấu hình qua biến môi trường: `CRYPTO_SYMBOLS` (mặc định BTCUSDT,ETHUSDT), `CRYPTO_DATASETS`,
`CRYPTO_WEIGHT_PER_MINUTE` (2400), `CRYPTO_LOOKBACK_HOURS` (24).

---

## 5. Kiến trúc Bronze / Silver / Gold

### Silver

| Silver | Từ Bronze | Việc làm |
|---|---|---|
| `bars_1s`, `bars_1m` | `klines_1s`, `klines_1m` | dedupe theo `(symbol, open_time)` giữ bản `ingested_at` mới nhất; thêm `market`, `taker_sell_base/quote` |
| `trades` | `agg_trades` | dedupe theo `(symbol, agg_trade_id)`; thêm `notional`, `aggressor_side` (`is_buyer_maker=true` tương ứng `SELL`) |
| `funding` | `funding_rate` | `mark_price = 0` coi là thiếu, đổi thành `NULL` |
| `open_interest` | `open_interest` | kiểm tra giá trị âm hoặc null |
| `dq_hourly` | do bước DQ sinh ra | trạng thái chất lượng từng giờ |

Dòng vi phạm (giá ≤ 0, `high < low`, `taker_buy > volume`, null…) không bị xoá mà chuyển sang `*_quarantine` kèm
`reject_reason`; quarantine không được đẩy lên R2.

### Cổng chất lượng dữ liệu (DQ)

Mỗi dataset × symbol × giờ được gán một trạng thái: `ok`, `missing` (giờ đã kết thúc nhưng không có dòng),
`partial` (ít hơn ngưỡng tối thiểu), `id_gaps` (`agg_trade_id` nhảy cóc). Biến `CRYPTO_DQ_FAIL_ON` (mặc định
`missing`) quyết định trạng thái nào làm task fail. Khi cổng fail, Gold **không** được dựng; Airflow vẫn refresh
catalog và đẩy Bronze, nhưng không đẩy Silver/Gold.

### Gold

| Mart | Khoá | Nội dung chính |
|---|---|---|
| `ohlcv` | `symbol, interval (5m/1h/1d), bucket` | OHLC, volume, vwap, `n_bars`, `is_complete` |
| `market_hourly` | `symbol, hour_start` | OHLCV giờ, `realized_vol`, `taker_buy_share`, `aggressor_imbalance` |
| `derivatives_hourly` | `symbol, hour_start` | `oi_*` và `last_funding_rate` (ASOF join) |
| `data_quality_daily` | `dataset, symbol` | số giờ ok/missing/partial, `pct_ok` |

Mỗi lần build, một ngày UTC được dựng lại hoàn toàn rồi ghi atomic, nên chạy lại bao nhiêu lần cũng cho cùng kết quả.
Công tắc `CRYPTO_MEDALLION_ENABLED=0` đưa DAG về luồng chỉ có Bronze.

---

## 6. Hạ tầng và vận hành

### 6.1. Máy chủ

| Thành phần | Chi tiết |
|---|---|
| Nhà cung cấp | Google Cloud Platform, một VM Ubuntu |
| Cấu hình | 2 vCPU, 4 GB RAM (khoảng 3,9 GB khả dụng cho hệ điều hành) |
| Chạy dưới dạng | Docker Compose, tự khởi động lại (`restart: unless-stopped`) |

### 6.2. Các container

| Service | Image | Vai trò |
|---|---|---|
| `postgres` | `postgres:16` | metadata của Airflow |
| `airflow-init` | Airflow 2.10.5 (Python 3.12) | khởi tạo DB, tạo tài khoản |
| `airflow-scheduler` | như trên | lập lịch và chạy task (**LocalExecutor**) |
| `airflow-webserver` | như trên | giao diện web |

Thư mục `dags/`, `src/`, `data/` được mount vào container, nên cập nhật code trên VPS là cập nhật bằng cách chép file
rồi restart scheduler. Image build từ `apache/airflow:2.10.5-python3.12` kèm các thư viện trong `requirements.txt`.

### 6.3. DAG `crypto_market_ingest`

Ba nhóm task:

```
bronze/ingest_klines_1s, ingest_klines_1m, ingest_agg_trades, ingest_funding_rate, ingest_open_interest
   └─► silver_gold/build_silver_layer ─► dq_check ─► build_gold_layer
          └─► serve/refresh_catalog + serve/publish_to_s3
```

Cấu hình đáng chú ý: `retries = 3`, `max_active_runs = 1`, `max_active_tasks = 2`. DAG chỉ dựng lại cửa sổ khoảng 24
giờ gần nhất, nên Silver/Gold cũ trên R2 không bị đụng tới.

Kết quả đo run `scheduled__2026-10-25T02:00:00` (bắt đầu 04:00 UTC ngày 25/10): cả 10 task `success`, tổng 2 phút 38 giây;
`build_silver_layer` ghi 4 partition, `dq_check` kiểm 280 giờ (280 đạt), `publish_to_s3` đẩy 56 file mới (khoảng 22,8 MiB),
bỏ qua 1.024 file không đổi và cập nhật `_manifest.json` bản đầy đủ. RAM đo được lúc đó: dùng khoảng 2,1 GB, còn
khoảng 1,7 GB khả dụng (một lần đo tại một thời điểm, không phải đỉnh).

### 6.4. Kho chia sẻ — Cloudflare R2

- Bucket `crypto-lake`, prefix `crypto/`, cùng layout Hive với local: `lake/` (Bronze), `silver/`, `gold/`, `_manifest.json`.
- R2 tương thích S3, không tính phí egress; layout Hive nên DuckDB, Athena, Spark tự nhận partition.
- Có hai loại key: key **ghi** cho DAG publish và key **chỉ đọc** cho người nhận. Người nhận chỉ cần endpoint, bucket
  và key chỉ đọc.

### 6.5. Chi phí

Theo báo cáo quản trị `docs/reports/bao-cao-hieu-qua-phong-du-lieu-2026-10.md` (lập ngày 24/10/2026): tổng chi phí hạ tầng
khoảng **8 USD/tháng** ở 2 symbol; ước tính 20 USD/tháng nếu mở rộng lên 20 symbol. Báo cáo đó cũng nêu các rủi ro kỹ thuật
lớn nhất: giới hạn gọi API, quá nhiều file nhỏ, và `open_interest` không thể tải lại ngoài cửa sổ 30 ngày.

---

## 7. Công nghệ sử dụng

| Lớp | Công nghệ | Dùng để |
|---|---|---|
| Ngôn ngữ | Python 3.10+ (image Airflow dùng 3.12) | toàn bộ pipeline |
| Điều phối | Apache Airflow 2.10.5, LocalExecutor | lập lịch, retry, theo dõi |
| Metadata | PostgreSQL 16 | CSDL của Airflow |
| Đóng gói | Docker, Docker Compose | triển khai thống nhất local và VPS |
| Định dạng lưu trữ | Apache Parquet qua PyArrow, nén Zstandard | lake theo cột |
| Truy vấn | DuckDB (SQL trên Parquet, `httpfs` đọc R2) | Silver, DQ, Gold, catalog, phân tích |
| Kho đối tượng | Cloudflare R2 (S3-compatible), `boto3` | chia sẻ dữ liệu |
| HTTP | `requests` | gọi Binance API |
| Kiểm thử | `pytest`, `ruff` | 37 test, kiểm tra phong cách mã |
| Phân tích | pandas, NumPy, matplotlib, Jupyter | metrics và biểu đồ B4 |
| Truy cập UI | LAN, Cloudflare Tunnel hoặc Tailscale Funnel; tài khoản `Viewer` chỉ xem | chia sẻ giao diện Airflow |

So sánh các phương án lưu trữ (Parquet + DuckDB, ClickHouse, TimescaleDB, BigQuery…) và lý do chọn Parquet + DuckDB ở
giai đoạn hiện tại nằm trong `docs/storage.md` của repo gốc: dữ liệu append-only, truy vấn theo symbol và khoảng thời gian,
xử lý theo lô 2 giờ, không cần realtime.

---

## 8. Bảo mật và tuân thủ

- Thông tin xác thực nằm trong `.env`, đã nằm trong `.gitignore`; không commit key.
- Tách key ghi (chỉ DAG trên VPS dùng) và key chỉ đọc (chia sẻ cho người nhận).
- Giao diện Airflow bật `EXPOSE_CONFIG=false` và `EXPOSE_STACKTRACE=false`; có tài khoản `Viewer` không thể trigger hay sửa DAG.
- Notebook gắn key chỉ đọc để gửi riêng được gitignore (`notebooks/*_R2.ipynb`), không đưa lên git.
- Giấy phép dữ liệu: CC BY-NC-SA 4.0, chỉ dùng cho nghiên cứu và giáo dục (xem `DE_BAI.md`).

---

## 9. Phân tích B4 trên nền tảng này

Thư mục `B4_analysis/` đọc dữ liệu từ R2 và tính các chỉ số chất lượng thị trường theo giờ:

| Chỉ số | Mã nguồn | Dữ liệu đầu vào |
|---|---|---|
| Chênh lệch giá hiệu dụng Roll, Amihud | `src/metrics_roll_amihud.py` | klines 1 giây |
| Mất cân bằng dòng lệnh (OFI) và tác động giá | `src/metrics_ofi_impact.py` | aggTrades |
| Tỷ số phương sai VR(2/5/10) | `src/metrics_variance_ratio.py` | klines 1 giây |
| Quoted spread (đối chiếu) | `src/ingest_lobster.py` | mẫu LOBSTER AAPL 21/06/2012 |

Notebook `B4_chuan_bi_du_lieu_R2.ipynb` lấy dữ liệu từ ba tầng, ánh xạ cột Silver sang đúng tên cột mà các hàm metrics cần
(`trade_time → transact_time`, `qty → quantity`, `aggressor_side='SELL' → is_buyer_maker=True`, `trades → n_trades`),
kiểm tra chất lượng khoảng phân tích rồi lưu cache. Notebook đã chạy trên R2 cho kỳ 19–21/10/2026: 518.400 dòng
klines 1 giây và 4.359.647 giao dịch, không có giờ lỗi.

Kết quả sơ bộ của báo cáo B4 hiện tại (50 giờ dữ liệu, xem `docs/B4_bao_cao.md`): Amihud của ETH lớn hơn BTC khoảng 21 lần,
nghĩa là ETH kém thanh khoản hơn BTC trên mẫu này.

---

## 10. Hạn chế và rủi ro

| Hạng mục | Nội dung |
|---|---|
| Phạm vi tài sản | chỉ BTCUSDT và ETHUSDT; thị trường crypto giao dịch 24/7, không biên độ, không phiên định kỳ |
| Ngoại suy sang Việt Nam | Binance không cung cấp BBO hay sổ lệnh; Việt Nam chưa có dữ liệu sổ lệnh công khai. Sản phẩm là bộ thước đo và danh sách dữ liệu trong nước cần có, không phải con số áp dụng được |
| `open_interest` | API chỉ trả 30 ngày gần nhất; phần ngoài cửa sổ không thể tải lại |
| Kiểu số | giá và khối lượng giữ `float64`, chưa chuyển sang decimal |
| Nến 1 giây | giây không có giao dịch không có nến, nên `bars_1s` thưa là bình thường |
| Tài nguyên VPS | 4 GB RAM; chưa đo đỉnh RAM khi `build_silver_layer` chạy cửa sổ lớn |
| Cổng DQ | nếu Bronze trên VPS thiếu giờ thì Gold bị bỏ qua, chỉ Bronze được publish (hành vi chủ đích) |
| Phụ thuộc nguồn | dữ liệu LOBSTER mẫu có thể bị đóng bất cứ lúc nào; đã tải sẵn |
| Vận hành tự động | sự cố của hệ thống tự động (rate limit, API lỗi, lệch giờ) là điểm đề bài yêu cầu phản biện |

Danh sách lỗi và vấn đề đã tìm thấy khi review mã B4 nằm ở thư mục `issue/` của repo gốc.

---

## 11. Cách tái lập

```bash
# 1. Chạy local, không cần Airflow
make install
.venv/bin/crypto-ingest ingest --hours 2          # Bronze
make layers                                       # Silver, DQ, Gold
make catalog                                      # view DuckDB

# 2. Chạy cả hệ thống bằng Docker
docker compose up -d --build                      # Postgres + Airflow

# 3. Phân tích B4 (cần key đọc R2 trong B4_analysis/.env)
cd B4_analysis
pip install -r requirements.txt
python -m src.compute_from_r2 --symbols BTCUSDT,ETHUSDT --date-from 2026-10-19 --date-to 2026-10-21
```

Tài liệu liên quan trong repo gốc: `README.md`, `docs/medallion.md`, `docs/storage.md`, `docs/deploy-vps.md`,
`docs/share-s3.md`, `docs/consumer.md`.
