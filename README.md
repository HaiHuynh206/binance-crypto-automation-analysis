# Crypto High-Frequency Data Platform

Pipeline thu thập dữ liệu thị trường crypto tần suất cao (Binance public API), chạy
Airflow mỗi **2 giờ**, lưu vào **Parquet lake** partition theo giờ và truy vấn bằng
**DuckDB**.

```
Binance REST API
      │  (klines 1s/1m, aggTrades, funding rate, open interest)
      ▼
crypto_ingest (rate limit + retry + pagination)
      │
      ▼
data/lake/exchange=binance/dataset=.../symbol=.../date=.../hour=.../part-00000.parquet
      │
      ├── data/_state/completed/...   (marker idempotency)
      └── data/catalog.duckdb         (DuckDB views)
                │
                ├──► SQL / dashboard / notebook
                └──► S3 / R2 / MinIO (share cho người khác)
```

## Vì sao thiết kế như vậy

- **Mỗi giờ = một file Parquet** → ghi atomic, retry/backfill chỉ ghi đè đúng giờ đó,
  không bao giờ có dữ liệu trùng hoặc file dở dang.
- **Marker hoàn thành theo từng partition** → chạy lại cùng window gần như miễn phí;
  task fail thì lần retry chỉ tải lại phần thiếu.
- **Rate limit + retry** nằm trong code, không phụ thuộc Airflow.
- **DuckDB** đọc trực tiếp Parquet, không cần server, không copy dữ liệu.

## Dataset

| Dataset | Nguồn | Tần suất dữ liệu | Ghi chú |
|---|---|---|---|
| `klines_1s` | `GET /api/v3/klines` | 1 giây | ~3.600 dòng/giờ/symbol |
| `klines_1m` | `GET /api/v3/klines` | 1 phút | ~60 dòng/giờ/symbol |
| `agg_trades` | `GET /api/v3/aggTrades` | từng trade | Nặng nhất, phân trang theo `fromId` |
| `funding_rate` | `GET /fapi/v1/fundingRate` | 8 giờ/lần | Futures |
| `open_interest` | `GET /futures/data/openInterestHist` | 5 phút | Chỉ lấy được 30 ngày gần nhất |

## Quickstart (không cần Airflow)

Yêu cầu: Python >= 3.10.

```bash
cd "bigdata project"
make install

# Lấy 2 giờ gần nhất cho toàn bộ dataset mặc định
.venv/bin/crypto-ingest ingest --hours 2

# Chỉ lấy nến 1 phút trong 1 ngày
.venv/bin/crypto-ingest ingest -d klines_1m \
  --start 2026-10-20T00:00:00Z --end 2026-10-21T00:00:00Z

# Xem dung lượng / độ phủ
.venv/bin/crypto-ingest status

# Tạo view DuckDB rồi query
.venv/bin/crypto-ingest catalog
.venv/bin/python -c "
import duckdb
con = duckdb.connect('data/catalog.duckdb', read_only=True)
print(con.sql('''
    SELECT symbol, count(*) rows, min(open_time), max(open_time)
    FROM klines_1m GROUP BY 1 ORDER BY 1
'''))
"

# Backfill 7 ngày
.venv/bin/crypto-ingest backfill --days 7 -d klines_1m,agg_trades

# Upload lên R2/S3/MinIO (cấu hình .env trước, xem docs/share-s3.md)
.venv/bin/crypto-ingest sync-s3 --dry-run
```

> Lần chạy đầu với `agg_trades` cho BTCUSDT có thể mất vài phút vì phải phân trang
> qua hàng trăm nghìn trade. Bắt đầu với `-d klines_1m` để test nhanh.

## Chạy Airflow (Docker)

Cài Docker (Ubuntu, cần quyền sudo):

```bash
sudo bash scripts/install-docker.sh
# đăng xuất/đăng nhập lại để dùng docker không cần sudo
```

Sau đó:

```bash
cd "/home/hai-huynh/bigdata project"
make up          # tạo .env + build image + postgres + webserver + scheduler
```

- UI: http://localhost:8080 (mặc định `admin` / `admin`, đổi trong `.env`).
- DAG: `crypto_market_ingest`, schedule `0 */2 * * *`.
- Mở UI và bật DAG (lúc tạo đang **paused**); scheduler sẽ chạy interval gần nhất.
- Logs: `make logs`; dừng: `make down`; reset toàn bộ: `make down && docker compose down -v`.

Lưu ý quan trọng:

- **Giữ `AIRFLOW_UID=50000`** trong `.env`. Nếu đổi thành uid của bạn (1000),
  Airflow báo lỗi `The user that Airflow is running as has no username`.
- Nếu vừa cài Docker mà gặp `permission denied` khi gọi `docker`, hãy logout/login
  (hoặc chạy `newgrp docker`) để nhận group mới.
- Chạy CLI trên host **và** Airflow trong container cùng lúc thì cần quyền ghi chung
  cho thư mục `data/`:
  `make permissions`.
- Đổi `CRYPTO_SYMBOLS`/`CRYPTO_DATASETS` trong `.env` rồi `docker compose up -d`
  để áp dụng cho container.

Luồng task:

```
ingest_* (5 task) ──┬─► refresh_catalog
                    └─► publish_to_s3 (chỉ khi CRYPTO_S3_BUCKET được set)
```

`max_active_tasks=2` để tránh 5 task cùng đập vào rate limit của Binance.

## Chia sẻ data

Đẩy lake lên **Cloudflare R2 / AWS S3 / MinIO**, giữ nguyên layout Hive để người
nhận query bằng DuckDB, Athena hoặc Spark:

```bash
make sync-s3 DRY_RUN=1   # xem trước
make sync-s3             # upload phần thay đổi + _manifest.json
```

DAG tự chạy task `publish_to_s3` sau mỗi lần ingest khi `CRYPTO_S3_BUCKET` được
set. Hướng dẫn tạo bucket R2, cấp quyền đọc, query mẫu và chi phí:
[`docs/share-s3.md`](docs/share-s3.md).

## Chia sẻ UI

- Cùng LAN: `http://<ip-may-nay>:8080` (ví dụ `http://192.168.1.17:8080`).
- Internet nhanh: `make share` → Cloudflare quick tunnel in ra URL HTTPS.
  Dừng bằng `make unshare`.
- Tài khoản: `admin` (không share) và `viewer` (role Viewer, chỉ xem).
- Chi tiết named tunnel + domain, Cloudflare Access, Tailscale và cách tạo tài
  khoản riêng: [`docs/sharing.md`](docs/sharing.md).

## Chạy 24/7 khi tắt PC

Airflow đang chạy trên chính PC này, nên tắt máy là dừng. Để chạy 24/7, migrate
sang VPS:

```bash
make migrate HOST=root@<IP> WITH_DATA=1
```

Hướng dẫn chọn VPS (4 GB RAM), migrate dữ liệu, bảo mật và backup:
[`docs/deploy-vps.md`](docs/deploy-vps.md).

## Cấu hình

Tất cả qua biến môi trường (xem `.env.example`):

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `CRYPTO_DATA_ROOT` | `./data` | Thư mục gốc chứa lake + state |
| `CRYPTO_SYMBOLS` | `BTCUSDT,ETHUSDT` | Danh sách symbol |
| `CRYPTO_DATASETS` | cả 5 dataset | Dataset cần chạy |
| `CRYPTO_WEIGHT_PER_MINUTE` | `2400` | Ngân sách weight/phút (Binance cho 6000/IP) |
| `CRYPTO_LOOKBACK_HOURS` | `24` | Quét lại 24h gần nhất, tự bù hour còn thiếu sau downtime |
| `CRYPTO_MAX_PAGES` | `5000` | Chặn an toàn khi phân trang |
| `CRYPTO_HTTP_TIMEOUT` | `20` | Timeout mỗi request (giây) |
| `CRYPTO_S3_BUCKET` | (trống) | Bucket đích; trống = tắt publish S3 |
| `CRYPTO_S3_PREFIX` | `crypto` | Prefix trên bucket |
| `CRYPTO_S3_ENDPOINT_URL` | (trống) | R2/MinIO endpoint; trống = AWS S3 |
| `CRYPTO_S3_REGION` | `auto` | `auto` cho R2, region cho AWS/MinIO |
| `CRYPTO_S3_ACL` | (trống) | Ví dụ `public-read` (chỉ AWS S3) |

Thêm symbol: sửa `CRYPTO_SYMBOLS` rồi restart scheduler (hoặc chạy CLI backfill cho
symbol mới). Không cần migration.

## Cấu trúc code

```
dags/crypto_ingest_dag.py     DAG Airflow (mỏng, chỉ gọi package)
src/crypto_ingest/
  config.py                   đọc env
  timeutil.py                 UTC, chia bucket giờ
  http.py                     rate limiter + retry
  binance.py                  fetcher & phân trang từng dataset
  schemas.py                  Arrow schema, dedupe, quality threshold
  storage.py                  ghi/đọc Parquet partition
  state.py                    marker hoàn thành
  s3.py                       upload lake lên S3/R2/MinIO + manifest
  pipeline.py                 orchestration một window
  catalog.py                  tạo DuckDB views
  cli.py                      ingest / backfill / status / catalog
tests/                        unit test (không gọi mạng)
docs/storage.md               khuyến nghị nơi lưu dữ liệu chi tiết
docs/share-s3.md              chia sẻ data qua S3/R2/MinIO
docs/consumer.md              hướng dẫn gửi cho người nhận data
```

## Lưu dữ liệu ở đâu?

**Khuyến nghị cho giai đoạn hiện tại: Parquet + Zstandard trên SSD local, partition
theo giờ, query bằng DuckDB** (đúng như code này đang làm).

Lý do: dữ liệu append-only, ít update, chủ yếu scan theo symbol + thời gian; Parquet
nén tốt (thường 5–15x so với JSON), DuckDB query vài trăm triệu dòng trên laptop mà
không cần vận hành server. Đây là lựa chọn rẻ nhất và ít rủi ro nhất để bắt đầu.

Khi scale lên, lộ trình:

1. **> 500 GB / nhiều máy / cần backup tự động** → MinIO hoặc S3 làm lake
   (giữ nguyên layout partition), DuckDB `httpfs` hoặc Athena query.
2. **Dashboard, nhiều người query, cần sub-second trên hàng tỷ dòng** → ClickHouse
   (`MergeTree`, `ORDER BY (symbol, trade_time)`, TTL + codec).
3. **Cần realtime < 1 phút** → Kafka/Redpanda → ClickHouse (ngoài phạm vi batch 2h).

Chi tiết so sánh, sizing và những thứ **không nên** dùng: [`docs/storage.md`](docs/storage.md).

## Vận hành & giới hạn cần biết

- **Idempotent**: chạy lại window cũ an toàn; mỗi lần chạy tự refresh thêm 1 giờ gần
  nhất để bắt trade/klines đến trễ.
- **Giờ thiếu**: nếu Binance lỗi, task fail và Airflow retry; marker chỉ được ghi sau
  khi file đã ghi xong.
- **`open_interest`** chỉ truy vấn được 30 ngày gần nhất → backfill sâu hơn sẽ lỗi.
- **`agg_trades`** rất nặng; tăng `CRYPTO_SYMBOLS` sẽ tăng thời gian chạy tuyến tính.
- **Không dùng Airflow metadata DB** để lưu market data.
- **File nhỏ**: mỗi symbol/dataset sinh 24 file/ngày. Với ~100k file, DuckDB vẫn ổn;
  nếu vượt nhiều, gộp theo ngày/tháng bằng job compaction (xem docs).
- Dữ liệu Binance thuộc Binance; kiểm tra điều khoản nếu dùng cho mục đích thương mại.

## Test

```bash
make test
```

Test không gọi mạng: fetcher được stub, chỉ kiểm tra chia giờ, dedupe, ghi/đọc
partition, idempotency và xử lý lỗi.
