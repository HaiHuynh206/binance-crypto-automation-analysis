# Chia sẻ data qua S3 / R2 / MinIO

Pipeline có thể đẩy nguyên lake Parquet lên object storage tương thích S3. Người
nhận chỉ cần endpoint + quyền đọc (hoặc bucket public) là query/tải được, không cần
chạy Airflow hay Docker.

**Khuyến nghị: Cloudflare R2** — 10 GB storage miễn phí, **không tính phí egress**,
hợp với dữ liệu hiện tại (vài GB/năm). AWS S3, Backblaze B2, MinIO đều dùng được.

## 1. Tạo bucket

### Cloudflare R2

1. Vào [dash.cloudflare.com](https://dash.cloudflare.com) → **R2** → **Create bucket**,
   đặt tên ví dụ `crypto-lake`.
2. **Manage R2 API Tokens** → **Create API token** → quyền **Object Read & Write**,
   giới hạn trong bucket vừa tạo.
3. Lưu lại:
   - Access Key ID
   - Secret Access Key
   - Endpoint: `https://<account-id>.r2.cloudflarestorage.com`

### AWS S3

Tạo bucket ở region gần (ví dụ `ap-southeast-1`), tạo IAM user chỉ có quyền
`PutObject`, `GetObject`, `ListBucket` trên bucket đó. Không dùng root key.

### MinIO (tự host)

Xem mục 8 để chạy MinIO local bằng Docker.

## 2. Cấu hình `.env`

```env
CRYPTO_S3_BUCKET=crypto-lake
CRYPTO_S3_PREFIX=crypto
CRYPTO_S3_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
CRYPTO_S3_REGION=auto

AWS_ACCESS_KEY_ID=<access-key-id>
AWS_SECRET_ACCESS_KEY=<secret-access-key>
```

- **AWS S3**: để trống `CRYPTO_S3_ENDPOINT_URL`, đặt `CRYPTO_S3_REGION=ap-southeast-1`.
- **MinIO**: `CRYPTO_S3_ENDPOINT_URL=http://localhost:9000` (CLI trên host) hoặc
  `http://minio:9000` (Airflow trong compose network), `CRYPTO_S3_REGION=us-east-1`.
- Để trống `CRYPTO_S3_BUCKET` nếu muốn tắt publish; DAG sẽ bỏ qua task S3.
- `CRYPTO_S3_ACL=public-read` chỉ dùng cho AWS S3 nếu muốn public từng object;
  R2/MinIO nên bật public ở cấp bucket (mục 5).

## 3. Upload thử

```bash
cd "/home/hai-huynh/bigdata project"

# xem trước, không upload
make sync-s3 DRY_RUN=1

# upload phần thay đổi + manifest
make sync-s3
```

Hoặc gọi thẳng CLI (nhớ nạp `.env`; `make` tự nạp giúp bạn):

```bash
set -a; . ./.env; set +a
.venv/bin/crypto-ingest sync-s3 --dry-run
.venv/bin/crypto-ingest sync-s3 -d klines_1m,agg_trades -s BTCUSDT   # có lọc: không đụng _manifest.json
.venv/bin/crypto-ingest sync-s3 --force          # upload lại tất cả
.venv/bin/crypto-ingest sync-s3 --no-manifest    # bỏ _manifest.json
```

Sau khi thêm `boto3`, phải rebuild image Airflow:

```bash
docker compose up -d --build
```

Kiểm tra task trong Airflow: DAG `crypto_market_ingest` có thêm
`serve.publish_to_s3`, chạy song song với `serve.refresh_catalog` sau khi ingest xong. Task chỉ
upload file mới/thay đổi (so sánh MD5/ETag, fallback mtime), nên chạy lại an toàn.

## 4. Cấu trúc trên bucket

```
s3://crypto-lake/crypto/
├── _manifest.json
└── lake/
    └── exchange=binance/
        └── dataset=klines_1m/
            └── symbol=BTCUSDT/
                └── date=2026-10-21/
                    └── hour=07/
                        └── part-00000.parquet
```

Giữ nguyên Hive layout nên DuckDB/Athena/Spark tự nhận partition. `_manifest.json`
chứa coverage, số dòng, số file, dung lượng và schema từng dataset — dùng để người
nhận biết có dữ liệu gì trước khi query.

## 5. Chia sẻ

### Cách A — bucket public (dữ liệu thị trường vốn công khai)

R2: bucket → **Settings** → **Public access** → bật `r2.dev` (test) hoặc gắn custom
domain (production, không rate-limit).

URL public có dạng `https://pub-<hash>.r2.dev/crypto/lake/...`; người nhận tải file
trực tiếp:

```bash
curl -O https://pub-<hash>.r2.dev/crypto/lake/exchange=binance/\
dataset=klines_1m/symbol=BTCUSDT/date=2026-10-21/hour=07/part-00000.parquet
```

R2 public **không có directory listing** và endpoint public không phải S3 API, nên
gửi kèm `_manifest.json` hoặc danh sách file. Với **AWS S3** bucket policy public,
có thể sync không cần ký:

```bash
aws s3 sync s3://crypto-lake/crypto/lake ./crypto-lake --no-sign-request
```

> `r2.dev` có rate limit; với dữ liệu lớn nên dùng custom domain.

### Cách B — cấp quyền đọc riêng (khuyến nghị)

Tạo thêm R2 API token **Object Read only** cho từng người/nhóm, gửi kèm endpoint và
bucket. Cách này kiểm soát được ai đang đọc và thu hồi dễ; người nhận query được
trực tiếp bằng DuckDB (mục 6).

### Cách C — presigned URL cho file lẻ

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="https://<account-id>.r2.cloudflarestorage.com",
    region_name="auto",
    aws_access_key_id="...",
    aws_secret_access_key="...",
)
url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": "crypto-lake", "Key": "crypto/_manifest.json"},
    ExpiresIn=7 * 24 * 3600,
)
print(url)
```

Presigned URL chỉ hợp với từng object, không dùng cho wildcard/glob.

> Gửi kèm [`docs/consumer.md`](consumer.md) cho người nhận: chọn tầng Bronze/Silver/Gold,
> công thức query và lưu ý, không cần biết Airflow.

## 6. Query dữ liệu

Bucket có ba tầng cùng prefix: `lake/` (Bronze), `silver/`, `gold/`. Phân tích nên bắt đầu
từ Gold (`gold/mart=ohlcv`, `market_hourly`, `derivatives_hourly`, `data_quality_daily`);
công thức đầy đủ nằm trong [`consumer.md`](consumer.md). Ví dụ dưới đây dùng Bronze để
minh hoạ kết nối.

### DuckDB (khuyến nghị)

```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint = '<account-id>.r2.cloudflarestorage.com';
SET s3_region = 'auto';
SET s3_access_key_id = '<read-key>';
SET s3_secret_access_key = '<read-secret>';
SET TimeZone = 'UTC';

SELECT symbol, date, hour, count(*) AS rows
FROM read_parquet(
  's3://crypto-lake/crypto/lake/exchange=binance/dataset=agg_trades/**/*.parquet',
  hive_partitioning = true
)
GROUP BY ALL
ORDER BY 1, 2, 3;
```

Nếu gặp lỗi đường dẫn với R2, thêm `SET s3_url_style = 'path';`.

Đọc Gold (nên lọc `symbol` và `date` trước để DuckDB chỉ đọc đúng file):

```sql
SELECT bucket, close, volume
FROM read_parquet('s3://crypto-lake/crypto/gold/mart=ohlcv/**/*.parquet', hive_partitioning = true)
WHERE symbol = 'BTCUSDT' AND "interval" = '1h' AND is_complete AND date >= '2026-10-01'
ORDER BY bucket;
```

Python:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='<account-id>.r2.cloudflarestorage.com';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_access_key_id='...';")
con.execute("SET s3_secret_access_key='...';")
con.execute("SET TimeZone='UTC';")
rows = con.sql("""
    SELECT * FROM read_parquet(
      's3://crypto-lake/crypto/lake/exchange=binance/dataset=klines_1m/**/*.parquet',
      hive_partitioning = true
    ) LIMIT 100
""")
print(rows)
```

### Athena

Mỗi dataset một bảng (schema khác nhau), partition theo `symbol/date/hour`:

```sql
CREATE EXTERNAL TABLE klines_1m (
  `interval` string, open_time timestamp, close_time timestamp,
  open double, high double, low double, close double, volume double,
  quote_volume double, trades bigint, taker_buy_base double,
  taker_buy_quote double, ingested_at timestamp
)
PARTITIONED BY (symbol string, date string, hour string)
STORED AS PARQUET
LOCATION 's3://crypto-lake/crypto/lake/exchange=binance/dataset=klines_1m/';

MSCK REPAIR TABLE klines_1m;
```

Thực tế nên dùng partition projection để tránh `MSCK` mỗi ngày.

### Spark

```python
df = spark.read.parquet(
    "s3a://crypto-lake/crypto/lake/exchange=binance/dataset=klines_1m/"
)
```

## 7. Chi phí & vận hành

| | R2 | S3 Standard | MinIO |
|---|---|---|---|
| Storage | ~$0.015/GB-tháng, free 10 GB | ~$0.023/GB-tháng | đĩa của bạn |
| Egress | **$0** | ~$0.09/GB | băng thông của bạn |
| Phù hợp | share công khai/không egress | hệ sinh thái AWS | nội bộ/LAN |

Ước lượng **thực tế** với cấu hình hiện tại (2 symbol, 5 dataset):

- 42 giờ dữ liệu = **48.8 MiB**
- ~1.16 MiB/giờ ≈ **28 MiB/ngày** ≈ **0.83 GiB/tháng** ≈ **9.9 GiB/năm**
- `agg_trades` chiếm **77%**, `klines_1s` **21%**, ba dataset còn lại rất nhỏ.

→ Với 2 symbol, bucket đầy 10 GB miễn phí sau **khoảng 12 tháng**. Thêm symbol
hoặc thị trường biến động mạnh sẽ nhanh hơn (x2 symbol ≈ 6 tháng, x5 ≈ 2.5 tháng).

Vượt 10 GB vẫn rất rẻ: khoảng **$0.015/GB-tháng**, tức 20 GB ≈ $0.15/tháng,
50 GB ≈ $0.60/tháng. R2 không tính phí egress.

Lưu ý:

- Upload là **incremental**, không xoá object khi file local bị xoá. Muốn dọn thì
  đặt lifecycle rule trên bucket hoặc dùng `aws s3 sync --delete` có kiểm soát.
- **Không nên bật versioning** nếu muốn giữ free tier: mỗi lần refresh giờ gần nhất
  sẽ tạo thêm version, làm tăng dung lượng nhanh. Nếu cần versioning, đặt lifecycle
  xoá noncurrent version sau 7–30 ngày.
- Muốn giảm dung lượng lâu dài: `agg_trades` + `klines_1s` chiếm ~98%. Có thể tắt
  `klines_1s` (suy ra được từ `agg_trades`), hoặc chỉ giữ `agg_trades` 3–6 tháng
  rồi downsample, trong khi `klines_1m`/`funding_rate`/`open_interest` giữ lâu.
- Không commit credentials vào git; `.env` đã nằm trong `.gitignore`.
- Nếu bucket public: dữ liệu Binance vốn công khai, nhưng nên theo dõi egress/abuse.

## 8. Test local (không cần cloud)

### S3 mock nhanh

`adobe/s3mock` chạy in-memory, rất hợp để test upload mà không cần tài khoản:

```bash
docker run -d --name s3mock \
  -p 127.0.0.1:9000:9090 \
  adobe/s3mock:latest
```

Tạo bucket (s3mock nhận bất kỳ access key nào):

```bash
cd "/home/hai-huynh/bigdata project"
.venv/bin/python - <<'PY'
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  region_name="us-east-1",
                  aws_access_key_id="test", aws_secret_access_key="test")
s3.create_bucket(Bucket="crypto-lake")
print(s3.list_buckets()["Buckets"])
PY
```

Cấu hình `.env`:

```env
CRYPTO_S3_BUCKET=crypto-lake
CRYPTO_S3_ENDPOINT_URL=http://localhost:9000
CRYPTO_S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```

```bash
make sync-s3
.venv/bin/python - <<'PY'
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  region_name="us-east-1",
                  aws_access_key_id="test", aws_secret_access_key="test")
for obj in s3.list_objects_v2(Bucket="crypto-lake").get("Contents", []):
    print(obj["Key"], obj["Size"])
PY
```

Dừng/xoá: `docker rm -f s3mock` (dữ liệu mất vì lưu trong RAM).

### MinIO

Nếu muốn S3 server thật, cài MinIO binary từ [dl.min.io](https://dl.min.io/server/minio/release/linux-amd64/minio)
(nếu image Docker `minio/minio` không pull được trong môi trường của bạn):

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server ./minio-data --console-address :9001
```

Mở `http://localhost:9001`, tạo bucket `crypto-lake`, rồi dùng endpoint
`http://localhost:9000` như mục trên. Nếu Airflow chạy trong Docker mà MinIO chạy
trên host, đổi endpoint thành `http://host.docker.internal:9000` và thêm
`extra_hosts: ["host.docker.internal:host-gateway"]` cho service Airflow.

> MinIO trên PC chỉ truy cập được khi PC bật và port được mở/tunnel. Muốn share
> 24/7 cho người ngoài, dùng R2/S3.
