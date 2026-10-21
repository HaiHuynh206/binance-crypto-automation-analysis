# Hướng dẫn cho người nhận dữ liệu

Tài liệu này gửi kèm khi share bucket. Người nhận cần 4 thông tin:

| | Ví dụ |
|---|---|
| Endpoint | `https://<account-id>.r2.cloudflarestorage.com` |
| Bucket | `crypto-lake` |
| Access key ID | `<read-only-key>` |
| Secret access key | `<read-only-secret>` |

Quyền đọc là đủ; **không cần** quyền ghi/xoá.

## 1. Xem có dữ liệu gì

`_manifest.json` ở gốc bucket liệt kê dataset, số dòng, số file, dung lượng,
khoảng thời gian và schema:

```python
import boto3, json

s3 = boto3.client(
    "s3",
    endpoint_url="https://<account-id>.r2.cloudflarestorage.com",
    region_name="auto",
    aws_access_key_id="<read-only-key>",
    aws_secret_access_key="<read-only-secret>",
)
body = s3.get_object(Bucket="crypto-lake", Key="crypto/_manifest.json")["Body"].read()
print(json.dumps(json.loads(body), indent=2))
```

## 2. Query bằng DuckDB

```bash
pip install duckdb
```

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='<account-id>.r2.cloudflarestorage.com';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_access_key_id='<read-only-key>';")
con.execute("SET s3_secret_access_key='<read-only-secret>';")
con.execute("SET TimeZone='UTC';")

print(con.sql("""
    SELECT symbol, date, hour, count(*) AS trades
    FROM read_parquet(
      's3://crypto-lake/crypto/lake/exchange=binance/dataset=agg_trades/**/*.parquet',
      hive_partitioning = true
    )
    GROUP BY ALL
    ORDER BY 1, 2, 3
"""))
```

Nếu lỗi đường dẫn với R2, thêm `SET s3_url_style='path';`.

Dataset có sẵn:

| Dataset | Nội dung |
|---|---|
| `klines_1s` | nến 1 giây |
| `klines_1m` | nến 1 phút |
| `agg_trades` | aggregate trades (nặng nhất) |
| `funding_rate` | funding rate futures |
| `open_interest` | open interest futures |

Partition: `exchange`, `dataset`, `symbol`, `date`, `hour`.

## 3. Tải toàn bộ hoặc một phần

Cài AWS CLI, cấu hình endpoint R2:

```bash
aws configure set AWS_ACCESS_KEY_ID '<read-only-key>'
aws configure set AWS_SECRET_ACCESS_KEY '<read-only-secret>'
aws configure set default.region auto

# cả lake (~vài GB)
aws s3 sync s3://crypto-lake/crypto/lake ./crypto-lake \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com

# chỉ một dataset
aws s3 sync s3://crypto-lake/crypto/lake/exchange=binance/dataset=klines_1m \
  ./klines_1m --endpoint-url https://<account-id>.r2.cloudflarestorage.com
```

Sau khi tải, query local:

```python
con.sql("""
    SELECT * FROM read_parquet('crypto-lake/**/*.parquet', hive_partitioning=true)
    LIMIT 10
""")
```

## 4. Lưu ý

- Dữ liệu là UTC; đặt `TimeZone='UTC'` để tránh lệch giờ khi hiển thị.
- Mỗi giờ là một file; giờ mới nhất có thể được cập nhật thêm trade trễ, nên file
  của giờ hiện tại có thể thay đổi.
- Không chia sẻ lại access key; nếu cần thêm người, yêu cầu cấp key riêng.
- Với dữ liệu public, có thể tải trực tiếp qua URL `https://pub-...r2.dev/...`
  nhưng R2 public không có directory listing; dùng `_manifest.json` để biết file.
