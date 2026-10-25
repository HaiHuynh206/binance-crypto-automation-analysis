# Hướng dẫn query và sử dụng dữ liệu

Tài liệu cho người nhận dữ liệu (đọc từ R2/S3 hoặc từ bản tải về). Lake có ba tầng;
**hầu hết trường hợp nên bắt đầu từ Gold, chỉ xuống Silver/Bronze khi cần chi tiết hơn.**

## 1. Chọn tầng nào?

| Bạn cần | Dùng | Vì sao |
|---|---|---|
| Nến 5m/1h/1d, volatility, áp lực mua/bán, OI, funding theo giờ | **Gold** (`gold_*`) | Đã tổng hợp sẵn, nhỏ, nhanh |
| Từng nến 1s/1m, từng trade, từng điểm funding/OI đã làm sạch | **Silver** (`silver_*`) | Đã dedupe, đổi tên cột, loại dòng sai |
| Dữ liệu y nguyên từ Binance, kiểm chứng ngược | **Bronze** (`bronze_*`) | Không sửa gì, có thể trùng giữa các giờ |

Quy ước nên theo: **lọc theo `symbol` và `date` trước** (đây là partition), rồi mới lọc cột khác.
Làm vậy DuckDB chỉ đọc đúng file cần thay vì quét cả bucket.

## 2. Thông tin kết nối

| | Ví dụ |
|---|---|
| Endpoint | `https://<account-id>.r2.cloudflarestorage.com` |
| Bucket / prefix | `crypto-lake` / `crypto/` |
| Access key ID / Secret | `<read-only-key>` / `<read-only-secret>` |

Chỉ cần quyền đọc. Cấu trúc trong bucket:

```
crypto/_manifest.json                                           # danh sách dataset, số dòng, khoảng thời gian
crypto/lake/exchange=binance/dataset=<D>/symbol=<S>/date=<YYYY-MM-DD>/hour=<HH>/…parquet   # Bronze
crypto/silver/dataset=<D>/symbol=<S>/date=<YYYY-MM-DD>/part-00000.parquet                  # Silver
crypto/gold/mart=<M>/symbol=<S>/date=<YYYY-MM-DD>/part-00000.parquet                       # Gold
```

Silver: `bars_1s`, `bars_1m`, `trades`, `funding`, `open_interest`, `dq_hourly`.
Gold: `ohlcv`, `market_hourly`, `derivatives_hourly`, `data_quality_daily`.
Bronze: `klines_1s`, `klines_1m`, `agg_trades`, `funding_rate`, `open_interest`.

## 3. Mở kết nối DuckDB và tạo view một lần

```bash
pip install duckdb
```

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET TimeZone='UTC';")                      # bắt buộc, xem mục 6
con.execute("SET s3_endpoint='<account-id>.r2.cloudflarestorage.com';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_access_key_id='<read-only-key>';")
con.execute("SET s3_secret_access_key='<read-only-secret>';")

BASE = "s3://crypto-lake/crypto"

def view(name, pattern):
    con.execute(f"""CREATE OR REPLACE VIEW {name} AS
        SELECT * FROM read_parquet('{BASE}/{pattern}', hive_partitioning = true)""")

for m in ["ohlcv", "market_hourly", "derivatives_hourly", "data_quality_daily"]:
    view(f"gold_{m}", f"gold/mart={m}/**/*.parquet")
for d in ["bars_1s", "bars_1m", "trades", "funding", "open_interest", "dq_hourly"]:
    view(f"silver_{d}", f"silver/dataset={d}/**/*.parquet")
view("bronze_klines_1m", "lake/exchange=binance/dataset=klines_1m/**/*.parquet")
```

Sau đó mọi ví dụ dưới đây chạy bằng `con.sql("...")`. Có bản tải về thì thay `BASE`
bằng đường dẫn local (ví dụ `./crypto-lake`) và bỏ các lệnh `SET s3_*`.
Chạy trong repo này thì bỏ cả đoạn trên: `make catalog` đã tạo sẵn các view đó trong
`data/catalog.duckdb` (`duckdb.connect('data/catalog.duckdb', read_only=True)`).

## 4. Công thức query hay dùng

**Nến 1 giờ đã hoàn chỉnh** (`interval` là từ khoá, phải đặt trong dấu nháy kép):

```sql
SELECT bucket, open, high, low, close, volume, vwap
FROM gold_ohlcv
WHERE symbol = 'BTCUSDT' AND "interval" = '1h' AND is_complete
  AND date BETWEEN '2026-10-01' AND '2026-10-07'
ORDER BY bucket;
```

`is_complete` = nến đủ `expected_bars` nến con. Bỏ điều kiện này sẽ lẫn cả nến cuối
còn đang chạy hoặc nến bị thiếu dữ liệu.

**Volatility và áp lực mua/bán theo ngày:**

```sql
SELECT date, avg(realized_vol) AS vol, avg(aggressor_imbalance) AS imbalance
FROM gold_market_hourly
WHERE symbol = 'ETHUSDT'
GROUP BY date ORDER BY date;
```

`aggressor_imbalance` dương = bên mua chủ động nhiều hơn bên bán.

**Giá + OI + funding cùng một bảng:**

```sql
SELECT m.hour_start, m.close, d.oi_close, d.oi_change, d.last_funding_rate
FROM gold_market_hourly m
JOIN gold_derivatives_hourly d USING (symbol, hour_start)
WHERE symbol = 'BTCUSDT' AND m.date >= '2026-10-20'
ORDER BY m.hour_start;
```

Open interest chỉ có từ 2026-09-25, và `last_funding_rate` có thể `NULL` ở giờ trước
mức funding đầu tiên của dữ liệu; đừng coi `NULL` là 0.

**Tổng giá trị mua/bán chủ động từ trade (Silver):**

```sql
SELECT date, aggressor_side, sum(notional) AS notional
FROM silver_trades
WHERE symbol = 'BTCUSDT' AND date = '2026-10-10'
GROUP BY ALL ORDER BY aggressor_side;
```

`silver_trades` rất nặng. Luôn lọc `symbol` + `date`, và gom lại (`GROUP BY`) thay vì `SELECT *`.

**Kiểm tra dữ liệu có đáng tin không trước khi phân tích:**

```sql
-- tỷ lệ giờ sạch theo dataset
SELECT dataset, symbol, min(pct_ok) AS worst_day
FROM gold_data_quality_daily
GROUP BY ALL ORDER BY worst_day;

-- liệt kê giờ lỗi cụ thể
SELECT dataset_name, symbol, hour_start, status, n_rows, expected_min
FROM silver_dq_hourly
WHERE status <> 'ok'
ORDER BY hour_start;
```

Cột trong `dq_hourly` tên là `dataset_name` (không phải `dataset`, vì `dataset` là cột partition).
Trạng thái: `ok`, `missing`, `partial`, `id_gaps`.

**Nến 1 phút bất kỳ khoảng nào (Silver):**

```sql
SELECT open_time, open, high, low, close, volume, taker_buy_base, taker_sell_base
FROM silver_bars_1m
WHERE symbol = 'BTCUSDT' AND date = '2026-10-10'
ORDER BY open_time;
```

**Xuất ra pandas / file:**

```python
df = con.sql("SELECT * FROM gold_ohlcv WHERE symbol='BTCUSDT' AND date >= '2026-10-01'").df()
con.sql("COPY (SELECT * FROM gold_market_hourly) TO 'market_hourly.parquet' (FORMAT parquet)")
```

## 5. Tải về rồi query local

```bash
aws configure set AWS_ACCESS_KEY_ID '<read-only-key>'
aws configure set AWS_SECRET_ACCESS_KEY '<read-only-secret>'
aws configure set default.region auto
EP=https://<account-id>.r2.cloudflarestorage.com

aws s3 sync s3://crypto-lake/crypto/gold   ./crypto-lake/gold   --endpoint-url $EP   # nhỏ, nên tải trước
aws s3 sync s3://crypto-lake/crypto/silver ./crypto-lake/silver --endpoint-url $EP   # lớn nhất là trades
```

Chỉ lấy một phần: thêm `--exclude '*' --include '*symbol=BTCUSDT/*'`.

## 6. Lưu ý dễ sai

- **Múi giờ:** dữ liệu là UTC. Nếu quên `SET TimeZone='UTC'`, DuckDB hiển thị theo giờ máy
  (ví dụ +07) và `date_trunc('day', …)` sẽ cắt sai ngày.
- **`date` là ngày UTC theo partition**, `hour_start`/`open_time` là timestamp; khi lọc khoảng
  thời gian, lọc cả hai: `date BETWEEN … AND …` để cắt file, rồi điều kiện timestamp để cắt chính xác.
- **Spot và futures:** Silver có cột `market` (`spot`, `usdm_futures`). Nếu dataset có cả hai, thêm `market = …`
  khi lọc, nếu không số liệu sẽ bị cộng lẫn.
- **Nến 1s thưa là bình thường:** giây không có giao dịch thì không có nến.
- **Ngày/giờ gần nhất có thể chưa ổn định:** Bronze giờ hiện tại còn được ghi thêm trade trễ; Silver/Gold
  của ngày hôm nay được dựng lại mỗi 2 giờ. Phân tích cố định nên dùng ngày đã qua.
- **Khi DQ fail**, pipeline vẫn đẩy Bronze nhưng không đẩy Silver/Gold mới; hãy xem
  `gold_data_quality_daily` trước khi tin một khoảng thời gian.
- Quarantine (dòng bị loại) không được publish.
- `float64` cho giá/khối lượng: đủ cho phân tích, không dùng để đối soát tiền.
- Không chia sẻ lại access key; cần thêm người thì xin key riêng.
