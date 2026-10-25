# Kiến trúc Bronze / Silver / Gold

Lake hiện tại (Parquet theo giờ) chính là **Bronze**. Hai tầng còn lại được dựng bằng
DuckDB SQL ngay trên đó, không thêm service nào.

```
Binance REST ─► Bronze ─► Silver ─► DQ gate ─► Gold ─► DuckDB catalog / S3 / R2
                (raw)    (sạch)    (chặn)     (mart)
```

## 1. Ba tầng

| Tầng | Đường dẫn | Đơn vị partition | Nội dung |
|---|---|---|---|
| Bronze | `data/lake/exchange=…/dataset=…/symbol=…/date=…/hour=…/` | 1 giờ | Dữ liệu thô như API trả về, không sửa |
| Silver | `data/silver/dataset=<tên>/symbol=<S>/date=<D>/part-00000.parquet` | 1 ngày UTC | Dedupe, thêm `market`, đổi tên, loại dòng sai |
| Gold | `data/gold/mart=<tên>/symbol=<S>/date=<D>/part-00000.parquet` | 1 ngày UTC | Bảng tổng hợp sẵn cho phân tích/dashboard |

Mỗi lần build, một ngày UTC được dựng lại hoàn toàn rồi ghi atomic (file tạm +
`os.replace`), nên chạy lại bao nhiêu lần cũng ra cùng kết quả. Silver bỏ qua
ngày có file Silver mới hơn mọi file Bronze; dùng `FORCE=1` (hoặc `--force`) để
rebuild. Một lần rebuild không tạo ra dòng hợp lệ nào sẽ **giữ nguyên** partition
Silver/Gold cũ thay vì xoá nó; chỉ quarantine và `dq_hourly` mới được dọn.

## 2. Silver

| Silver | Từ Bronze | Việc làm |
|---|---|---|
| `bars_1s`, `bars_1m` | `klines_1s`, `klines_1m` | Dedupe theo `(symbol, open_time)` giữ bản `ingested_at` mới nhất; thêm `market`; thêm `taker_sell_base/quote` |
| `trades` | `agg_trades` | Dedupe theo `(symbol, agg_trade_id)` (Bronze chỉ dedupe trong từng giờ); thêm `notional`, `aggressor_side` |
| `funding` | `funding_rate` | `mark_price = 0` được coi là thiếu và đổi thành `NULL`; DQ kiểm tra độ mới của funding |
| `open_interest` | `open_interest` | Kiểm tra giá trị âm/null |
| `dq_hourly` | (do bước DQ sinh ra) | Trạng thái chất lượng từng giờ |

Spot và futures dùng chung `symbol`/`exchange` ở Bronze nên Silver thêm cột `market`
(`spot`, `usdm_futures`) để không bị lẫn.

**Quarantine**: dòng vi phạm (giá ≤ 0, `high < low`, `taker_buy > volume`, null…) không
bị xoá mà chuyển sang `dataset=<tên>_quarantine` kèm cột `reject_reason`. Quarantine
không được đẩy lên S3 và không có view trong catalog.

## 3. Cổng chất lượng dữ liệu (DQ)

`crypto-ingest dq` ghi bảng `dq_hourly` (mỗi dataset × symbol × giờ) với `status`:

| Status | Ý nghĩa |
|---|---|
| `missing` | Giờ đã kết thúc nhưng không có dòng nào; với `funding`: không có funding point nào trong 8 giờ gần nhất |
| `partial` | Có dòng nhưng ít hơn ngưỡng tối thiểu (1s: 3000, 1m: 55) |
| `id_gaps` | `agg_trade_id` nhảy cóc, có trade bị thiếu |
| `ok` | Đạt |

Quy tắc: giờ chưa kết thúc và các giờ trước dữ liệu đầu tiên của lake không bị tính là
`missing`. Ngưỡng `partial` lấy trực tiếp từ `schemas.MIN_ROWS_PER_HOUR` nên không bị
lệch với lúc ingest. Biến `CRYPTO_DQ_FAIL_ON` (mặc định `missing`) chọn status nào làm
lệnh thoát mã 1 / task Airflow fail. Gold **không** được dựng khi cổng fail. Task DQ cũng
fail nếu bất kỳ task ingest nào lỗi, kể cả dataset không có luật riêng. `crypto-ingest
dq -d bars_1m -d trades` chỉ kiểm tra các dataset được chọn.

Cột nguồn trong `dq_hourly` tên là `dataset_name` (không phải `dataset`) để không bị
partition `dataset=dq_hourly` che khi DuckDB đọc Hive.

Khi DQ fail, Airflow vẫn refresh catalog và đẩy Bronze lên S3, nhưng **không** đẩy
Silver/Gold. Chạy `crypto-ingest sync-s3` thủ công vẫn có thể đẩy layer nếu bạn muốn.
`sync-s3 -d klines_1m` chỉ đẩy Silver dataset tương ứng (`bars_1m` cộng thêm
`dq_hourly`); Gold vẫn đẩy đủ vì mọi mart đều dựng từ Silver được cấu hình.

Đặt `CRYPTO_MEDALLION_ENABLED=0` để tạm quay lại luồng chỉ có Bronze: DAG bỏ qua
Silver/DQ/Gold (không tạo group `silver_gold`) và S3 không đẩy layer. Các lệnh CLI
`silver`/`dq`/`gold` vẫn chạy bình thường khi cần backfill thủ công.

## 4. Gold marts

| Mart | Khoá | Cột chính |
|---|---|---|
| `ohlcv` | `symbol, interval (5m/1h/1d), bucket` | OHLC, volume, quote_volume, trades, vwap, `n_bars`, `is_complete` |
| `market_hourly` | `symbol, hour_start` | OHLCV giờ, `realized_vol` (từ log-return nến 1m), `taker_buy_share`, `aggressor_imbalance` từ trade |
| `derivatives_hourly` | `symbol, hour_start` | `oi_open/close/change`, `oi_value_close`, `last_funding_rate` (ASOF join) |
| `data_quality_daily` | `dataset, symbol` | `hours_ok/missing/partial`, `pct_ok` |

## 5. Lệnh

```bash
make silver            # Bronze -> Silver, 36 giờ gần nhất (HOURS=72 để đổi)
make silver FORCE=1    # rebuild cả những ngày Silver đã mới hơn Bronze
make dq                # ghi dq_hourly, dừng nếu có giờ bị thiếu
make gold              # Silver -> Gold
make layers            # cả ba bước, Gold bị bỏ qua nếu DQ fail
make catalog           # thêm view bronze_*, silver_*, gold_* vào DuckDB

crypto-ingest silver --start 2026-10-01 --end 2026-10-08 -d bars_1m
crypto-ingest dq -d bars_1m -d trades --fail-on missing,id_gaps
crypto-ingest gold -m ohlcv -m market_hourly
crypto-ingest sync-s3 --bronze-only    # chỉ đẩy Bronze
crypto-ingest sync-s3 -d klines_1m     # Bronze + Silver bars_1m + Gold
```

Airflow (3 task group): `bronze/ingest_* → silver_gold/build_silver_layer →
silver_gold/dq_check → silver_gold/build_gold_layer → serve/refresh_catalog +
serve/publish_to_s3`. Task_id bên trong group giữ nguyên; tên đầy đủ có tiền tố
group (ví dụ `bronze.ingest_klines_1m`).
Tắt toàn bộ tầng layer bằng `CRYPTO_MEDALLION_ENABLED=0`: DAG chỉ còn `bronze/ingest_*`
và `serve/refresh_catalog` + `serve/publish_to_s3`.

## 6. Query

```sql
-- nến 1h đã hoàn chỉnh
SELECT * FROM gold_ohlcv WHERE "interval" = '1h' AND is_complete AND symbol = 'BTCUSDT';

-- volatility và áp lực mua/bán theo giờ
SELECT hour_start, realized_vol, aggressor_imbalance FROM gold_market_hourly ORDER BY hour_start;

-- độ sạch dữ liệu theo ngày
SELECT date, dataset, pct_ok FROM gold_data_quality_daily ORDER BY date DESC;
```

## 7. Giới hạn

- `realized_vol` của giờ đầu ngày dùng thêm nến cuối của ngày trước nếu Silver có.
- Nến 1s không tồn tại cho giây không có giao dịch, nên `bars_1s` thưa là bình thường.
- Giá/khối lượng vẫn là `float64`, chưa chuyển sang decimal.
- Luồng này đã chạy end-to-end trên 1 ngày dữ liệu thật; vẫn nên chạy `make test` rồi
  thử `make layers` trên 1–2 ngày trước khi bật cho toàn bộ lịch sử.
