# Nên lưu dữ liệu crypto tần suất cao ở đâu?

Tài liệu này trả lời câu hỏi storage cho pipeline trong repo: dữ liệu dạng
append-only, query theo `symbol + thời gian`, ghi theo lô 2 giờ, khối lượng tăng dần.

## 1. Đặc điểm dữ liệu → tiêu chí chọn storage

| Đặc điểm | Hệ quả |
|---|---|
| Append-only, gần như không UPDATE/DELETE | Không cần transaction phức tạp, ưu tiên ghi tuần tự |
| Query theo `symbol`, `time range`, aggregate nến | Cần partition/clustering theo thời gian |
| Ít cột, nhiều dòng (trade) | Columnar + nén tốt thắng row-store |
| Batch 2 giờ, không cần realtime | Không bắt buộc Kafka/streaming |
| Dữ liệu có thể tái tạo từ API | Có thể chấp nhận backup lỏng hơn, nhưng vẫn nên backup |

## 2. So sánh các lựa chọn

| Giải pháp | Query | Nén | Vận hành | Phù hợp khi |
|---|---|---|---|---|
| **Parquet + DuckDB (local/S3)** | Rất nhanh trên GB–vài trăm GB | Rất tốt (zstd, dictionary) | Gần như bằng 0 | 1 máy, analyst, batch, chi phí thấp |
| **ClickHouse** | Sub-second trên hàng tỷ dòng, nhiều user | Rất tốt (codec chuyên dụng) | Trung bình (server, backup, upgrade) | Dashboard realtime, nhiều concurrent query |
| **TimescaleDB** | Nhanh vừa, SQL quen thuộc | Khá (compression) | Trung bình | Đã có Postgres, dữ liệu < ~vài trăm triệu dòng |
| **Postgres thường** | Chậm dần khi > 50–100M dòng | Kém | Thấp | Prototype rất nhỏ, không phải trade tick |
| **MongoDB** | Kém cho scan/aggregate lớn | Kém hơn columnar | Trung bình | Không khuyến nghị cho use case này |
| **BigQuery / Athena** | Rất nhanh, serverless | Tốt | Thấp | Team đã ở cloud, query không thường xuyên |
| **CSV/JSON trên disk** | Rất chậm | Kém (5–15x so với Parquet) | Thấp | Chỉ để trao đổi dữ liệu, không phải storage |

**Kết luận: không có lý do gì để dùng row-store cho trade tick ở quy mô này.**

## 3. Khuyến nghị theo giai đoạn

### Giai đoạn 1 — hiện tại (máy này, 1 người dùng, vài symbol)

**Parquet + Zstandard, partition Hive theo giờ, query bằng DuckDB.**

- Đúng với code trong repo: `data/lake/exchange=.../dataset=.../symbol=.../date=.../hour=...`
- Ưu điểm: 0 đồng hạ tầng, không server, backup = copy thư mục, DuckDB đọc trực tiếp.
- Chỉ cần ổ SSD còn trống. Máy hiện tại còn **~181 GB**.
- Nên backup `data/lake` + `data/_state` (state nhỏ, nhưng giữ để resume backfill).

### Giai đoạn 2 — khi dữ liệu lớn hơn hoặc cần chia sẻ

**MinIO (S3-compatible) hoặc S3 làm lake, giữ nguyên layout partition.**

- DuckDB đọc S3 qua extension `httpfs`; Athena/BigQuery External Table cũng đọc được.
- Giữ 30–90 ngày "hot" trên local SSD, phần cũ đẩy lên object storage.
- Không cần đổi code ghi file nhiều: có thể sync bằng `aws s3 sync`/`mc mirror`,
  hoặc thay `ParquetLake` bằng implementation dùng `pyarrow.fs.S3FileSystem`.

### Giai đoạn 3 — dashboard, nhiều người query, sub-second

**ClickHouse** (giữ Parquet làm lớp raw/backup).

```sql
CREATE TABLE trades (
    symbol          LowCardinality(String),
    agg_trade_id    UInt64,
    price           Float64,
    qty             Float64,
    trade_time      DateTime64(3, 'UTC'),
    is_buyer_maker  Bool
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_time)
ORDER BY (symbol, trade_time)
TTL trade_time + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;
```

Nạp từ Parquet bằng `INSERT INTO trades SELECT ... FROM file('...parquet')` hoặc
`clickhouse-client`/`clickhouse-local`. ClickHouse nén trade tick thường 10–20x,
query theo symbol + time range rất nhanh.

### Giai đoạn 4 — cần realtime dưới 1 phút

Kafka/Redpanda (WebSocket ingest) → ClickHouse. Batch 2 giờ của repo này không phù
hợp cho mục tiêu đó; kiến trúc sẽ khác (streaming + exactly-once hoặc ít nhất
at-least-once kèm dedupe).

## 4. Sizing tham khảo

Công thức: `rows/ngày × bytes/row sau nén`.

| Dataset | Rows/ngày/symbol | Dung lượng nén ước tính |
|---|---|---|
| `klines_1s` | 86.400 | 2–5 MB |
| `klines_1m` | 1.440 | 50–100 KB |
| `agg_trades` | 0,3–1,5 triệu (BTC/ETH) | 10–60 MB |
| `funding_rate` | 3 | < 1 KB |
| `open_interest` | 288 | < 20 KB |

Với 5 symbol, chủ yếu là `agg_trades`:

- ~30–150 MB/ngày → **~10–55 GB/năm** (đo thực tế 1 giờ BTCUSDT: 30k trade ≈ 400 KB).
- 181 GB hiện có đủ cho **nhiều năm**; giới hạn thực tế thường là nhu cầu truy vấn
  và backup chứ không phải dung lượng.
- Nến 1s/1m gần như không đáng kể (~3 MB/ngày/symbol với 1s).
- Con số thực tế phụ thuộc mức độ sôi động của thị trường; hãy chạy thử 1 ngày rồi đo
  bằng `crypto-ingest status`.

## 5. Những điều nên làm ngay cả khi dùng local

1. **Layout partition cố định** như repo (ổn định cho mọi engine sau này).
2. **File 1 giờ/partition**, ghi atomic, không append mù vào file đang có.
3. **Nén zstd**; không giữ JSON/CSV làm bản chính.
4. **Backup định kỳ**: `restic`/`rsync` sang ổ ngoài hoặc S3. Dữ liệu Parquet bất biến
   nên backup rất an toàn.
5. **Theo dõi dung lượng** (`crypto-ingest status`) và đặt ngưỡng cảnh báo ở 70% disk.
6. **Retention policy** gợi ý:
   - `klines_1m`, `klines_1s`: giữ lâu (rất nhỏ).
   - `agg_trades`: giữ 6–12 tháng raw; nếu cần lịch sử dài, aggregate thành nến 1m
     rồi xoá raw cũ.
7. **Compaction** khi số file quá lớn (> ~100k file/dataset): gộp các file giờ thành
   file ngày/tháng bằng DuckDB/Spark, xoá file gốc sau khi kiểm tra. Với quy mô hiện
   tại chưa cần.

## 6. Không nên

- Lưu trade tick vào **Postgres thường** hoặc **Airflow metadata DB**.
- Lưu một file CSV khổng lồ; không partition, không nén.
- Dùng **MongoDB** cho aggregate time-series nặng.
- Coi local disk là nơi lưu duy nhất nếu dữ liệu quan trọng (không backup).
- Ghi trực tiếp từng trade vào ClickHouse theo batch 2 giờ nếu chưa cần realtime —
  Parquet trước, ClickHouse sau, sẽ đơn giản và rẻ hơn.

## 7. Tóm tắt 1 dòng

**Bắt đầu với Parquet + DuckDB trên SSD local (repo này đã sẵn sàng); chuyển sang
MinIO/S3 khi cần chia sẻ/backup, và ClickHouse khi cần dashboard sub-second trên
hàng tỷ dòng.**
