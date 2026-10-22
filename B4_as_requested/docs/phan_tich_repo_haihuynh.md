# Phân tích repo `binance-crypto-automation-analysis`

**Clone:** `group_4/binance-crypto-automation-analysis/`  
**Upstream:** https://github.com/HaiHuynh206/binance-crypto-automation-analysis

## Đây là gì?

**Platform ingest crypto 24/7**, không phải notebook phân tích chất lượng thị trường.

- Nguồn: Binance **REST API** (live), không phải zip trên `data.binance.vision`
- Orchestration: Airflow DAG `crypto_market_ingest` (đúng DAG đang chạy trên lab UI)
- Lưu: Parquet lake partition `exchange/dataset/symbol/date/hour`
- Catalog: DuckDB
- Publish: S3 / Cloudflare R2 / MinIO + `_manifest.json`
- Deploy: Docker Compose, VPS, Tailscale Funnel (khớp URL lab `.ts.net`)

## Kiến trúc (tóm tắt)

```
Binance REST → crypto_ingest (rate limit, retry, pagination)
  → data/lake/.../hour=.../part.parquet
  → data/_state (idempotency markers)
  → catalog.duckdb
  → (optional) S3/R2
```

DAG mỏng (`dags/crypto_ingest_dag.py`): 5 task `ingest_*` → `refresh_catalog` + `publish_to_s3`.  
Logic thật nằm trong package `src/crypto_ingest/`.

## Dataset

| Dataset | API | Ghi chú |
|---------|-----|---------|
| klines_1s / 1m | `/api/v3/klines` | Spot |
| agg_trades | `/api/v3/aggTrades` | Nặng, paginate `fromId` |
| funding_rate | futures | 8h/lần |
| open_interest | futures hist | Chỉ ~30 ngày gần nhất |

Mặc định symbol: `BTCUSDT,ETHUSDT`. Schedule: `0 */2 * * *` UTC, lookback 24h.

## Điểm thiết kế hay

1. **Idempotent theo giờ** — marker `_state`; chạy lại gần như free.
2. **Atomic Parquet / hour** — fail không để file dở.
3. **Rate limit trong code** — không phụ thuộc Airflow.
4. **DAG mỏng** — CLI `crypto-ingest` chạy được không cần Airflow.
5. **Có test** (không gọi mạng).

## So với bài B4 `group_4` hiện tại

| | Repo HaiHuynh (platform) | group_4 B4 (nghiên cứu) |
|--|--------------------------|-------------------------|
| Mục tiêu | Thu thập + chia sẻ lake liên tục | Đo chất lượng TT (Amihud, Roll, OFI, VR) + phản biện VN |
| Nguồn | REST live | Vision zip lịch sử + LOBSTER sample |
| Airflow | DAG production `crypto_market_ingest` | DAG local `b4_market_quality` (chưa deploy) |
| Metrics microstructure | **Không có** | Có |
| Báo cáo / checklist VN | Không | Có |

**Kết luận:** repo này = **hạ tầng dữ liệu** mà lab Airflow đang chạy. B4 của nhóm = **tầng phân tích** trên (hoặc song song với) dữ liệu đó.

## Cách dùng cho nhóm (không cần clone lại logic)

1. **Demo Airflow:** UI lab → DAG `crypto_market_ingest` (đúng code repo này).
2. **Lấy data dài hơn cho B4 (tuỳ chọn):**
   - Cài repo, `make install`, cấu hình `.env`, chạy  
     `crypto-ingest backfill --days 14 -d klines_1s,agg_trades`  
     với `CRYPTO_SYMBOLS=BTCUSDT,ADAUSDT`
   - Hoặc đọc lake/S3 nếu lab đã publish (xem `docs/consumer.md`, `docs/share-s3.md`) — **cần credential R2/S3 thật, không đoán**.
3. **Giữ B4 metrics** trên Parquet group_4; có thể viết adapter đọc layout lake của repo này sau.

## Việc không nên làm

- Không trộn nhầm: repo này **không thay** phân tích Roll/Amihud/phản biện VN.
- Không chạy DAG local + VPS lab cùng lúc (README cảnh báo trùng ingest).
- Không commit `.env` / AWS keys của repo này.
