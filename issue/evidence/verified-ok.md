# Verified OK — những thứ đã kiểm tra và KHÔNG phải bug

Ghi lại để lần sau không điều tra lại. Ngày review: 2026-10-24, commit `5a37e3a`.

## 1. Hive partitioning + filter đọc đúng

Layout lake (`src/crypto_ingest/storage.py`):

```
lake/exchange=binance/dataset=klines_1s/symbol=BTCUSDT/date=2026-10-19/hour=22/part-00000.parquet
```

`compute_from_r2.export_dataset()` đọc bằng:

```sql
read_parquet('<glob>', hive_partitioning=true)
WHERE symbol IN (...) AND date >= DATE '...' AND date <= DATE '...'
```

Điểm nghi ngờ: file parquet **đã có cột `symbol`** trong khi `symbol=` cũng là partition key
→ có thể xung đột/tạo cột trùng. Kiểm tra thực tế trên DuckDB 2.0.0 với lake giả cùng layout:

```
--- plain hive read, no filter
[('BTCUSDT', ..., 'klines_1s', datetime.date(2026,10,19), 'binance', '22'), ...]

--- with symbol IN filter + date range
[('BTCUSDT', ...), ('BTCUSDT', ...), ('ETHUSDT', ...), ('ETHUSDT', ...)]   # SOL bị loại đúng

--- COPY TO local parquet
OK, cùng kết quả
```

Kết luận: **không lỗi**. Cột `symbol` trong file thắng partition key (không sinh cột trùng),
filter đẩy xuống pruning đúng. Script: `repro/duckdb_hive_check.py`.

Ghi chú phụ: kết quả export có thêm các cột `exchange`, `dataset`, `date`, `hour` — vô hại
nhưng nếu muốn sạch thì `SELECT symbol, open_time, ...` thay vì `SELECT *`.

## 2. Chiều dấu OFI

`metrics_ofi_impact.signed_volume()`: `is_buyer_maker=True` → người mua là maker → kẻ chủ
động là **bên bán** → signed volume âm. Code:

```python
buy_aggressor = ~df["is_buyer_maker"].astype(bool)
sign = np.where(buy_aggressor, 1.0, -1.0)
```

Đúng quy ước Binance. `test_ofi_sign` trong issue 14 khoá lại hành vi này.

## 3. Schema lake ↔ code

| Cột lake (`schemas.py`) | Code dùng |
|---|---|
| `klines_1s`: `open_time, close, volume, quote_volume, trades` | `klines_for_metrics()` map `trades`→`n_trades` (không dùng), còn lại khớp |
| `agg_trades`: `price, qty, trade_time, is_buyer_maker, symbol` | `agg_for_metrics()` rename `qty`→`quantity`, `trade_time`→`transact_time` |

Không thiếu cột nào → không có `KeyError` tiềm ẩn ở 2 nhánh chính.

## 4. Encoding

Toàn bộ `.py`, `.md`, `.ipynb`, `.csv`, `.env.example` trong `B4_analysis/` là UTF-8 hợp lệ,
không có replacement char (U+FFFD). Chữ méo khi xem bằng PowerShell chỉ là codepage console.

## 5. Số liệu báo cáo ↔ CSV

| Báo cáo (`B4_bao_cao.md:35-36`) | `docs/results/summary_r2_2026-10-19_2026-10-21.csv` |
|---|---|
| BTC Amihud 2.45e-9 | 2.4451947582097537e-09 ✓ |
| ETH Amihud 5.25e-8 | 5.245659992710176e-08 ✓ |
| BTC OFI +0.014 | 0.014141654600300434 ✓ |
| ETH OFI +0.021 | 0.020887464736634786 ✓ |
| VR 1.00/0.95/0.89 & 0.98/0.98/0.88 | khớp ✓ |

Tỉ lệ Amihud ETH/BTC = 21.45× ✓ (báo cáo ghi ~21×). `n_hours = 50` cho cả hai ✓.

## 6. Secret trong git history

```
git log --all --name-only | grep -E "\.env|secret|credential"
→ chỉ có .env.example (key để trống)
```

Không có `.env` thật hay key nào từng bị commit.

## 7. PNG trong docs

3 file `docs/figures/r2_*.png` đều là PNG hợp lệ (magic bytes `89504e470d0a1a0a`), không phải
file rỗng/hỏng.
