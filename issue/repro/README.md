# Repro scripts

Chạy được offline, **không cần** `.env` hay key R2.

| Script | Cho issue | Cần gì | In ra |
|--------|-----------|--------|-------|
| `roll_spread_nan.py` | [01](../01-roll-spread-nan.md) | `pandas`, `numpy` | tỉ lệ NaN của Roll trên 3 kịch bản dữ liệu 1 giây |
| `duckdb_hive_check.py` | [verified-ok](../evidence/verified-ok.md) | `duckdb`, `numpy` | xác nhận đọc lake hive + filter + COPY chạy đúng |

```bash
python issue/repro/roll_spread_nan.py
python issue/repro/duckdb_hive_check.py
```

Kết quả mong đợi:

```
60% giây đứng giá + 40% bounce ±0.5              NaN rate = 45.x%
bounce thuần ±0.5 (mọi giây đều có trade)        NaN rate = 47.x%
increments tự tương quan dương φ=0.1             NaN rate = 100.0%
```

```
--- read hive, không filter        rows=6 symbols=['BTCUSDT','ETHUSDT','SOLUSDT']
--- filter symbol + date           rows=4 symbols=['BTCUSDT','ETHUSDT']      # SOL bị loại đúng
--- COPY TO local parquet          rows=4 symbols=['BTCUSDT','ETHUSDT']
```

Nếu numbers lệch nhẹ do seed/numpy version thì vẫn cùng kết luận (thứ tự độ lớn).

## Script tái hiện cần dữ liệu thật (sẽ thêm khi sửa issue 01)

- `roll_from_trades_check.py` — so `roll_effective_spread` trên 1s close vs trade price
  (cần `data/clean/r2/*.parquet` từ `compute_from_r2`).
