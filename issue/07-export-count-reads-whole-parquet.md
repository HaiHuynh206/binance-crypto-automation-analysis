---
id: 07
title: Đọc lại toàn bộ parquet chỉ để đếm số dòng
severity: P2
status: open
area: pipeline
files:
  - B4_analysis/src/compute_from_r2.py
---

# 07 — Memory spike khi export

## Hiện tượng

`compute_from_r2.py:44`:

```python
con.execute(sql)                                  # COPY ... TO out_path
n = len(pd.read_parquet(out_path))                # đọc lại TOÀN BỘ vào pandas
```

## Ảnh hưởng

- `klines_1s` 3 ngày × 2 symbol ≈ 0.5M dòng → tạm ổn.
- `agg_trades` cùng kỳ có thể 5–20M dòng → pandas ngốn ~1–3 GB RAM chỉ để lấy một con số,
  dễ OOM trên máy 8 GB hoặc khi mở rộng sang 7 ngày / thêm symbol.
- Ngoài ra còn I/O đọc lại file vừa ghi.

## Cách sửa

Đếm bằng DuckDB, không kéo dữ liệu vào pandas:

```python
con.execute(sql)
n = con.execute(
    f"SELECT count(*) FROM read_parquet('{out_path.as_posix()}')"
).fetchone()[0]
```

Hoặc lấy luôn từ `COPY` bằng cách `RETURNING`/`COPY ... TO ... (FORMAT PARQUET)` rồi
`SELECT count(*)` trên nguồn (s3 glob) — nhưng cách trên là đủ.

## Nghiệm thu

- [ ] Không còn `pd.read_parquet(out_path)` chỉ để đếm.
- [ ] Export `agg_trades` 3 ngày chạy với peak RAM < 1 GB.
