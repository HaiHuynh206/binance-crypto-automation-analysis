---
id: 05
title: Cache --skip-export không phân biệt symbol
severity: P2
status: open
area: pipeline
files:
  - B4_as_requested/src/compute_from_r2.py
---

# 05 — Cache key thiếu `--symbols`

## Hiện tượng

`compute_from_r2.py:86-87`:

```python
kl_path = cache / f"klines_1s_{args.date_from}_{args.date_to}.parquet"
ag_path = cache / f"agg_trades_{args.date_from}_{args.date_to}.parquet"
```

Tên file cache chỉ gồm khoảng ngày. `--symbols` không tham gia vào key.

## Ảnh hưởng

```bash
python -m src.compute_from_r2 --symbols BTCUSDT                    # tạo cache
python -m src.compute_from_r2 --symbols ETHUSDT --skip-export      # dùng lại cache BTC
```

→ vòng lặp per-symbol sẽ chết ở `missing symbol data on R2: ETHUSDT` (nếu fail-hard),
hoặc tệ hơn: nếu ETH có trong cache cũ thì bạn tưởng mình vừa tính ETH mới nhưng thực ra
đang đọc dữ liệu cũ. Không có log nào cho biết cache thuộc về symbol nào.

## Cách sửa

Đưa symbol (đã sort, join) vào tên file:

```python
tag = "-".join(sorted(symbols))
kl_path = cache / f"klines_1s_{tag}_{args.date_from}_{args.date_to}.parquet"
ag_path = cache / f"agg_trades_{tag}_{args.date_from}_{args.date_to}.parquet"
```

Hoặc chắc hơn: bỏ `--skip-export`, thay bằng kiểm tra "cache có đủ symbol không" trước khi
dùng lại:

```python
if args.skip_export and kl_path.exists():
    have = set(pd.read_parquet(kl_path, columns=["symbol"])["symbol"].unique())
    if not set(symbols) <= have:
        print("Cache missing symbols, re-exporting")
        args.skip_export = False
```

## Nghiệm thu

- [ ] Hai lệnh chạy với symbol khác nhau tạo ra 2 file cache khác nhau.
- [ ] `--skip-export` với symbol chưa có trong cache sẽ re-export hoặc báo lỗi rõ ràng.
