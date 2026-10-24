---
id: 06
title: Không có data-quality gate (số dòng / giờ thiếu)
severity: P2
status: open
area: pipeline
files:
  - B4_as_requested/src/compute_from_r2.py
  - B4_as_requested/docs/B4_bao_cao.md
---

# 06 — Thiếu kiểm tra chất lượng dữ liệu

## Hiện tượng

`compute_from_r2.py` chỉ kiểm tra:

```python
if n1 == 0 or n2 == 0:            # export rỗng
if k.empty or a.empty:            # thiếu hẳn symbol
```

Không có kiểm tra **mỗi giờ có đủ dữ liệu không**. Trong khi đó repo gốc đã có sẵn
`src/crypto_ingest/schemas.py: MIN_ROWS_PER_HOUR` (klines_1s ≥ 3000, agg_trades ≥ 1) nhưng
B4 không dùng.

## Ảnh hưởng

- `n_hours = 50` trong `docs/results/summary_*.csv` chỉ đếm số nhóm giờ **có ít nhất 1 dòng**.
  Một giờ thiếu 40% số giây vẫn được tính là 1 giờ đầy đủ → Amihud/VR của giờ đó bị lệch.
- Báo cáo `B4_bao_cao.md:39,46` khẳng định "không bịa giờ thiếu" và "QA fail-hard" — nhưng
  code hiện tại không có gì đảm bảo điều đó. Đây là chỗ dễ bị hỏi nhất khi bảo vệ.

## Cách sửa

Thêm bước kiểm tra ngay sau khi tính metric, trước khi ghi parquet:

```python
def assert_hourly_coverage(df, dataset, min_rows=3000):
    counts = df.groupby(df["open_time"].dt.floor("h")).size()
    bad = counts[counts < min_rows]
    if len(bad):
        raise RuntimeError(
            f"{dataset}: {len(bad)} giờ thiếu dữ liệu "
            f"(min={min_rows}): {list(bad.index[:5])}"
        )
```

Hoặc mềm hơn: log cảnh báo + ghi cột `is_complete` vào metrics, rồi báo cáo chỉ dùng
`is_complete == True` và nêu rõ "N giờ bị loại".

## Nghiệm thu

- [ ] Có assert/log cho mỗi giờ (klines ≥ 3000 dòng, agg_trades ≥ 1).
- [ ] Số giờ trong summary khớp với số giờ đã qua gate, và báo cáo nói rõ đã loại bao nhiêu.
- [ ] Câu "QA fail-hard" trong báo cáo là đúng với code.
