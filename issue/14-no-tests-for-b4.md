---
id: 14
title: Không có test nào cho metrics B4
severity: P3
status: open
area: testing
files:
  - B4_analysis/src/
  - tests/
---

# 14 — B4 không có test

## Hiện tượng

`tests/` chỉ có test cho package ingest (`test_pipeline.py`, `test_schemas.py`,
`test_storage.py`, `test_s3.py`, `test_timeutil.py`). Không có test nào chạm vào
`B4_analysis/src/*.py` — kể cả các hàm thuần toán học rất dễ test.

## Ảnh hưởng

- Lỗi [01](01-roll-spread-nan.md) lẽ ra bị bắt bởi một test 5 dòng ("Roll phải khác NaN trên
  chuỗi bid-ask bounce tổng hợp").
- Không có lưới an toàn khi sửa metric: đổi công thức Amihud/VR xong không biết có làm vỡ gì.
- Không chứng minh được "đã kiểm tra" khi bảo vệ.

## Cách sửa — bộ test tối thiểu (không cần R2, không cần data/)

```python
# tests/test_b4_metrics.py
import numpy as np, pandas as pd
from B4_analysis.src.metrics_roll_amihud import roll_effective_spread
from B4_analysis.src.metrics_variance_ratio import variance_ratio
from B4_analysis.src.metrics_ofi_impact import signed_volume

def test_roll_recovers_known_spread():
    # bounce ±s/2 quanh 100 -> Roll ~ s
    prices = 100 + np.tile([0.05, -0.05], 200)
    assert abs(roll_effective_spread(pd.Series(prices)) - 0.10) < 0.02

def test_roll_nan_on_flat_series():
    assert np.isnan(roll_effective_spread(pd.Series([100.0] * 100)))

def test_vr_close_to_one_on_random_walk():
    rng = np.random.default_rng(0)
    r = rng.normal(0, 0.001, 20_000)
    assert abs(variance_ratio(r, 5) - 1.0) < 0.15

def test_ofi_sign():
    df = pd.DataFrame({"quantity": [1.0, 2.0], "is_buyer_maker": [False, True]})
    assert list(signed_volume(df)) == [1.0, -2.0]
```

(`B4_analysis` chưa phải package import được từ root — hoặc thêm `B4_analysis/__init__.py`,
hoặc dùng `sys.path.insert` như các module trong `src/` đang làm.)

## Nghiệm thu

- [ ] Có ít nhất 1 test cho mỗi metric (Roll, Amihud, OFI, VR).
- [ ] Test chạy offline, < 5 giây, không cần `.env`.
- [ ] `pytest tests/test_b4_metrics.py` xanh.
