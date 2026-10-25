---
id: 02
title: price_impact sai thứ nguyên, nổ khi net signed notional ≈ 0
severity: P1
status: open
area: metrics
files:
  - B4_analysis/src/metrics_ofi_impact.py
evidence:
  - issue/evidence/verified-ok.md
repro: null
---

# 02 — `price_impact` sai thứ nguyên

## Hiện tượng

`metrics_ofi_impact.py:40-41`:

```python
signed_notional = float((g["signed_qty"] * g["price"].astype(float)).sum())
impact = float(ret / signed_notional) if signed_notional != 0 else float("nan")
```

`impact` có đơn vị **1/USD**: chia return của giờ cho tổng notional ròng có dấu.

## Ảnh hưởng

- Trong một giờ có mua/bán gần cân bằng, `signed_notional → 0` → `impact` bùng nổ
  (có thể 1e-6…1e6), hoàn toàn không phải "tác động giá".
- `impact_mean` trong notebook (cell 4) bị chi phối bởi vài giờ như vậy → con số vô nghĩa.
- Báo cáo chưa trích số này (may mắn), nhưng chỉ cần một lần ai đó đưa vào slide là sai.

## Nguyên nhân

Nhầm giữa "return trên mỗi đơn vị dòng lệnh" (cần hồi quy) và "return chia tổng dòng lệnh"
(chỉ đúng nếu quan hệ tuyến tính và mẫu số xa 0). Với net flow, mẫu số có thể triệt tiêu.

## Cách sửa

**A. Kyle's lambda theo giờ (khuyến nghị)** — hồi quy thay đổi giá lên dòng lệnh ròng luỹ kế
trong giờ, hệ số là impact biên:

```python
# trong mỗi (symbol, hour)
g = g.sort_values("transact_time")
cum_signed = g["signed_qty"].cumsum()
p = g["price"].astype(float)
dp = p.diff().fillna(0.0)
mask = cum_signed != 0
lam = np.nan
if mask.sum() >= 10 and np.ptp(cum_signed[mask]) > 0:
    lam = float(np.polyfit(cum_signed[mask], dp[mask], 1)[0])
```

**B. Bỏ hẳn `price_impact`**, chỉ giữ OFI (đã ổn và có ý nghĩa), và nói rõ trong báo cáo
là impact để phần sau.

**C. Nếu vẫn muốn bản đơn giản**: dùng `|ret| / dollar_volume` (giống Amihud) và đặt tên
đúng là `amihud_hourly` — đừng gọi là impact.

## Nghiệm thu

- [ ] Không còn phép chia cho `signed_notional`.
- [ ] `impact` (hoặc tên mới) có phân phối hợp lý: median dương, không có outlier > 1e3× median.
- [ ] Notebook/báo cáo dùng đúng tên và đơn vị.
