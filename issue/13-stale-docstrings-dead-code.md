---
id: 13
title: Docstring sai và code chết
severity: P3
status: open
area: code quality
files:
  - B4_analysis/src/metrics_roll_amihud.py
  - B4_analysis/src/compute_from_r2.py
  - B4_analysis/src/metrics_ofi_impact.py
---

# 13 — Docstring không khớp code, code chết

## Hiện tượng

**a) Docstring mô tả việc không làm** — `metrics_roll_amihud.py:39-41`:

```
Expect columns: open_time (UTC), close, volume, quote_volume, symbol.
Aggregates to hour then computes Roll on close path and Amihud on hourly bars.
Also returns within-hour Roll using 1s closes.
```

Code thực tế (`:54`) chỉ tính Roll **within-hour** trên close 1 giây; không có "Roll on close
path" cấp giờ nào được trả ra, và Amihud cũng là per-giây chứ không phải "on hourly bars".

**b) Code chết** — `compute_from_r2.py:48-53`:

```python
if "n_trades" not in d.columns and "trades" in d.columns:
    d["n_trades"] = d["trades"]
```

`n_trades` không được dùng ở bất kỳ đâu phía sau (metrics tự đếm `len(g)`).

**c) Comment lệch code** — `metrics_ofi_impact.py:19-22` nói "regression delta_mid ~ signed_volume
(simple OLS slope per hour)" nhưng code không hồi quy gì, chỉ chia return cho tổng
(xem [02](02-price-impact-wrong-unit.md)).

## Ảnh hưởng

Người đọc (và chính bạn sau 2 tuần) tin vào docstring → tính lại sai chỗ, hoặc tưởng đã có
regression trong khi chưa. Đây cũng là chỗ dễ bị hỏi khi bảo vệ.

## Cách sửa

- Viết lại docstring theo đúng code hiện tại, hoặc sửa code cho khớp docstring (tốt hơn).
- Xoá block `n_trades` nếu không dùng.
- Sửa comment cell OFI cho khớp công thức thực tế (hoặc sửa công thức — xem 02).

## Nghiệm thu

- [ ] Mỗi hàm: docstring mô tả đúng input/output/đơn vị như code làm.
- [ ] Không còn cột/biến được tạo ra mà không ai đọc.
