---
id: 04
title: VR(q) chỉ còn 6 block/giờ và không có khoảng tin cậy
severity: P1
status: open
area: metrics
files:
  - B4_analysis/src/metrics_variance_ratio.py
  - B4_analysis/docs/B4_bao_cao.md
---

# 04 — Variance ratio thiếu mẫu, thiếu CI

## Hiện tượng

`metrics_variance_ratio.py:21` chia return 1 phút thành các block **không chồng lấn**:

```python
rq = np.array([r[i : i + q].sum() for i in range(0, len(r) - q + 1, q)])
```

Trong 1 giờ chỉ có ~60 return 1 phút → `q=2` còn 30 block, `q=5` còn 12, **`q=10` còn 6**.
Báo cáo (`B4_bao_cao.md:35-36`) trình bày `VR(2/5/10) = 1.00/0.95/0.89` như một phát hiện,
không có sai số.

## Ảnh hưởng

- Với 6 quan sát, `np.var(rq, ddof=1)` có độ bất định cực lớn; VR(10) = 0.88 vs 1.00 có thể
  chỉ là nhiễu. Kết luận "mean-reversion" chưa đủ cơ sở.
- Số block ít còn tạo bias hệ thống (Lo–MacKinlay dùng block chồng lấn chính vì lý do này).

## Nguyên nhân

Tính VR trong phạm vi 1 giờ thay vì trên toàn chuỗi, và dùng công thức rút gọn không kèm
phương sai tiệm cận.

## Cách sửa

**A. Block chồng lấn + SE (khuyến nghị)** — với `n` quan sát:

```python
def variance_ratio(r, q):
    r = r[np.isfinite(r)]
    n = len(r)
    if n < q + 2:
        return float("nan"), float("nan")
    mu = r.mean()
    var1 = ((r - mu) ** 2).sum() / (n - 1)
    rq = np.convolve(r, np.ones(q), mode="valid")          # overlapping, n-q+1 block
    m = q * (n - q + 1) * (1 - q / n)
    varq = ((rq - q * mu) ** 2).sum() / m
    vr = varq / var1
    # Lo-MacKinlay (1988) heteroskedasticity-robust SE
    delta = 0.0
    for j in range(1, q):
        a = ((r[j:] - mu) ** 2 * (r[:-j] - mu) ** 2).sum()
        b = (((r - mu) ** 2).sum()) ** 2 / n
        delta += (2 * (q - j) / q) ** 2 * (a / b)
    se = np.sqrt(delta) if delta > 0 else float("nan")
    return float(vr), float(se)
```

**B. Pool toàn kỳ** — tính VR trên toàn bộ 50 giờ cho mỗi symbol (không theo giờ), báo 1 số
+ CI. Đơn giản hơn và trả lời đúng câu hỏi "thị trường có random walk không".

**C. Giữ theo giờ** nhưng báo median + IQR qua 50 giờ, kèm ghi chú q=10 quá ít mẫu.

## Nghiệm thu

- [ ] VR dùng block chồng lấn (hoặc có giải thích rõ vì sao không).
- [ ] Có SE/CI hoặc phân phối qua các giờ.
- [ ] Báo cáo không kết luận mạnh hơn mức dữ liệu cho phép; bỏ VR(10) nếu vẫn < 10 block.
