---
id: 01
title: Roll spread = NaN 100% — biểu đồ Roll trống
severity: P1
status: open
area: metrics
files:
  - B4_as_requested/src/metrics_roll_amihud.py
  - B4_as_requested/notebooks/B4_phan_tich_chat_luong_tt.ipynb
  - B4_as_requested/docs/B4_bao_cao.md
evidence:
  - issue/evidence/roll-spread-nan.md
  - issue/evidence/notebook-cell3-roll-empty.png
repro: issue/repro/roll_spread_nan.py
---

# 01 — Roll spread luôn NaN, 2 panel biểu đồ trống

## Hiện tượng

Trong notebook, cột `roll_spread` của cả BTCUSDT và ETHUSDT đều là `NaN` ở **mọi giờ**.
`summary` in ra `roll_mean = NaN` cho cả hai symbol. Trong cell vẽ hình, 2 panel
dưới ("Roll spread theo giờ UTC") trắng trơn, chỉ có trục — xem
[ảnh bằng chứng](evidence/notebook-cell3-roll-empty.png).

## Ảnh hưởng

- Báo cáo mục 3 nêu "Thanh khoản: Amihud, Roll" nhưng bảng kết quả mục 4 **không có Roll**.
  Người chấm thấy một metric được khai báo rồi biến mất, và một hình bị hỏng.
- Không có cảnh báo nào khi Roll thất bại: code im lặng trả `NaN`.

## Nguyên nhân

`metrics_roll_amihud.py:54` gọi `roll_effective_spread(g["close"])` trên close của nến
**1 giây** trong phạm vi 1 giờ (~3600 điểm), và `:20` trả `NaN` ngay khi

```python
cov = np.cov(dp[1:], dp[:-1])[0, 1]
if cov >= 0:
    return float("nan")
```

Với dữ liệu 1 giây: rất nhiều giây không có khớp lệnh → `Δp = 0`; phần còn lại là
nhiễu bid-ask trên nền random walk. Cov mẫu trên 3600 quan sát gần như không bao giờ âm
→ hàm trả `NaN` gần 100%.

| Kịch bản | Tỉ lệ NaN |
|----------|-----------|
| 60% giây không có trade (Δp = 0), 40% bounce ±0.5 | ~45% |
| Bounce thuần ±0.5, không có giây đứng giá | ~47% |
| `Δp` tự tương quan dương φ = 0.1 (persistence) | **100%** |

Kịch bản 3 là mô hình đúng cho dữ liệu 1 giây thật: nhiều giây đứng giá + move lan sang
các giây kế tiếp → covariance dương → `roll_effective_spread` trả `NaN` theo định nghĩa.
Đó là lý do output notebook có `roll_mean = NaN` cho cả hai symbol, không phải do xui.

Chi tiết + số đo: [evidence](evidence/roll-spread-nan.md).

## Cách sửa (chọn 1)

**A. Tính Roll trên giá khớp lệnh thật (khuyến nghị)** — dùng `agg_trades`, bỏ các giây
không có trade, lấy chuỗi `price` theo `trade_time` trong từng giờ:

```python
# metrics_roll_amihud.py
def hourly_roll_from_trades(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["transact_time"] = pd.to_datetime(d["transact_time"], utc=True)
    d = d.sort_values("transact_time")
    d["hour"] = d["transact_time"].dt.floor("h")
    rows = []
    for (symbol, hour), g in d.groupby(["symbol", "hour"], sort=True):
        rows.append({
            "symbol": symbol,
            "hour": hour,
            "n_trades": len(g),
            "roll_spread": roll_effective_spread(g["price"]),   # chỉ trade price
        })
    return pd.DataFrame(rows)
```
Trade price trên 1 giờ vẫn nhiều quan sát, nhưng bỏ được các "giây chết" — đây chính là
giả định gốc của Roll (price changes do bounce, không do zero-return).

**B. Hạ tần suất** — tính Roll trên nến 1 phút (60 điểm/giờ). Rẻ, nhưng 60 quan sát là quá
ít để ước lượng covariance ổn định → chỉ nên dùng nếu chấp nhận nhiều NaN có ghi chú.

**C. Pool nhiều ngày** — tính Roll một lần cho mỗi symbol trên toàn bộ chuỗi trade
(50 giờ), báo 1 số + ghi rõ là ước lượng toàn kỳ thay vì theo giờ.

Dù chọn cách nào, **không được để NaN im lặng**: nếu không nhận diện được thì ghi
"Roll không xác định (Cov ≥ 0)" trong báo cáo và bỏ panel đó khỏi hình.

## Nghiệm thu

- [ ] `roll_spread` có ít nhất ~70% giờ khác `NaN` (hoặc có giải thích rõ nếu thấp hơn).
- [ ] `summary.roll_mean` không còn `NaN`.
- [ ] 2 panel Roll trong notebook có dữ liệu, hoặc đã bị bỏ khỏi hình.
- [ ] Báo cáo mục 4 có Roll khớp với notebook.
- [ ] Thêm cảnh báo/log khi tỉ lệ NaN > 50%.
