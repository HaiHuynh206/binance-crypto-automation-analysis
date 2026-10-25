---
id: 03
title: Amihud là "per 1 giây" nhưng báo cáo trình bày như Amihud chuẩn
severity: P1
status: open
area: metrics / báo cáo
files:
  - B4_analysis/src/metrics_roll_amihud.py
  - B4_analysis/docs/B4_bao_cao.md
---

# 03 — Đơn vị Amihud không được nêu

## Hiện tượng

`metrics_roll_amihud.py:53` tính Amihud trên **từng nến 1 giây**:

```python
amihud = amihud_illiquidity(g["ret"], g["quote_volume"] if ... )
# = mean( |r_1s| / dollar_volume_1s )   -> ~1e-9 (BTC), ~5e-8 (ETH)
```

Báo cáo `B4_bao_cao.md:33-38` in `2.45e-9` / `5.25e-8` và kết luận "ETH kém thanh khoản
hơn 21×" mà **không nói đơn vị/khung thời gian**.

## Ảnh hưởng

- Amihud (2002) chuẩn là `|r_daily| / dollar_volume_daily` (~1e-10…1e-12). Con số 1e-9 với
  người đọc quen literature sẽ bị hiểu sai scale.
- 21× vẫn đúng **trong nội bộ mẫu này**, nhưng không so được với bất kỳ bảng nào khác.

## Nguyên nhân

Không phải lỗi tính toán — là **thiếu chú thích đơn vị** và thiếu quyết định có chủ ý về
khung thời gian.

## Cách sửa

1. Ghi rõ trong báo cáo + docstring: "Amihud tính trên bar 1 giây, đơn vị `|r|/$volume` mỗi
   giây; chỉ so sánh trong cùng khung."
2. Hoặc chuẩn hoá về giờ cho dễ đọc — Amihud không cộng tuyến tính theo thời gian, nên cách
   trung thực nhất là **tính lại trên bar 1 phút / 1 giờ** rồi báo một con số duy nhất:

```python
def hourly_amihud_from_1m(df):   # bar 1 phút
    ...
    return amihud_illiquidity(g["ret"], g["quote_volume"])
```

3. Trong bảng kết quả thêm cột `unit` hoặc chú thích dưới bảng.

## Nghiệm thu

- [ ] Báo cáo ghi rõ khung thời gian + đơn vị của Amihud.
- [ ] Docstring `amihud_illiquidity` / `hourly_roll_amihud_from_klines` khớp với cách gọi.
- [ ] Con số 21× vẫn tái lập được sau khi đổi khung (nếu đổi).
