# Evidence — Roll spread NaN (issue 01)

## 1. Từ output notebook đã commit

Notebook `B4_phan_tich_chat_luong_tt.ipynb`, cell 2 (`profile.head()`):

```
    symbol  hour_of_day        amihud  roll_spread  dollar_volume
0  BTCUSDT            0  3.599697e-09          NaN   4.634702e+07
1  BTCUSDT            1  3.658531e-09          NaN   4.416160e+07
2  BTCUSDT            2  3.247651e-09          NaN   3.135222e+07
3  BTCUSDT            3  1.131545e-09          NaN   2.667083e+07
4  BTCUSDT            4  6.337323e-10          NaN   2.792713e+07
```

Cell 4 (`summary`):

```
          amihud_mean  roll_mean  dollar_vol_mean  n_hours  ofi_mean
BTCUSDT  2.445195e-09        NaN     5.169012e+07       50  0.014142
ETHUSDT  5.245660e-08        NaN     2.899754e+07       50  0.020887
```

`roll_mean` = NaN cho **cả hai** symbol ⇒ 100% giờ (100/100) trả NaN.

## 2. Hình: 2 panel Roll trống

Ảnh `notebook-cell3-roll-empty.png` là output `image/png` của cell 3, trích nguyên từ
notebook. Hai panel dưới có tiêu đề "Roll spread theo giờ UTC" nhưng **không có một điểm dữ
liệu nào**, trục y chỉ hiện khoảng mặc định `-0.05…0.05`.

## 3. Vì sao NaN: tái hiện bằng mô phỏng

`repro/roll_spread_nan.py` mô phỏng 200 giờ × 3600 giây, đo tỉ lệ NaN của
`roll_effective_spread`:

| Kịch bản | Tỉ lệ NaN |
|----------|-----------|
| 60% giây không có trade (Δp = 0), 40% bounce ±0.5 | ~45% |
| Bounce thuần ±0.5, không có giây đứng giá | ~47% |
| Bounce + increments tự tương quan dương φ = 0.1 (persistence) | **100%** |

Hai kịch bản đầu cho thấy ngay cả "điều kiện lý tưởng" thì ước lượng covariance trên 3600
quan sát vẫn nhiễu → ~50% giờ ra NaN.

Kịch bản 3 giải thích con số 100% của dữ liệu thật: chỉ cần `Δp` có tự tương quan dương **rất
nhỏ** (φ = 0.1) là `E[cov] > 0`, và khi đó hàm trả `NaN` **theo đúng định nghĩa**, không phải
do may rủi. Dữ liệu 1 giây thật thuộc nhóm này: phần lớn giây đứng giá (Δp = 0) + một move
thường lan sang vài giây kế tiếp → covariance dương.

Kết luận: không phải "hôm đó dữ liệu xấu", mà là **phương pháp không nhận diện được** trên
chuỗi 1 giây trả về theo timestamp đều.

## 4. Ghi chú kỹ thuật

- `cryptofeed`/Binance trả nến 1 giây ngay cả khi không có trade (OHLC = close trước) → hàng
  loạt `Δp = 0` làm covariance bị kéo về 0/positive.
- Roll (1984) giả định price changes do bid-ask bounce trên **transaction prices**, không
  phải trên bar có timestamp đều.
