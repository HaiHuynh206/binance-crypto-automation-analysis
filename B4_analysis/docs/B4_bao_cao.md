# B4 — Đo chất lượng thị trường từ dữ liệu HFT

**Nhóm:** group_4  
**Flow:** DAG `crypto_market_ingest` → R2 lake → metrics (`compute_from_r2`) → notebook.  
**Đối chiếu sổ lệnh:** LOBSTER sample AAPL 2012-06-21.

## 1. Câu hỏi nghiên cứu

Thanh khoản và tác động giá thay đổi thế nào theo giờ trong ngày và giữa các tài sản? Cần dữ liệu gì để đo trên cổ phiếu Việt Nam?

## 2. Dữ liệu (thật)

| Nguồn | Nội dung | Kỳ |
|-------|----------|-----|
| R2 lake (`crypto_market_ingest`) | BTCUSDT / ETHUSDT — `klines_1s` + `agg_trades` | 2026-10-19…21 (~50 giờ) |
| LOBSTER sample | AAPL message + orderbook L1 | 2012-06-21 |

Giấy phép Binance: nghiên cứu/giáo dục (CC BY-NC-SA 4.0 theo brief môn).

## 3. Thước đo “chất lượng thị trường”

| Nhóm | Chỉ tiêu | Dữ liệu cần |
|------|----------|-------------|
| Thanh khoản | Amihud, Roll | trade / kline |
| Dòng lệnh / tác động | OFI, price impact | aggTrades |
| Nhiễu / RW | Variance ratio VR(q) | chuỗi giá HF |
| Sổ lệnh | Quoted spread, depth | BBO / LOB |

Bias: Amihud nhạy volume thấp; Roll cần CovΔp âm; OFI phụ thuộc phân loại bên mua/bán. **Một số không đủ.**

## 4. Kết quả — R2 lake (50 giờ)

| Symbol | Amihud mean | OFI mean | VR(2/5/10) |
|--------|-------------|----------|------------|
| BTCUSDT | 2.45e-9 | +0.014 | 1.00 / 0.95 / 0.89 |
| ETHUSDT | 5.25e-8 | +0.021 | 0.98 / 0.98 / 0.88 |

- Amihud ETH/BTC ≈ **21×** → ETH kém thanh khoản hơn trên cùng khung.
- 50 giờ (không 72): lake bắt đầu giữa ngày 19/10 — không bịa giờ thiếu.
- Hình: `docs/figures/r2_*.png` · CSV: `docs/results/summary_r2_2026-10-19_2026-10-21.csv`
- Profile theo giờ UTC **không** map 1-1 sang phiên HOSE.

## 5. Phản biện

1. **Ngoại suy crypto/LOBSTER → VN:** không biên độ, không phiên định kỳ; HOSE khác hoàn toàn. Kết quả chỉ minh họa phương pháp.
2. **Rủi ro vận hành:** thiếu partition giờ; schema đổi; QA fail-hard, không bịa dữ liệu (liên hệ sự cố buổi 4).
3. **Chỉ tiêu nào:** mục 3 — bộ thước + bias, không một số duy nhất.

## 6. Checklist dữ liệu VN cần có

| Dữ liệu | Mục đích |
|---------|----------|
| Tick/trade + timestamp | Roll, Amihud, impact |
| BBO hoặc LOB | Quoted spread, depth |
| Lịch phiên / nghỉ | Profile theo giờ đúng VN |
| Corporate actions | Điều chỉnh giá |
| Phân loại thanh khoản cao/thấp | So hai tài sản |

## 7. LOBSTER (đối chiếu)

| | Lake trade/kline | LOBSTER AAPL L1 |
|--|------------------|-----------------|
| Quoted spread | Không | ~0.155 USD (~2.66 bps) |
| Depth L1 | Không | bid≈227, ask≈147 |

→ Đo spread/depth kiểu CK cần **BBO/LOB**, không đủ chỉ lệnh khớp.

## 8. Tài liệu khởi điểm

Peña (2022) ch.3; Ferrouhi & Bouabdallaoui (2022) ch.4; Hendershott et al. (2011); Brogaard et al. (2014); Budish et al. (2015); Amihud (2002).
