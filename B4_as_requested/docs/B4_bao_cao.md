# B4 — Đo chất lượng thị trường từ dữ liệu HFT

**Nhóm:** group_4  
**Nguồn chính:** lake R2 từ DAG sẵn `crypto_market_ingest` (không cần deploy DAG riêng).  
**Phụ:** Vision zip 2024-01 + LOBSTER sample (đối chiếu).

## 1. Câu hỏi nghiên cứu

Thanh khoản và tác động giá thay đổi thế nào theo giờ trong ngày và giữa các tài sản? Cần dữ liệu gì để đo trên thị trường cổ phiếu Việt Nam?

## 2. Dữ liệu đã dùng (thật)

| Nguồn | Nội dung | Kỳ |
|-------|----------|-----|
| **R2 lake** (DAG `crypto_market_ingest`) | `BTCUSDT` / `ETHUSDT` `klines_1s` + `agg_trades` | **2026-10-19 … 2026-10-21** (~50 giờ có partition) |
| data.binance.vision (phụ) | BTC/ADA klines 1s + aggTrades | 2024-01 (3 ngày trades) |
| LOBSTER sample | AAPL L1 | 2012-06-21 |

Lake R2: endpoint Cloudflare, bucket `crypto-lake`, đọc bằng DuckDB (`src/r2_lake.py`, `src/compute_from_r2.py`).  
Giấy phép Binance: nghiên cứu/giáo dục (CC BY-NC-SA 4.0 theo brief môn).

## 3. Thước đo và chỉ tiêu “chất lượng thị trường”

| Nhóm | Chỉ tiêu | Ý nghĩa | Dữ liệu cần |
|------|----------|---------|-------------|
| Thanh khoản | Amihud, Roll | Chi phí giao dịch / độ mỏng thanh khoản | Trade hoặc kline |
| Dòng lệnh / tác động | OFI, price impact | Áp lực mua/bán và dịch giá | aggTrades (aggressor) |
| Nhiễu / RW | Variance ratio VR(q) | Microstructure vs random walk | Chuỗi giá tần suất cao |
| Sổ lệnh | Quoted spread, depth | Spread niêm yết, độ dày top-of-book | BBO / LOB |

**Một số không đủ:** cần bộ chỉ tiêu + nêu bias (Amihud nhạy volume thấp; Roll cần CovΔp âm; OFI phụ thuộc phân loại bên).

## 4. Kết quả chính — từ Airflow R2 lake (50 giờ, 2026-10-19…21)

### 4.1 So hai tài sản (BTC vs ETH)

| Symbol | Amihud mean | OFI mean | VR(2) / VR(5) / VR(10) |
|--------|-------------|----------|-------------------------|
| BTCUSDT | 2.45e-9 | +0.014 | 1.00 / 0.95 / 0.89 |
| ETHUSDT | 5.25e-8 | +0.021 | 0.98 / 0.98 / 0.88 |

- Amihud ETH / BTC ≈ **21×** → ETH kém thanh khoản hơn BTC trên cùng khung đo (lake lab).
- Số giờ = **50** (không đủ 72): lake bắt đầu giữa ngày 19/10 — **không bịa giờ thiếu**.
- Artifact (trong repo): `docs/results/summary_r2_2026-10-19_2026-10-21.csv`.

### 4.2 Theo giờ trong ngày (UTC)

Hình R2: `docs/figures/r2_amihud_by_hour.png`, `r2_amihud_btc_vs_eth.png`, `r2_ofi_by_hour.png`.  
Notebook đã trỏ sang metrics `source=r2`. Profile **không** map 1-1 sang phiên HOSE.

### 4.3 Đối chiếu phụ (Vision 2024-01 BTC vs ADA)

Amihud ADA/BTC ≈ 852× trên mẫu 3 ngày Vision — cùng hướng “tài sản mỏng hơn → Amihud cao hơn”, kỳ khác nên không gộp số với R2.

## 5. Phản biện (bắt buộc)

1. **Ngoại suy crypto / LOBSTER Mỹ → VN:** crypto không biên độ, không phiên định kỳ; HOSE có phiên, lot, tick, circuit breaker. Kết quả BTC/ADA/AAPL là **minh họa phương pháp**, không phải ước lượng thanh khoản cổ phiếu VN.
2. **Rủi ro vận hành hệ thống tự động:** thiếu file ngày; schema aggTrades đổi 7→8 cột; URL LOBSTER official trả HTML SPA; Basic Auth Airflow 401 trong khi session OK. Cần QA fail-hard, không bịa dữ liệu. (Liên hệ sự cố buổi 4 trong môn.)
3. **Chất lượng thị trường đo bằng chỉ tiêu nào:** xem mục 3 — chọn bộ thước + bias, không dùng một số duy nhất.

## 6. Checklist dữ liệu trong nước cần có (sản phẩm áp dụng VN)

| Dữ liệu | Mục đích | Ghi chú |
|---------|----------|---------|
| Tick/trade + timestamp | Roll, Amihud, impact | Thường CTCK/vendor |
| BBO hoặc LOB | Quoted spread, depth | Chưa public rộng |
| Lịch phiên / nghỉ | Profile theo giờ đúng VN | HOSE/HNX công bố |
| Corporate actions | Điều chỉnh giá | Vendor / công bố DN |
| Phân loại thanh khoản cao/thấp | So hai tài sản | Turnover, free float… |

## 7. Đối chiếu LOBSTER

| | Binance trade/kline | LOBSTER AAPL 2012-06-21 L1 |
|--|---------------------|----------------------------|
| Quoted spread | Không | mean ≈ 0.155 USD (~2.66 bps) |
| Depth L1 | Không | bid size≈227, ask≈147 |
| Amihud / OFI trade | Có | Bổ sung |

→ Muốn đo spread/depth kiểu thị trường chứng khoán cần **BBO/LOB**, không đủ chỉ lệnh khớp.

## 8. Pipeline & Airflow

- **Chính:** DAG lab `crypto_market_ingest` → R2 → `python -m src.compute_from_r2` → notebook.
- Phụ Vision: `ingest_binance` → `compute_all_metrics`.
- DAG riêng `b4_market_quality` **không bắt buộc** (đã dùng flow sẵn + R2).
- Client: `python -m src.airflow_client` / `python -m src.r2_lake`.

## 9. Tài liệu khởi điểm

Peña (2022) ch.3; Ferrouhi & Bouabdallaoui (2022) ch.4; Hendershott et al. (2011) JF; Brogaard et al. (2014) RFS; Budish et al. (2015) QJE; Amihud (2002) JFM.

## 10. Việc còn lại (optional)

- [ ] Admin deploy `dags/b4_market_quality.py`
- [ ] Thêm sample LOBSTER mã thứ hai (MSFT/INTC) nếu muốn so 2 cổ phiếu Mỹ
