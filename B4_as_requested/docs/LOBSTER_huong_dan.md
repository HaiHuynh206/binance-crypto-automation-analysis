# LOBSTER — sample đủ, không cần acc trả phí

Site mới yêu cầu Sign In cho data đầy đủ. **Sample 21.06.2012** vẫn dùng được cho B4 (không cần account trả phí).

**Đã có trong repo (thật):**  
`data/raw/lobster/AAPL_2012-06-21_*_message_1.csv` + `*_orderbook_1.csv`  
(nguồn: mirror public HuggingFace của sample LOBSTER chính thức; cùng ngày/mã với Data Samples).

Nếu muốn tự tải từ web: vào mục **Data Samples** (không cần gói Student/Institute) → thả zip/CSV vào `data/raw/lobster/`.

## Các bước cụ thể

1. Vào [lobsterdata.com](https://lobsterdata.com) → chọn portal → **Sign In** (tạo account nếu chưa có).
2. Trong Data Catalog / Samples, tải **ít nhất 1 mã** ngày **2012-06-21**, mức **1** hoặc **10** levels (đủ để minh họa spread/depth).
3. Giải nén (nếu là zip) — cần cặp file kiểu:
   - `*_message_*.csv`
   - `*_orderbook_*.csv`
4. Copy vào máy:

```
group_4/data/raw/lobster/
  AAPL_2012-06-21_..._message_1.csv
  AAPL_2012-06-21_..._orderbook_1.csv
```

(hoặc để nguyên file `.zip` trong `group_4/data/raw/lobster/`)

5. Nhắn lại chat: **đã thả LOBSTER** → agent chạy ingest + đoạn đối chiếu metric cần sổ lệnh.

## Không làm được thì sao?

- Không có account / sample bị khóa → **skip LOBSTER** (đã note trong README); báo cáo vẫn dựa Binance + checklist dữ liệu VN cần LOB.
- **Không** nhờ agent đăng nhập hộ (credential của bạn).
- **Không** bịa file sổ lệnh.

## Mục đích trong B4 (sau khi có file)

Chỉ để **đối chiếu**: thước đo nào cần BBO/LOB (spread niêm yết, depth) mà aggTrades Binance không có — không dùng để kết luận thanh khoản HOSE.
