# BÁO CÁO QUẢN TRỊ
## Hiệu quả kinh doanh của Phòng Dữ liệu

| | |
|---|---|
| **Kính gửi** | Tổng Giám đốc (CEO); Giám đốc Tài chính (CFO) |
| **Người lập** | Phòng Dữ liệu |
| **Ngày** | 24/10/2026 |
| **Loại báo cáo** | Báo cáo quản trị + đề xuất ngân sách 12 tháng |
| **Phạm vi** | Hiện tại 2 symbol → kế hoạch 20 symbol → đánh giá tiền đề cho mô hình phân tích/dự báo |
| **Đơn vị tiền** | USD; quy đổi tham chiếu 1 USD = 26.000 VND |

---

## 1. Tóm tắt điều hành

### 1.1. Kết luận trong một câu

**Phòng Dữ liệu đang vận hành với tổng chi phí hạ tầng $8/tháng. Mở rộng lên 20 symbol chỉ tăng lên $20/tháng và làm chi phí đơn vị giảm 4,3 lần. Nút thắt duy nhất không phải tiền mà là 7 ngày công kỹ thuật cho gói việc ưu tiên.**

### 1.2. Bảng điều khiển

| Chỉ số | Hiện tại (2 symbol) | Kế hoạch (20 symbol) | Ghi chú |
|---|---:|---:|---|
| Tổng chi phí hạ tầng/tháng (máy ảo + dữ liệu + phụ trợ) | **$8** (208k VND) | **$20** (520k VND) | Đã gồm mọi khoản |
| Ngân sách đề xuất P90/tháng | **$10** (260k VND) | **$28** (728k VND) | Đã gồm dự phòng theo độ biến động từng pha |
| TCO 3 năm (tiền mặt) | **$366** (9,5 triệu VND) | **$860** (22,4 triệu VND) | Toàn bộ hoá đơn phải trả |
| Chi phí/symbol/năm | **$61** | **$14** | Giảm 4,3 lần |
| Thời gian kỹ thuật/tháng | 6 giờ | 12 giờ | Nguồn lực, không phải tiền |
| Gói việc ưu tiên (P0) | — | **7 ngày công** | Không phát sinh tiền mặt |
| ML (dự báo) | Chưa làm | **Chưa duyệt** | Chỉ mở khi vượt cổng giá trị |

### 1.3. Năm điểm cần ghi nhớ

1. **Hạ tầng không phải vấn đề.** Tổng chi phí tất cả (máy ảo + lưu trữ dữ liệu + sao lưu + giám sát + domain) ở quy mô 20 symbol là **$20/tháng (520k VND)**; ngân sách có dự phòng là **$28/tháng**. Trong đó máy ảo **$11,50** và lưu trữ dữ liệu chỉ **$0,45**. Tối ưu dung lượng tiết kiệm tối đa ~$5,6/tháng.
2. **Nút thắt là thời gian, không phải tiền.** Toàn bộ chi phí tiền mặt ở 20 symbol là $20/tháng; thứ khan hiếm là **12 giờ kỹ thuật/tháng**. Mọi sáng kiến nên được đo bằng số giờ công giải phóng được, không bằng đô-la cloud.
3. **Mở rộng có lợi theo quy mô.** Từ 2 lên 20 symbol, chi phí/symbol/năm giảm từ **$61 xuống $14**; mỗi symbol tăng thêm chỉ tốn **~$0,67/tháng** hạ tầng.
4. **Có 3 rủi ro lớn hơn cả ngân sách năm**, xử lý được trong dưới 2 tuần công: giới hạn gọi API, quá nhiều file nhỏ, và mất dữ liệu `open_interest` (không thể tải lại).
5. **Chưa nên làm ML.** Chi phí tiền mặt biên chỉ ~$385/năm, nhưng cần ~10 giờ/tháng R&D; chỉ đáng đầu tư khi mô hình thắng baseline trên dữ liệu ngoài mẫu và tạo giá trị **>$10.000/năm**.

### 1.4. Đề nghị

| # | Đề nghị | Chi phí / nguồn lực | Thời hạn |
|---|---|---:|---|
| QĐ1 | Duyệt ngân sách 12 tháng, giải ngân theo pha | **$336/năm** (8,7 triệu VND), tối đa $28/tháng | 12 tháng |
| QĐ2 | Duyệt gói kỹ thuật ưu tiên P0 | **7 ngày công nội bộ**, không phát sinh tiền mặt | 45 ngày |
| QĐ3 | Duyệt mở rộng lên 20 symbol **khi đạt cổng** | Trong ngân sách QĐ1 | Sau 4–6 tháng |
| QĐ4 | Chưa duyệt ML; giữ quỹ dự phòng | $0 giải ngân; quỹ dự phòng $65/tháng từ tháng 7 (~$390 cho 6 tháng) | Xem xét lại tháng 7 |

> **Trần tiền mặt năm 1:** QĐ1 $336 (vận hành) + quỹ dự phòng ML $390 (QĐ4, chỉ giải ngân sau cổng) = **$726 (~18,9 triệu VND)**. Nếu cổng ML không đạt, giải ngân thực tế chỉ là **$336**. Gói P0 (QĐ2) không dùng tiền mặt.

---

## 2. Bối cảnh và phạm vi

Phòng Dữ liệu vận hành một nền tảng dữ liệu thị trường crypto nội bộ: tự động thu thập dữ liệu từ Binance **12 lần/ngày**, lưu theo ba lớp **Bronze → Silver → Gold**, có **cổng kiểm tra chất lượng (DQ gate)** chặn báo cáo chạy trên dữ liệu rác, và xuất bản lên kho đối tượng (Cloudflare R2) để phân tích.

Câu hỏi quản trị đặt ra:

1. Phòng Dữ liệu đang tốn bao nhiêu, cấu trúc chi phí ra sao?
2. Hiệu quả kinh doanh được đo bằng gì khi phòng ban không tạo doanh thu trực tiếp?
3. Có nên mở rộng từ 2 lên 20 symbol, và nên làm mô hình dự báo hay không?
4. Cần phê duyệt ngân sách và cơ chế kiểm soát nào?

Báo cáo này trình bày **kết luận trước, chi tiết sau**, theo hướng phục vụ quyết định của CEO/CFO. Các giả định kỹ thuật và phương pháp tính nằm ở Phụ lục A.

---

## 3. Hiệu quả kinh doanh: Phòng Dữ liệu tạo giá trị gì?

### 3.1. Giá trị không nằm ở doanh thu trực tiếp

Phòng Dữ liệu là **hạ tầng năng lực**, không phải trung tâm lợi nhuận. Giá trị được đo qua bốn kênh:

| Kênh giá trị | Cách đo | Hiện tại | Sau 12 tháng |
|---|---|---|---|
| **Tốc độ ra quyết định** | Độ trễ dữ liệu → báo cáo | ≤ 2 giờ | ≤ 2 giờ |
| **Độ tin cậy số liệu** | Tỷ lệ giờ dữ liệu được kiểm tra | Chưa có cảnh báo | **≥99,5%** giờ được kiểm tra; phát hiện trong **≤1 giờ** |
| **Năng suất phân tích** | Thời gian chờ truy vấn toàn lake | <10 phút | **1–3 phút** |
| **Chi phí đơn vị** | $/symbol/năm (tiền mặt) | $61 | **$14** |

> **Lưu ý minh bạch:** chưa có số liệu đo “thời gian tiết kiệm được” vì hiện chưa có đủ analyst sử dụng thường xuyên. Đây là chỉ số sẽ được đo trong 90 ngày tới, không phải con số hứa hẹn.
>
> Con số **5–13 giờ** ở mục 7.2 là **rủi ro của 20 symbol nếu không gộp file**, không phải hiện trạng hôm nay (2 symbol có ~93.000 file/năm, truy vấn dưới 10 phút).

### 3.2. Năng lực đã có và năng lực kỳ vọng

| Năng lực | Trạng thái | Ghi chú |
|---|---|---|
| Thu thập tự động 5 loại dữ liệu, 2 symbol | **Đã có** | 12 lần/ngày, có chống trùng và chạy bù |
| Ba lớp Bronze/Silver/Gold + cổng DQ | **Đã có** | Gold bị chặn khi dữ liệu lỗi |
| Backfill 90 ngày | **Đã có năng lực**, đang hoàn tất | Chạy lại không sinh dữ liệu trùng |
| Catalog tự phục vụ (DuckDB) | **Đã có công cụ** | Chưa có analyst dùng thường xuyên; sẽ xác nhận trong 90 ngày |
| Xuất bản lên R2 | **Đã có** | Phí truyền dữ liệu ra bằng $0 |
| 20 symbol | **Kỳ vọng** | Cần 4 tuần ổn định + 3 việc P0 |
| Gộp file + lưu trữ theo tầng | **Kỳ vọng** | Xem mục 8 |
| Mô hình dự báo | **Kỳ vọng** | Có cổng chất lượng, xem mục 10 |

### 3.3. Điều quan trọng nhất với CFO

Ở quy mô hiện tại, **chi phí tiền mặt chỉ $8/tháng** — tương đương một suất ăn công ty. Giá trị thật nằm ở việc **chặn các quyết định dựa trên dữ liệu sai hoặc cũ**, và ở khả năng mở rộng mà không tăng chi phí tương ứng. Vì vậy, câu hỏi quản trị không phải “có nên chi $8 hay $28/tháng”, mà là **“có nên phân bổ 6–12 giờ công/tháng cho vận hành, hay dùng số giờ đó để tạo giá trị phân tích”**.

---

## 4. Cơ cấu chi phí và hiệu quả tài chính

### 4.1. Cơ cấu chi phí tháng ở phương án 20 symbol

| Thành phần | $/tháng | % | Loại |
|---|---:|---:|---|
| Máy chủ CX32 + volume 80 GB | 11,50 | 57,5% | Cố định |
| Lưu trữ R2 (tiered ~40 GB) | 0,45 | 2,3% | Biến đổi, tăng rất chậm |
| Sao lưu (B2, khác vùng) | 1,50 | 7,5% | Biến đổi |
| Giám sát & log | 3,00 | 15,0% | Cố định |
| Domain/TLS | 1,00 | 5,0% | Cố định |
| Request API | 0,50 | 2,5% | Biến đổi |
| Dự phòng vận hành | 2,05 | 10,2% | Cố định |
| **Tổng tiền mặt** | **20,00** | **100%** | |

### 4.2. Tổng chi phí tất cả — máy ảo + dữ liệu + phụ trợ

**So what: tổng chi phí tiền mặt ở 20 symbol là $20/tháng (520k VND), đã gồm máy ảo, lưu trữ dữ liệu, sao lưu và phụ trợ. Không có khoản nào bị bỏ ngoài bảng này.**

Chi tiết phương án B (20 symbol):

| Khoản | $/tháng | VND/tháng | Ghi chú |
|---|---:|---:|---|
| Máy ảo (VPS CX32 + ổ đĩa 80 GB) | 11,50 | 299.000 | Chạy ingest, DuckDB, Airflow |
| Lưu trữ dữ liệu (R2, tiered ~40 GB) | 0,45 | 11.700 | Dữ liệu gốc + archive |
| Sao lưu dữ liệu (B2, khác vùng) | 1,50 | 39.000 | Bản sao độc lập |
| Giám sát, domain, request API | 4,50 | 117.000 | |
| Dự phòng vận hành | 2,05 | 53.300 | ~10% |
| **TỔNG CHI PHÍ** | **20,00** | **520.000** | **Toàn bộ chi phí phải trả** |

Tổng theo kỳ và theo kịch bản:

| Kịch bản | Chi phí/tháng | Chi phí/năm | TCO 3 năm |
|---|---:|---:|---:|
| **A** — 2 symbol | **$8** (208k) | **$96** (2,5 tr) | **$366** (9,5 tr) |
| **B** — 20 symbol | **$20** (520k) | **$240** (6,24 tr) | **$860** (22,4 tr) |
| **C** — 20 symbol + ML | **$40** (1,04 tr) | **$480** (12,5 tr) | **$2.016** (52,4 tr) |

> Toàn bộ số liệu trong báo cáo là **tiền mặt phải trả**, không quy đổi giờ công nội bộ thành tiền. Giờ công được báo cáo riêng như một chỉ số nguồn lực ở mục 10. Quy đổi 1 USD = 26.000 VND.

### 4.3. Vì sao chi phí gần như cố định

- Dung lượng tăng 138 GB/năm nhưng giá lưu trữ chỉ **$0,015/GB-tháng**, nên mỗi năm tăng thêm chưa tới **$2/năm**.
- Máy chủ nhảy theo **bậc** khi nâng cấu hình, không tuyến tính theo số symbol.

**Hệ quả quản trị:** dự báo ngân sách rất dễ, sai số ±15% sau khi có 6 tháng dữ liệu thực; rủi ro vượt ngân sách đến từ **sự cố và việc nâng cấp máy chủ**, không từ khối lượng dữ liệu.

### 4.4. Chi phí biên mỗi symbol

| Khoản | $/symbol/tháng |
|---|---:|
| Hạ tầng (tiền mặt, chi phí biên) | **0,67** |
| **Tổng** | **~0,67** (~$8/năm) |

Đây là con số quan trọng nhất để quyết định tốc độ mở rộng: **mỗi symbol tăng thêm gần như miễn phí về tiền mặt**; chi phí thật là thời gian cấu hình và giám sát thêm, trong khi giá trị phân tích tăng thêm một thị trường.

---

## 5. Ba kịch bản và ngân sách đề xuất

### 5.1. So sánh kịch bản

| | **A** (2 symbol) | **B** (20 symbol) | **C** (20 symbol + ML) |
|---|---:|---:|---:|
| Dung lượng/năm | 19,7 GB | 138 GB | 138 GB + feature/model |
| Tổng chi phí/tháng (P50) | **$8** | **$20** | **$40** |
| Ngân sách P90/tháng | **$10** | **$28** | **$65** |
| Ngân sách P90/năm | **$120** (3,1 tr) | **$336** (8,7 tr) | **$780** (20,3 tr) |
| TCO 3 năm (tiền mặt) | $366 | $860 | $2.016 |
| TCO 3 năm (VND) | 9,5 tr | 22,4 tr | 52,4 tr |
| Một lần (tiền mặt) | $78 | **$140** | $576 |
| Thời gian kỹ thuật/tháng | 6h | 12h (10–14) | 22h (20–26) |
| Chi phí/symbol/năm | $61 | **$14** | $34 |

> Ngân sách P90 = hạ tầng P50 + dự phòng theo độ biến động từng pha (A +25%, B +40%, C +63%; C cao vì chi phí GPU/feature dao động mạnh) và là con số nên đưa vào kế hoạch năm. Gói P0 của B (**7 ngày công**) không phát sinh tiền mặt; khoản một lần bằng tiền của B ($140) là nâng cấp hạ tầng và sao lưu.

### 5.2. Vì sao chọn B làm phương án mở rộng

- TCO tiền mặt 3 năm chỉ **2,35 lần** A nhưng phục vụ **10 lần** số symbol.
- Chi phí/symbol/năm thấp nhất trong ba kịch bản.
- Dòng tiền mặt **$20/tháng** — không tạo áp lực ngân sách.
- Biên an toàn cao: dung lượng tăng 50% chỉ làm TCO tăng ~5%.

### 5.3. Vì sao chưa chọn C

Chi phí tiền mặt biên của ML so với B là **$1.156/3 năm (~$385/năm)**. Về tiền, ML rất rẻ; cái đắt là **~10 giờ/tháng R&D**. Vì vậy khuyến nghị chỉ đầu tư khi mô hình thắng baseline trên dữ liệu ngoài mẫu và tạo giá trị **>$10.000/năm** — ngưỡng này trả cho rủi ro nghiên cứu, không phải cho hạ tầng. Khi chưa có bằng chứng ngoài mẫu, ML là **chi phí cơ hội**, không phải đầu tư.

---

## 6. Unit economics và ROI

### 6.1. Chi phí đơn vị

| Chỉ số | Giá trị | Diễn giải |
|---|---:|---|
| Chi phí/symbol/năm (B, tiền mặt) | **$14** (~364k VND) | Giảm 4,3 lần so với A |
| Giá lưu trữ R2 | $0,015/GB-tháng | Giá nhà cung cấp |
| Chi phí hạ tầng/GB dữ liệu nạp vào (3 năm) | ~$2/GB | Toàn bộ hoá đơn chia cho 414 GB |

> Lưu trữ chỉ chiếm **2,3%** chi phí hạ tầng; phần lớn là máy ảo, sao lưu và giám sát. Tối ưu dung lượng tiết kiệm tối đa ~$5,6/tháng — không phải đòn bẩy tài chính.

### 6.2. So sánh phương án lưu trữ (3 năm, tiền mặt)

| | B | C |
|---|---:|---:|
| **Cloudflare R2 (chọn)** | **$860** | **$2.016** |
| AWS S3 | $1.617 (**+88%**) | $6.143 (**+205%**) |
| Tự vận hành | Hòa vốn ở ~1,8 TB (~năm thứ 13 với 138 GB/năm); phát sinh thêm công vận hành | Không khả thi |

**Kết luận:** R2 là lựa chọn đúng và không cần xem xét lại trong 3 năm. Chuyển sang S3 làm tăng 88% chi phí tiền mặt ở B và hơn gấp đôi ở C, hoàn toàn do **phí truyền dữ liệu ra**.

### 6.3. Gói kỹ thuật ưu tiên (P0) — đầu tư bằng thời gian

**So what: 7 ngày công, không phát sinh tiền mặt, loại bỏ 3 rủi ro vận hành lớn và tiết kiệm ~$108/năm hạ tầng.**

| Việc | Thời gian | Lợi ích |
|---|---:|---|
| Sửa giới hạn gọi API dùng chung | 4 giờ | Tránh bị chặn IP, dừng toàn bộ ingest 1–3 ngày |
| Gộp file nhỏ (compaction) | 3 ngày | 930k → ~8k file/năm; truy vấn 5–13 giờ → 1–3 phút ở 20 symbol |
| Cảnh báo đĩa/DQ/open interest/chi phí | 1 ngày | Chặn mất dữ liệu OI vĩnh viễn; kiểm soát ngân sách |
| Backfill bằng file tĩnh Binance | 4 giờ | 24 giờ → 4 giờ khi backfill |
| Chia lô 4 nhóm symbol | 2 ngày | Tránh nâng cấp máy chủ ~$108/năm |
| **Tổng** | **~7 ngày công** | **~$108/năm tiền mặt + 3 rủi ro lớn được loại** |

> Không quy đổi ngày công thành tiền. Giá trị của P0 nằm ở việc giảm rủi ro gián đoạn và giải phóng thời gian phân tích — hai thứ này theo dõi qua KPI ở mục 10, không quy ra đô-la. Kiểm tra backup 16 giờ/năm nằm trong 12 giờ vận hành/tháng.

### 6.4. Cách đo hiệu quả không cần doanh thu

Bốn chỉ số vận hành, báo cáo hằng tháng:

1. Giờ công/tháng.
2. Chi phí/symbol/tháng.
3. Tỷ lệ lần chạy lỗi.
4. Số symbol phục vụ trên mỗi giờ công.

Mục tiêu năm 1 của B: **giờ công ≤12h/tháng, chi phí hạ tầng/symbol ≤$1,2/tháng**.

---

## 7. Kế hoạch năng lực và mở rộng lên 20 symbol

### 7.1. Vì sao dung lượng không tăng 10 lần

- **22%** dữ liệu (nến 1 giây/1 phút) tăng đúng **10 lần** theo số symbol.
- **76%** là dữ liệu giao dịch, phụ thuộc **thanh khoản từng mã**; 18 altcoin còn lại trung bình chỉ bằng 0,4–0,6 lần BTC/ETH.
- Hệ số tổng hợp: **~7 lần** (khoảng 6,2–7,5 lần; mức 10 lần là kịch bản tuyến tính đã bị bác bỏ vì bỏ qua thanh khoản từng mã).

| Hạng mục | A: 2 symbol | B: 20 symbol | C: + ML |
|---|---:|---:|---:|
| Dung lượng/năm | 19,7 GB | **138 GB** (105–150) | 138 GB + feature |
| Dung lượng sau 3 năm (tiered) | ~5 GB | **~40 GB** | ~55 GB |
| Hạ tầng/tháng | $8 | **$20** | **$40** |
| Thời gian kỹ thuật/tháng | 6h | **12h** | **22h** |
| Thời gian chạy/lần | 3–6 phút | 15–35 phút | 15–35 phút |

### 7.2. Nút thắt và cách xử lý

| Nút thắt | Biểu hiện | Xử lý | Công |
|---|---|---|---|
| **Giới hạn gọi API** | 5 task song song = **12.000 weight/phút**, vượt trần Binance **6.000** → nguy cơ chặn IP | Bộ giới hạn dùng chung | **4 giờ — P0** |
| **Quá nhiều file nhỏ** | 930.000 file/năm; truy vấn toàn lake 5–13 giờ | Gộp file (compaction) | 3 ngày — P0 |
| **Backfill chậm** | 20 symbol × 90 ngày ≈ 24 giờ qua API | Dùng kho file tĩnh của Binance | 4 giờ |
| **Nghẽn scheduler** | 1 task/dataset, không song song theo symbol | Chia 4 nhóm symbol | 2 ngày |

### 7.3. Điều kiện chuyển pha B (phải đạt đủ)

1. **14 ngày liên tiếp** không thiếu giờ dữ liệu.
2. Thời gian chạy **<10 phút/lần ở 2 symbol** (hiện 3–6 phút).
3. Đĩa sử dụng **<65%**.
4. Có **ít nhất một người dùng thật** đang dùng lake hằng tuần.
5. Chi phí biên **hạ tầng** <$1,5/symbol/tháng (dự kiến $0,67).

---

## 8. Chính sách lưu trữ: rolling 3 tháng hay theo tầng?

### 8.1. So sánh ba phương án

| Phương án | Dung lượng sau 3 năm | R2 $/tháng (cuối kỳ) | Truy vấn lịch sử | ML khả thi | Rủi ro | Kết luận |
|---|---:|---:|---|---|---|---|
| Rolling 3 tháng | 35 GB | $0,38 | Chỉ 3 tháng | ❌ Không | Mất lịch sử vĩnh viễn | Loại |
| **Tiered (khuyến nghị)** | **40 GB** | **$0,45** | Đầy đủ, 1–3 phút | ✅ Có | Thấp | **Chọn** |
| Giữ vĩnh viễn | 414 GB | $6,06 | Đầy đủ nhưng chậm dần | ✅ Có | Đĩa đầy, file phình | Không cần |

> Con số 40 GB là **tính đúng theo chính sách ở mục 8.2** (bỏ bản Silver trùng grain, nến 1 giây chỉ giữ 12 tháng). Nếu giữ nến 1 giây 3 năm thay vì 12 tháng thì thành ~70 GB (~$0,90/tháng).

### 8.2. Chính sách tiered đề xuất

| Dataset | Local | R2 | Sau đó | Lý do |
|---|---|---|---|---|
| Giao dịch thô (`agg_trades`) | 30 ngày | 90 ngày | Xoá (đã có nến 1 phút) | Chiếm ~40% dung lượng, tái tạo được |
| `trades` (Silver) | 7 ngày | Không giữ | Xoá | Cùng grain với Bronze, tái tạo được |
| Nến 1 giây | 30 ngày | 12 tháng | Xoá bản Silver | Nến 1 phút đủ cho 95% phân tích |
| **Nến 1 phút** | Tất cả | **Vĩnh viễn** | — | Chỉ ~3 GB/năm ở 20 symbol |
| **Funding** | Tất cả | **Vĩnh viễn** | — | Vài chục MB/năm |
| **Open interest** | Tất cả | **Vĩnh viễn** | — | **Không thể tải lại quá 30 ngày** |
| **Gold + `dq_hourly`** | Tất cả | **Vĩnh viễn** | — | ~0,2 GB/năm |

### 8.3. Vì sao không tối ưu storage vì tiền — nhưng vẫn phải làm

Chênh lệch giữa phương án tiết kiệm nhất và tốn nhất chỉ **~$5,6/tháng** (~$67/năm). Tuy nhiên:

- **Gộp file (compaction)** tốn 3 ngày công nhưng ở quy mô 20 symbol giảm thời gian chờ truy vấn từ 5–13 giờ xuống 1–3 phút và tránh phải nâng cấp máy chủ. Giá trị thật là **analyst không bỏ cuộc giữa lúc chờ số**.
- **Disk đầy** có thể làm mất vĩnh viễn dữ liệu `open_interest`. Một sự cố như vậy tốn hơn toàn bộ tiết kiệm lưu trữ của 3 năm.

**Kết luận:** làm retention và compaction vì **vận hành và khả năng phân tích**, không phải vì tiết kiệm vài đô-la.

---

## 9. Rủi ro và kiểm soát

### 9.1. Ba rủi ro lớn hơn ngân sách năm

| Rủi ro | Xác suất | Tác động | Giảm thiểu | Trạng thái |
|---|---|---|---|---|
| **Mất dữ liệu open interest** (chỉ tải được 30 ngày) | Thấp | **Cao — không thể tái tạo** | Cảnh báo riêng, kiểm tra hằng ngày | Cần làm (1 ngày) |
| **Đĩa đầy** | Trung bình | Cao — mất ingest, có thể mất OI | Cảnh báo 70%, retention tiered, đĩa 80 GB | Cần làm (1 ngày) |
| **Bị chặn IP do vượt hạn mức API** | Trung bình | Cao — dừng toàn bộ ingest | Bộ giới hạn dùng chung (P0) | **Chưa xử lý** |

### 9.2. Các rủi ro còn lại

| Rủi ro | Xác suất | Tác động | Giảm thiểu |
|---|---|---|---|
| Vendor lock-in (Binance, R2) | Thấp–TB | Trung bình | Parquet mở, layout chuẩn; đổi S3/B2 trong 1 ngày |
| Model overfit | **Cao** nếu không kiểm soát | Trung bình — quyết định sai | Bắt buộc thắng baseline ngoài mẫu; walk-forward; chưa vận hành trước 12 tháng dữ liệu |
| Phụ thuộc một người | Trung bình | Cao | Runbook, tài liệu hoá, backup cấu hình |
| **Compaction xoá nhầm dữ liệu gốc** | Thấp | Cao — mất dữ liệu không tái tạo | Chạy `--dry-run` + đối chiếu số dòng/checksum; giữ bản gốc 7 ngày trước khi xoá |
| **Chỉ có một máy chủ**, không bản sao | Trung bình | Trung bình–Cao — dừng ingest; quá 30 ngày mất OI | Snapshot VPS; chấp nhận RTO 2 ngày; ưu tiên khôi phục ingest trước |
| **Chưa diễn tập phục hồi** | Cao | Trung bình — backup không dùng được khi cần | Restore thử 6 tháng/lần, ghi kết quả vào KPI |

**Nguyên tắc:** rủi ro mất dữ liệu **không thể tái tạo** được ưu tiên cao hơn rủi ro mất dữ liệu tái tạo được.

---

## 10. Lộ trình 90 ngày – 12 tháng và KPI

### 10.1. Lộ trình

| Thời gian | Mốc bàn giao | Điều kiện hoàn thành |
|---|---|---|
| **0–30 ngày** | Sửa giới hạn API; cảnh báo đĩa/DQ/heartbeat; kiểm tra backup | Không còn nguy cơ 12.000 weight/phút |
| **31–60 ngày** | Compaction + retention tiered; backfill bằng file tĩnh | 930k → ~8k file/năm; truy vấn <3 phút |
| **61–90 ngày** | Chia 4 nhóm symbol; báo cáo phân tích đầu tiên | 14 ngày DQ sạch; chạy <10 phút (2 symbol) |
| **4–6 tháng** | Mở rộng 20 symbol; bàn giao catalog tự phục vụ | Đạt đủ 5 điều kiện chuyển pha |
| **7–12 tháng** | Baseline ML (naive/ARIMA), đánh giá cổng đầu tư | Quyết định có/không làm ML |

Tổng đầu tư một lần cho 90 ngày: **~56 giờ công (~7 ngày), không phát sinh tiền mặt**. Kiểm tra backup 16 giờ/năm nằm trong 12 giờ vận hành/tháng.

### 10.2. KPI báo cáo hằng tháng

| KPI | Hiện tại | Mục tiêu 90 ngày | Mục tiêu 12 tháng | Chủ sở hữu |
|---|---:|---:|---:|---|
| Chi phí/symbol/năm | $61 | $61 | **$14** | CFO |
| Chi phí thực tế vs ngân sách P90 | Chưa theo dõi | ±10% | ±10% | CFO |
| Số ngày DQ sạch liên tiếp | Chưa đo | **14** | **90** | Platform Owner |
| Độ sẵn sàng (availability) | Chưa đo | 99,0% | **99,5%** | Platform Owner |
| Thời gian chạy/lần | 3–6 phút | <8 phút (2 symbol) | **<25 phút (20 symbol)** | Platform Owner |
| Giờ vận hành/tháng | 6h | 6h | **12h (20 symbol)** | Platform Owner |
| Số file/năm | 930.000 | **~8.000** | ~8.000 | Platform Owner |
| Độ trễ truy vấn toàn lake | <10 phút | **<3 phút** | <3 phút | Platform Owner |
| Diễn tập phục hồi (restore drill) | Chưa có | — | **2 lần/năm** | Platform Owner |
| Sự cố mất dữ liệu không tái tạo | 0 | **0** | **0** | Platform Owner |

### 10.3. Cổng quyết định cho ML

Chỉ đầu tư tiếp khi **cả bốn** điều kiện đúng:

1. Có **12+ tháng dữ liệu sạch**.
2. Mô hình **thắng baseline đơn giản** (naive/ARIMA) trên dữ liệu **ngoài mẫu**.
3. Giá trị tạo ra **>$400/năm** (chi phí tiền mặt biên của ML).
4. Có **≥10 giờ/tháng** thực sự dành cho R&D, không lấy từ giờ vận hành.

Khuyến nghị của Phòng Dữ liệu: đặt ngưỡng cao hơn — **>$10.000/năm** — để bù rủi ro nghiên cứu.

---

## 11. Quyết định cần duyệt

| # | Nội dung | Chi phí / nguồn lực | Thời hạn | Điều kiện |
|---|---|---:|---|---|
| **QĐ1** | Duyệt ngân sách 12 tháng, giải ngân theo pha | **$336/năm** (8,7 tr), tối đa $28/tháng | 12 tháng | Hoàn thành P0 trong 30 ngày |
| **QĐ2** | Duyệt gói kỹ thuật ưu tiên P0 | **7 ngày công**, không phát sinh tiền mặt | 45 ngày | Báo cáo kết quả trước đợt 2 |
| **QĐ3** | Duyệt mở rộng 20 symbol khi đạt cổng | Trong ngân sách QĐ1 | Sau 4–6 tháng | Đạt 5 điều kiện mục 7.3 |
| **QĐ4** | Chưa duyệt ML; giữ quỹ dự phòng | $0 giải ngân; quỹ dự phòng $65/tháng từ tháng 7 (~$390) | Xem xét tháng 7 | Đạt 4 cổng mục 10.3, giá trị >$10.000/năm |

> **Trần tiền mặt năm 1:** $336 (QĐ1) + $390 quỹ dự phòng ML (QĐ4, chỉ giải ngân sau cổng) = **$726 (~18,9 triệu VND)**. Nếu cổng ML không đạt, giải ngân thực tế chỉ là **$336**. Gói P0 không dùng tiền mặt.

### Ba việc đề nghị **không** duyệt

1. **Chuyển sang AWS S3** — đắt hơn 88% (B) và 205% (C) do phí truyền dữ liệu ra.
2. **Tự vận hành object storage** — chỉ hòa vốn ở ~1,8 TB (~năm thứ 13) và phát sinh thêm công vận hành.
3. **Bỏ lớp dữ liệu gốc (Bronze) để “tiết kiệm dung lượng”** — Bronze là bản gốc để audit và tái lập; chỉ nên bỏ **bản sao cùng grain ở Silver** (tiết kiệm ~50 GB/năm, không mất khả năng tái lập).

### Nguyên tắc kiểm soát ngân sách

1. Vượt **120% ngân sách P90 tháng** → đóng băng chi tiêu ngoài P0 đến khi có giải trình.
2. Vượt **150%** → CFO phê duyệt lại ngân sách quý.
3. Mọi khoản **mới >$20/tháng** phải được duyệt trước.
4. Nếu **3 tháng liên tiếp** giờ công thực tế <10h → hạ dự báo và giải phóng năng lực; nếu >18h → dừng mở rộng symbol, xử lý nợ kỹ thuật trước.

---

## Phụ lục A — Giả định và phương pháp

### A.1. Số liệu đo thực tế (2 symbol, 5 dataset)

| Lớp | MB/ngày | GB/năm |
|---|---:|---:|
| Bronze (dữ liệu thô) | 27,8 | 10,1 |
| Silver (đã làm sạch) | 25,7 | 9,4 |
| Gold (bảng tổng hợp) | 0,06 | 0,02 |
| State/catalog | 0,5 | 0,18 |
| **Tổng** | **54,1** | **19,7** |

### A.2. Giả định chính

| Thông số | Giá trị | Ghi chú |
|---|---|---|
| Hệ số scale 20 symbol | **7×** | Khoảng 6,2–7,5×; 22% dữ liệu scale 10×, 76% scale theo thanh khoản |
| Giá R2 | $0,015/GB-tháng, free 10 GB, egress $0 | |
| Giá S3 | $0,023/GB-tháng + egress $0,09/GB | |
| VPS | CX32 4 vCPU/8 GB/80 GB + volume | ~$11–12/tháng |
| Tỷ giá | 26.000 VND/USD | |
| Tăng trưởng dữ liệu | Tuyến tính theo ngày | Chưa tính bull-run (có thể ×1,3–1,5 lưu lượng giao dịch) |

### A.3. Công thức chính

```
Dung lượng/năm (GB)      = MB/ngày × 365 / 1024
Chi phí R2/tháng ($)     = max(0, GB_lưu − 10) × 0,015
Tổng 3 năm (tiền mặt)   = Chi phí/tháng × 36 + chi phí một lần
Chi phí/symbol/năm      = TCO 3 năm / (số symbol × 3)
```

### A.4. Phương pháp

Báo cáo được lập bởi hai nhóm độc lập — một nhóm phụ trách kiến trúc/vận hành, một nhóm phụ trách tài chính — sau đó đối chiếu chéo. Các điểm bất đồng được xử lý bằng cách **lấy số bảo thủ hơn để lập ngân sách** và **lấy số trung vị để ra quyết định**.

---

## Phụ lục B — Thuật ngữ

| Thuật ngữ | Giải thích cho lãnh đạo |
|---|---|
| **Bronze / Silver / Gold** | Ba lớp dữ liệu: thô → đã làm sạch → bảng tổng hợp sẵn để báo cáo |
| **DQ gate** | Cổng kiểm tra chất lượng; dữ liệu lỗi thì báo cáo không được dựng |
| **Compaction** | Gộp nhiều file nhỏ thành ít file lớn để truy vấn nhanh |
| **Retention** | Chính sách thời gian lưu dữ liệu trước khi xoá |
| **R2 / S3** | Kho lưu trữ đối tượng (object storage); R2 không tính phí tải dữ liệu ra |
| **TCO** | Tổng chi phí sở hữu trong một khoảng thời gian |
| **P50 / P90** | Mức chi phí kỳ vọng / mức trần ngân sách (90% khả năng không vượt) |
| **Baseline** | Mô hình đơn giản để so sánh (naive/ARIMA); mô hình mới phải thắng nó |
| **Out-of-sample** | Dữ liệu chưa dùng khi huấn luyện, dùng để kiểm tra mô hình có thực sự tốt |

---

*Báo cáo được lập ngày 24/10/2026. Số liệu dung lượng lấy từ đo thực tế trên lake hiện tại; các chi phí khác là ước tính có giả định ghi rõ. Giá nhà cung cấp cần xác nhận lại tại thời điểm mua.*
