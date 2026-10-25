# Đề bài B4 — Đo chất lượng thị trường từ dữ liệu giao dịch tần suất cao

Thuyết trình ở buổi 8 và buổi 9.

**Nộp trước để được nhận xét:** mọi nhóm nộp slide, một trang tóm tắt và sổ tay phân tích trên LMS
ít nhất **ba ngày trước buổi 10**.

---

## 1. Yêu cầu chung

- **Căn cứ.** Đề cương yêu cầu nhóm chọn chủ đề, tìm các nghiên cứu gần đây và phân tích các lý thuyết,
  ứng dụng liên quan tới môn học; các nhóm khác nghe và đặt câu hỏi. Bài thuyết trình tính vào điểm quá trình (A1).
- **Một quyết định tài chính.** Mỗi chủ đề gắn với một quyết định cụ thể của một loại tổ chức. Bài trình bày đi theo
  sáu bước đã dùng suốt học phần: vấn đề và quyết định, dữ liệu, phương pháp, kiến trúc, rủi ro và tuân thủ, khuyến nghị.
- **Có dữ liệu, có kết quả tự chạy.** Mỗi chủ đề kèm một bộ dữ liệu thật tải được và một bộ dự phòng. Mỗi nhóm phải làm
  ít nhất phần phân tích tối thiểu ghi ở chủ đề của mình: mô tả dữ liệu bằng số đo (số dòng, kỳ dữ liệu, biến chính,
  tỷ lệ thiếu), một kết quả tự chạy có so với đường cơ sở và kiểm ngoài mẫu. Nộp notebook cùng slide.
- **Công cụ.** Google Colab như các bài lab trên lớp; sổ tay (notebook) của các buổi là mẫu để chép cách nạp dữ liệu,
  đo và vẽ.
- **Khi tệp dữ liệu quá lớn.** Vài bộ dữ liệu nặng hơn RAM của phiên Colab (ví dụ FNSPID 29,6 GB). Nạp cả tệp bằng một
  lệnh đọc của pandas sẽ làm tràn bộ nhớ và phiên bị ngắt. Có ba cách xử lý, dùng riêng hoặc kết hợp:
  1. đọc từng khối, ví dụ mỗi lần một triệu dòng (tham số `chunksize` của pandas), lọc hoặc tổng hợp xong khối nào thì
     chỉ giữ lại phần kết quả nhỏ của khối đó;
  2. chỉ nạp những cột cần dùng (tham số `usecols`);
  3. dùng PySpark như mục 5 của sổ tay buổi 1, nơi Spark tổng hợp 100 triệu dòng mà không nạp cả bảng vào bộ nhớ.

  Sau khi lọc về vài chục mã hoặc vài năm cần dùng, lưu kết quả thành tệp Parquet để các lần sau đọc nhanh.
- **Điều khoản dữ liệu.** Đọc giấy phép trước khi dùng. Dữ liệu trên Kaggle mỗi học viên tự tải bằng tài khoản của mình
  và không phát lại; dữ liệu có thông tin doanh nghiệp, cá nhân thì không nêu tên trong bài.
- **Nghiên cứu gần đây.** Tài liệu khởi điểm chỉ là điểm xuất phát. Mức đề nghị: mỗi nhóm tự tìm thêm ít nhất ba nghiên
  cứu công bố từ năm 2021, đọc bản gốc và ghi đủ trích dẫn.
- **Quan hệ với đồ án cuối kỳ cá nhân.** Mỗi chủ đề có năm đề tài đồ án cá nhân, ghi ở đề đồ án cuối kỳ. Bài nhóm làm
  phần phân tích tối thiểu chung; đồ án của từng thành viên đi tiếp từ đó theo một câu hỏi riêng, và kết quả nhóm đã
  trình bày không được tính điểm lần nữa ở đồ án.
- **Khoảng cách với Việt Nam.** Phần lớn dữ liệu công khai là của nước khác. Bài phải nói rõ điều gì chuyển sang bối cảnh
  Việt Nam được (phương pháp, khung thẩm định) và điều gì không (con số). Số liệu thị trường Việt Nam do nhóm tự tra,
  ghi nguồn và năm.

---

## 2. Chủ đề B4

**Câu hỏi nghiên cứu:** thanh khoản và tác động giá thay đổi thế nào theo giờ trong ngày và giữa các tài sản, và cần dữ
liệu gì để đo điều đó trên thị trường cổ phiếu Việt Nam?

**Dữ liệu chính:** Binance public data (`data.binance.vision`): nến 1 giây của BTCUSDT khoảng 70 MB nén mỗi tháng; giao
dịch gộp (aggTrades) 358 đến 498 MB nén mỗi tháng, có tệp theo ngày. Dữ liệu thật, tải trực tiếp, CC BY-NC-SA 4.0 theo
điều khoản 26.08.2026: chỉ dùng cho nghiên cứu và giáo dục.

**Dữ liệu dự phòng:** tệp mẫu LOBSTER: sổ lệnh một ngày (21.06.2012) của năm cổ phiếu Mỹ, mỗi tệp 0,5 đến 7,2 MB; nguồn có
thể đóng bất cứ lúc nào nên tải sớm.

**Phân tích tối thiểu:**
- chênh lệch giá hiệu dụng theo Roll;
- thước đo Amihud;
- mất cân bằng dòng lệnh và tác động giá;
- tỷ số phương sai theo giờ;
- so hai giai đoạn hoặc hai tài sản có thanh khoản khác nhau.

**Giới hạn dữ liệu:** thị trường tài sản số giao dịch liên tục, không biên độ, không phiên định kỳ. Việt Nam chưa có dữ
liệu sổ lệnh công khai: sản phẩm là bộ thước đo và danh sách dữ liệu trong nước cần có để áp dụng.

**Tài liệu khởi điểm:**
- Peña (2022), chương 3, và Ferrouhi và Bouabdallaoui (2022), chương 4 của giáo trình.
- Hendershott, Jones và Menkveld (2011), *Does Algorithmic Trading Improve Liquidity?*, Journal of Finance 66(1).
- Brogaard, Hendershott và Riordan (2014), *High-Frequency Trading and Price Discovery*, Review of Financial Studies 27(8).
- Budish, Cramton và Shim (2015), *The High-Frequency Trading Arms Race*, Quarterly Journal of Economics 130(4).
- Amihud (2002), *Illiquidity and Stock Returns*, Journal of Financial Markets 5(1).

**Phải phản biện:**
- ngoại suy từ tài sản số và thị trường Mỹ sang Việt Nam;
- rủi ro vận hành của hệ thống tự động (sự cố ở buổi 4);
- chất lượng thị trường đo bằng chỉ tiêu nào.

---

## 3. Rubric chấm điểm

| Tiêu chí | Điểm | Phải có khi đề xuất | Phải có khi trình bày |
|---|---|---|---|
| Nội dung và lập luận | 40 | Gọi tên một quyết định tài chính của một loại tổ chức và một câu hỏi trung tâm trả lời được bằng dữ liệu; nêu trước đường cơ sở sẽ đem ra so; phạm vi vừa với thời lượng một lượt trình bày. | Đủ sáu bước: vấn đề, dữ liệu, phương pháp, kiến trúc, rủi ro và tuân thủ, khuyến nghị. Có ít nhất một phản biện với chính kết luận của nhóm. Khuyến nghị rõ: làm, làm có điều kiện hay dừng. |
| Nghiên cứu và bằng chứng | 20 | Bảng kê dữ liệu đã tải thử: tên bộ, nơi tải, giấy phép, kỳ dữ liệu, số dòng, số cột, dung lượng đo được, thật hay mô phỏng; kèm một bộ dự phòng. Ba nghiên cứu công bố từ năm 2021 làm điểm tựa. | Mô tả dữ liệu bằng số đo; một kết quả tự chạy có so với đường cơ sở và kiểm ngoài mẫu; mọi con số có nguồn và năm. Dữ liệu mô phỏng hoặc tổng hợp phải ghi nhãn và không dùng làm bằng chứng về thị trường. |
| Hỏi đáp và phản biện | 15 | Nêu hai phản biện mạnh nhất mà nhóm dự kiến sẽ gặp. Câu hỏi không trùng với chủ đề của nhóm khác. | Gửi slide, trang tóm tắt và sổ tay phân tích cho nhóm phản biện đúng hạn. Mọi thành viên trả lời được câu hỏi về dữ liệu và phương pháp. Có hai câu hỏi viết sẵn cho nhóm được phân công phản biện. |
| Hình thức và phong cách | 15 | Dàn bài sáu bước, nằm trong giới hạn số slide của thể thức thuyết trình. | Mỗi slide trả lời một câu hỏi. Hình tự vẽ từ dữ liệu của nhóm, có trục, đơn vị, nguồn và kỳ số liệu; không dùng trục cắt hay hai trục gây hiểu lầm. |
| Thời gian và hợp tác nhóm | 10 | Phân công từng thành viên theo sáu bước; kế hoạch làm việc theo các mốc buổi 5, 6, 7 và hạn nộp lần 1 trước buổi 8. | Đúng thời lượng của thể thức; nộp lần 1 và bản chính thức đúng hạn; mọi thành viên cùng trình bày và cùng trả lời. |

---

## 4. Nơi bắt đầu tìm dữ liệu công khai

Đều là các kho đã dùng trong danh mục này:

- UCI Machine Learning Repository
- OpenML
- Kaggle (cần tài khoản)
- Hugging Face Datasets
- Cổng dữ liệu của cơ quan quản lý như SBA, HMDA và EDGAR của SEC
- Wikimedia Pageviews API
- Binance public data
- Open Source Asset Pricing
- Global Factor Data

Bộ nào cũng phải đọc giấy phép trên trang gốc trước khi dùng.
