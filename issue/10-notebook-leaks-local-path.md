---
id: 10
title: Output notebook lộ path máy cá nhân
severity: P3
status: open
area: hygiene
files:
  - B4_analysis/notebooks/B4_phan_tich_chat_luong_tt.ipynb
---

# 10 — Notebook lưu đường dẫn tuyệt đối của máy tác giả

## Hiện tượng

Output cell 1 (stream) chứa:

```
Loaded 6 tables from D:\Thsi_2025\bigdata\group_4\data\metrics\source=r2 2026-10-19_2026-10-21
```

## Ảnh hưởng

- Lộ cấu trúc thư mục cá nhân (`D:\Thsi_2025\bigdata\...`) trong bài nộp.
- Người chấm mở notebook thấy path không tồn tại trên máy họ → dễ tưởng notebook không chạy
  được (thực ra chỉ là output cũ), và không phân biệt được "hình này sinh từ dữ liệu nào".
- Trùng với [09](09-leftover-group4-naming.md): path còn ghi `group_4` trong khi repo đã đổi tên.

## Cách sửa

1. Đổi print sang đường dẫn tương đối:

```python
print("Loaded", len(tables), "tables from", METRICS.relative_to(ROOT.parent), RANGE)
# -> Loaded 6 tables from B4_analysis/data/metrics/source=r2 2026-10-19_2026-10-21
```

2. Clear output của cell 1 và chạy lại (Restart & Run All), rồi lưu.
3. Nếu vẫn muốn giữ output làm bằng chứng: thêm `# dữ liệu: <range>` thay vì path tuyệt đối.

## Nghiệm thu

- [ ] `grep -o '[A-Z]:\\\\[^"]*' notebook` không còn kết quả.
- [ ] Notebook chạy sạch từ đầu đến cuối bằng Restart & Run All (khi đã có `data/`).
