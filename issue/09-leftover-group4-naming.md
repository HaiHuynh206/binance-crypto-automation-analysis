---
id: 09
title: Còn sót tên cũ group_4
severity: P3
status: open
area: naming
files:
  - B4_as_requested/src/paths.py
  - B4_as_requested/src/__init__.py
  - B4_as_requested/src/r2_lake.py
  - B4_as_requested/docs/B4_bao_cao.md
  - B4_as_requested/notebooks/B4_phan_tich_chat_luong_tt.ipynb
---

# 09 — Sót tên `group_4` sau khi đổi cấu trúc repo

## Hiện tượng

| File | Dòng | Nội dung |
|------|------|----------|
| `src/paths.py` | 1 | `"""Project paths rooted at group_4/."""` |
| `src/__init__.py` | 1 | `# group_4 metrics & ingest package` |
| `src/r2_lake.py` | 4 | `Requires in group_4/.env:` |
| `docs/B4_bao_cao.md` | 3 | `**Nhóm:** group_4` (chỗ này hợp lệ — là tên nhóm) |
| notebook cell 0 | | `**Nhóm:** group_4` (hợp lệ) |

## Ảnh hưởng

Người đọc code tưởng phải tạo thư mục `group_4/.env` thì mới chạy được, trong khi thực tế
là `B4_as_requested/.env`. Gây lỗi "chạy không được" không đáng có khi chấm bài.
(Nhãn "Nhóm: group_4" trong báo cáo/notebook thì **giữ nguyên**.)

## Cách sửa

Chỉ sửa 4 chỗ trong `src/`:

```diff
- """Project paths rooted at group_4/."""
+ """Project paths rooted at B4_as_requested/."""
- # group_4 metrics & ingest package
+ # B4_as_requested metrics & ingest package
- Requires in group_4/.env:
+ Requires in B4_as_requested/.env:
```

## Nghiệm thu

- [ ] `grep -rn "group_4" B4_as_requested/src` không còn kết quả.
- [ ] Các nhãn "Nhóm: group_4" trong báo cáo/notebook vẫn còn (đúng chủ ý).
