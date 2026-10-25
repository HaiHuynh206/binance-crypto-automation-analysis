---
id: 08
title: Cùng một khoảng ngày hard-code ở 3 nơi
severity: P2
status: open
area: config / reproducibility
files:
  - B4_analysis/src/compute_from_r2.py
  - B4_analysis/notebooks/B4_phan_tich_chat_luong_tt.ipynb
  - B4_analysis/docs/figures/export_figures_r2.py
---

# 08 — Ngày mặc định rải rác 3 chỗ

## Hiện tượng

| Nơi | Giá trị |
|-----|---------|
| `compute_from_r2.py:72-73` | `default="2026-10-19"` / `"2026-10-21"` |
| notebook cell 1 | `RANGE = "2026-10-19_2026-10-21"` |
| `docs/figures/export_figures_r2.py` | `RANGE = "2026-10-19_2026-10-21"` |

## Ảnh hưởng

Chỉ cần chạy lại với khoảng ngày mới (ví dụ thêm 2026-10-22) mà quên một chỗ là:

- notebook đọc thư mục không tồn tại → `FileNotFoundError`;
- hoặc tệ hơn, hình `docs/figures/*.png` vẫn là **số cũ** trong khi báo cáo đã cập nhật.
  Không có gì báo lỗi, vì script figure chỉ ghi đè 3 file PNG trùng tên.

## Cách sửa

Một nguồn duy nhất, có thể đặt trong `src/paths.py` hoặc file config:

```python
# src/paths.py
DEFAULT_RANGE = ("2026-10-19", "2026-10-21")
```

- `compute_from_r2.py`: `default=DEFAULT_RANGE[0]`, `default=DEFAULT_RANGE[1]`.
- Notebook: `from src.paths import DEFAULT_RANGE` (hoặc đọc `data/metrics/source=r2/summary_*.csv`
  mới nhất để tự suy ra range).
- Figures: import cùng hằng số, và ghi thêm ngày vào tên file
  (`r2_amihud_by_hour_2026-10-19_2026-10-21.png`) để không bao giờ lẫn hình cũ.

## Nghiệm thu

- [ ] Chỉ còn **một** chỗ khai báo khoảng ngày.
- [ ] Chạy lại cho kỳ mới: notebook + figures + báo cáo cùng một kỳ, không cần sửa tay 3 chỗ.
- [ ] Tên file hình (hoặc nội dung hình) mang thông tin kỳ dữ liệu.
