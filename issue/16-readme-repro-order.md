---
id: 16
title: README thiếu thứ tự chạy lại từ clone mới
severity: P3
status: open
area: docs
files:
  - B4_analysis/README.md
  - B4_analysis/.gitignore
---

# 16 — Clone mới không chạy được notebook ngay

## Hiện tượng

`README.md` liệt kê lệnh nhưng không nói rõ **thứ tự phụ thuộc**:

```bash
cd B4_analysis
cp .env.example .env
pip install -r requirements.txt
python -m src.r2_lake --dataset klines_1m --limit 10
python -m src.compute_from_r2 --symbols BTCUSDT,ETHUSDT --date-from 2026-10-19 --date-to 2026-10-21
```

Trong khi đó:

- `data/` bị `.gitignore` → clone mới **không có** `data/metrics/source=r2/...`.
- Notebook cell 1 sẽ `raise FileNotFoundError` nếu thiếu metrics (tốt — có thông báo rõ).
- `docs/figures/export_figures_r2.py` **không** được nhắc ở đâu, nhưng hình trong báo cáo
  lại do nó sinh ra.
- Không nói `.env` cần key R2 **read-only**, ai cấp, hay chạy được bao lâu.

## Ảnh hưởng

Người chấm làm đúng README vẫn có thể kẹt: chạy `r2_lake` OK (vì có .env) nhưng quên
`compute_from_r2` thì notebook chết; hoặc chạy xong notebook nhưng hình trong `docs/figures/`
vẫn là bản cũ.

## Cách sửa

Viết lại phần "Chạy" thành luồng có đánh số + nói rõ cái gì cần credentials:

````markdown
## Chạy lại từ đầu (cần key R2 read-only)

```bash
cd B4_analysis
pip install -r requirements.txt
cp .env.example .env          # điền AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY

# 1. kiểm tra kết nối lake
python -m src.r2_lake --dataset klines_1m --limit 5

# 2. tính metrics (bắt buộc trước khi mở notebook)
python -m src.compute_from_r2 --symbols BTCUSDT,ETHUSDT \
       --date-from 2026-10-19 --date-to 2026-10-21

# 3. vẽ hình cho báo cáo
python docs/figures/export_figures_r2.py

# 4. mở notebooks/B4_phan_tich_chat_luong_tt.ipynb → Restart & Run All
```

**Không có key R2?** Notebook vẫn đọc được `docs/results/` (CSV đã commit) nhưng phần
metrics chi tiết thì không — xem cell 1 sẽ báo thiếu file gì.
````

## Nghiệm thu

- [ ] README nêu rõ bước nào cần key R2, bước nào không.
- [ ] Có đủ 4 bước theo đúng thứ tự, gồm cả bước vẽ hình.
- [ ] Người chưa từng xem repo làm theo README từ clone sạch → ra được hình + notebook chạy hết.
