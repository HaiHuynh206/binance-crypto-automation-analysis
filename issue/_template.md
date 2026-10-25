---
id: NN
title: <một dòng, nêu hiện tượng chứ không nêu giải pháp>
severity: P1 | P2 | P3
status: open | fixing | fixed | wontfix
area: metrics | pipeline | config | docs | security | testing
files:
  - B4_analysis/src/<file>.py          # đường dẫn tính từ repo root
evidence:
  - issue/evidence/<file>.md              # bỏ nếu không có
repro: issue/repro/<script>.py            # hoặc null
---

# NN — <tiêu đề>

## Hiện tượng

Chuyện gì xảy ra, quan sát được ở đâu (notebook cell nào, file dòng nào, log gì).
Trích code/`file:line` thay vì diễn giải.

## Ảnh hưởng

Nó làm sai cái gì / làm ai đó hiểu nhầm cái gì. Nếu là P3 thì ghi rõ "không đổi số liệu".

## Nguyên nhân

Cơ chế gây ra. Nếu chưa chắc, ghi rõ "giả thuyết" + cách kiểm chứng.

## Cách sửa

Ưu tiên 1-2 phương án kèm diff nhỏ. Nếu có nhiều lựa chọn, ghi rõ cái nào khuyến nghị và vì sao.

## Nghiệm thu

- [ ] tiêu chí kiểm tra được (chạy lệnh gì, thấy gì)
- [ ] …
