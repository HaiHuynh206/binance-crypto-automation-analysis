---
id: 11
title: .env.example commit endpoint R2 + bucket thật
severity: P3
status: open
area: security
files:
  - B4_as_requested/.env.example
---

# 11 — Endpoint và bucket R2 bị commit

## Hiện tượng

`.env.example` (được commit, repo public):

```env
CRYPTO_S3_ENDPOINT_URL=https://29bc01…2437.r2.cloudflarestorage.com   # account ID thật
CRYPTO_S3_BUCKET=crypto-lake
CRYPTO_S3_PREFIX=crypto
```

`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` **để trống** — tốt. Nhưng endpoint chứa
**Cloudflare account ID** và tên bucket.

## Ảnh hưởng

- Không lộ secret (key trống), nhưng lộ account ID + tên bucket. Kẻ xấu có thể dùng để
  dò/scan endpoint, thử tấn công credential stuffing, hoặc xác nhận tài khoản thuộc ai.
- Mức độ: thấp. Nhiều dự án vẫn commit endpoint vì tiện. Nhưng tiêu chuẩn tốt là để placeholder.

## Cách sửa

```diff
-CRYPTO_S3_ENDPOINT_URL=https://<account_id>.r2.cloudflarestorage.com
+CRYPTO_S3_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
-CRYPTO_S3_BUCKET=crypto-lake
+CRYPTO_S3_BUCKET=<BUCKET_NAME>
```

Endpoint thật chuyển vào `.env` (đã có trong `.gitignore`) hoặc ghi trong tài liệu nội bộ
không public.

Nếu repo public lâu dài và bạn muốn an toàn hơn: rotate key R2 hiện tại (vì bucket đã bị biết
là tồn tại).

## Nghiệm thu

- [ ] `.env.example` không còn account ID / tên bucket thật.
- [ ] Vẫn chạy được sau khi copy sang `.env` và điền giá trị thật.
