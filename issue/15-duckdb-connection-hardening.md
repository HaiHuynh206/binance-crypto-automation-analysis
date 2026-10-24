---
id: 15
title: duckdb_connect — INSTALL mỗi lần, nội suy credential vào SQL
severity: P3
status: open
area: infra
files:
  - B4_as_requested/src/r2_lake.py
---

# 15 — Kết nối DuckDB chưa được gia cố

## Hiện tượng

`r2_lake.py:38-56`:

```python
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_access_key_id='{cfg['AWS_ACCESS_KEY_ID']}';")
con.execute(f"SET s3_secret_access_key='{cfg['AWS_SECRET_ACCESS_KEY']}';")
```

1. `INSTALL httpfs` chạy **mỗi lần tạo connection** → cần mạng, chậm, và lỗi khó hiểu khi
   máy offline/proxy chặn (dù sau đó cũng cần mạng để đọc R2).
2. Credential được **nội suy thẳng vào chuỗi SQL** — nếu secret chứa dấu `'` (hoặc ai đó
   chỉnh `.env` lạ) thì SQL vỡ, và secret có thể lọt vào log/exception.
3. Với DuckDB 1.1+, cách chính thức là `CREATE SECRET`, cũng tránh được việc set 4 biến riêng lẻ.

## Ảnh hưởng

Không sai số. Rủi ro vận hành: khó debug khi máy khác/proxy khác, và một đường rò rỉ
credential không cần thiết.

## Cách sửa

```python
def duckdb_connect(cfg: dict[str, str]):
    import duckdb
    host = urlparse(cfg["CRYPTO_S3_ENDPOINT_URL"]).netloc
    con = duckdb.connect()
    con.execute("LOAD httpfs")                      # hoặc để DuckDB tự autoload
    con.execute("""
        CREATE OR REPLACE SECRET r2 (
            TYPE s3,
            KEY_ID ?,
            SECRET ?,
            REGION ?,
            ENDPOINT ?,
            URL_STYLE 'path',
            USE_SSL true
        )
    """, [cfg["AWS_ACCESS_KEY_ID"], cfg["AWS_SECRET_ACCESS_KEY"],
          cfg["CRYPTO_S3_REGION"], host])
    con.execute("SET TimeZone='UTC'")
    return con
```

Dùng prepared parameters (`?`) để credential không bao giờ đi qua string interpolation.

## Nghiệm thu

- [ ] Không còn f-string chứa `AWS_SECRET_ACCESS_KEY`.
- [ ] `python -m src.r2_lake --limit 3` chạy được trên máy sạch (chưa từng `INSTALL httpfs`).
- [ ] Cache extension: có thể bật `autoinstall_known_extensions`/`autoload_known_extensions`.
