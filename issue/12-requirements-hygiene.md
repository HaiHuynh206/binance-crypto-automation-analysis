---
id: 12
title: requirements.txt có dep chết, không pin, thiếu jupyter
severity: P3
status: open
area: packaging
files:
  - B4_as_requested/requirements.txt
  - B4_as_requested/src/r2_lake.py
---

# 12 — Vệ sinh dependency

## Hiện tượng

```
pandas>=2.0
pyarrow>=14.0
requests>=2.31
python-dotenv>=1.0      <-- không dùng
matplotlib>=3.8
numpy>=1.26
duckdb>=1.0
```

1. **`python-dotenv` không được import ở đâu cả.** `r2_lake.load_env()` tự viết parser
   (`r2_lake.py:15-27`). Grep để chắc: `grep -rn "dotenv" src/` → rỗng.
2. **`requests` cũng không dùng** trong B4 (chỉ dùng ở package ingest gốc).
3. **Không có version trần** → hôm nay `duckdb>=1.0` cài 2.0.0 (đã khác API secret), mai có
   thể vỡ. Repo đã từng chạy với duckdb 1.x.
4. **Thiếu `jupyter` / `ipykernel`** dù deliverable chính là notebook — người chấm
   `pip install -r requirements.txt` xong vẫn không mở được notebook.

## Ảnh hưởng

Không sai số, nhưng là "ma sát" đầu tiên khi chấm bài: cài xong vẫn thiếu thứ để chạy, và
khi chạy có thể ra hành vi khác do version mới.

## Cách sửa

```diff
 pandas>=2.0
 pyarrow>=14.0
-python-dotenv>=1.0
 matplotlib>=3.8
 numpy>=1.26
-duckdb>=1.0
+duckdb>=1.0,<3       # hoặc pin ==1.1.3 nếu muốn tái lập tuyệt đối
+jupyterlab>=4.0
```

Thêm `requirements-dev.txt` nếu muốn tách. Nếu quyết định dùng dotenv thật thì
thay parser tay trong `load_env()` bằng `dotenv.dotenv_values(ENV_FILE)` — ngắn hơn và xử
lý được quote/comment đúng chuẩn.

## Nghiệm thu

- [ ] Mọi dòng trong requirements đều được import ít nhất 1 lần.
- [ ] Có upper bound cho duckdb.
- [ ] `pip install -r requirements.txt` → mở được notebook và chạy hết cell.
