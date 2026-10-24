# Issue log — B4_as_requested

Danh sách lỗi/bug/vấn đề tìm thấy khi review `B4_as_requested/` (commit `5a37e3a`).
Mỗi issue là một file riêng; trong file có link xuống bằng chứng và script tái hiện.

## Cách đọc (progressive disclosure)

| Tầng | Ở đâu | Mất bao lâu | Dùng khi |
|------|-------|-------------|----------|
| **L0 — Dashboard** | bảng dưới đây | 30 giây | cần biết cái gì hỏng, nặng cỡ nào |
| **L1 — Chi tiết issue** | `NN-*.md` | ~2 phút | cần hiểu nguyên nhân + cách sửa |
| **L2 — Bằng chứng / repro** | `evidence/`, `repro/` | ~10 phút | cần tự kiểm chứng hoặc viết test hồi quy |

Quy ước: mỗi issue có front-matter (id / severity / status / files) để máy đọc được,
rồi các mục cố định **Hiện tượng → Ảnh hưởng → Bằng chứng → Nguyên nhân → Cách sửa → Nghiệm thu**.
Đường dẫn trong front-matter tính từ repo root; link trong thân bài tính tương đối với file.
Mẫu để tạo issue mới: [`_template.md`](_template.md).

## Định nghĩa severity

| Mức | Nghĩa | Xử lý |
|-----|-------|-------|
| **P1** | Sai/không có kết quả, ảnh hưởng trực tiếp nội dung nộp | Bắt buộc sửa trước khi nộp |
| **P2** | Kết quả đúng nhưng dễ sai khi chạy lại / tái lập | Nên sửa |
| **P3** | Vệ sinh repo, không đổi số liệu | Sửa nếu còn thời gian |

## Dashboard

| ID | Sev | Vấn đề | File chính | Trạng thái |
|----|-----|--------|-----------|-----------|
| [01](01-roll-spread-nan.md) | **P1** | Roll spread = NaN 100%, 2 panel biểu đồ trống | `src/metrics_roll_amihud.py:20,54` | open |
| [02](02-price-impact-wrong-unit.md) | **P1** | `price_impact` sai thứ nguyên, nổ khi net flow ≈ 0 | `src/metrics_ofi_impact.py:40-41` | open |
| [03](03-amihud-unit-unclear.md) | **P1** | Amihud là "per 1 giây" nhưng báo cáo không nêu đơn vị | `src/metrics_roll_amihud.py:53`, `docs/B4_bao_cao.md:33-38` | open |
| [04](04-variance-ratio-small-sample.md) | **P1** | VR(q) chỉ còn 6 block/giờ, không có khoảng tin cậy | `src/metrics_variance_ratio.py:21-26` | open |
| [05](05-cache-key-missing-symbols.md) | P2 | `--skip-export` dùng lại cache của symbol khác | `src/compute_from_r2.py:86-87` | open |
| [06](06-no-data-quality-gate.md) | P2 | Không kiểm tra số dòng/giờ thiếu dù báo cáo nói "QA fail-hard" | `src/compute_from_r2.py` | open |
| [07](07-export-count-reads-whole-parquet.md) | P2 | Đọc lại cả parquet chỉ để đếm dòng | `src/compute_from_r2.py:44` | open |
| [08](08-hardcoded-date-range.md) | P2 | Cùng một khoảng ngày hard-code ở 3 nơi | `compute_from_r2.py:72-73`, notebook `RANGE`, `docs/figures/export_figures_r2.py` | open |
| [09](09-leftover-group4-naming.md) | P3 | Còn sót tên cũ `group_4` trong code/docs/notebook | nhiều file | open |
| [10](10-notebook-leaks-local-path.md) | P3 | Output notebook lộ path máy cá nhân | notebook cell 1 | open |
| [11](11-env-example-exposes-endpoint.md) | P3 | `.env.example` commit endpoint R2 + bucket thật | `.env.example` | open |
| [12](12-requirements-hygiene.md) | P3 | Dep chết (`python-dotenv`), không pin, thiếu jupyter | `requirements.txt` | open |
| [13](13-stale-docstrings-dead-code.md) | P3 | Docstring sai + code chết (`n_trades`) | `metrics_roll_amihud.py:39-41`, `compute_from_r2.py:48-53` | open |
| [14](14-no-tests-for-b4.md) | P3 | Không có test nào cho metrics B4 | `tests/` | open |
| [15](15-duckdb-connection-hardening.md) | P3 | `INSTALL httpfs` mỗi lần + nội suy credential vào SQL | `src/r2_lake.py:38-56` | open |
| [16](16-readme-repro-order.md) | P3 | Clone mới không chạy được notebook, README thiếu thứ tự | `README.md` | open |

## Đã kiểm tra và **KHÔNG** phải bug

Ghi lại để khỏi điều tra lại: xem [evidence/verified-ok.md](evidence/verified-ok.md).

- Hive partitioning + filter `symbol`/`date` chạy đúng trên layout lake thật (kể cả khi file đã có cột `symbol`).
- Chiều dấu OFI đúng (`is_buyer_maker`).
- Encoding UTF-8 sạch toàn bộ; số trong `docs/results/*.csv` khớp báo cáo.
- Không có secret nào từng bị commit.

## Thứ tự sửa đề xuất

1. **01 → 03 → 04** (đúng số liệu; 01 còn làm hỏng hình nộp bài).
2. **05 → 07 → 06** (tái lập được, không sợ ra số sai khi chạy lại).
3. **08 → 09 → 10 → 16** (làm gọn repo, hướng dẫn chạy).
4. Còn lại rải rác khi rảnh.

## Cách dùng lại

```bash
# tái hiện bằng chứng (không cần credentials R2)
python repro/roll_spread_nan.py
python repro/duckdb_hive_check.py      # cần: pip install duckdb numpy

# chạy lại đúng luồng thật (cần .env có key R2)
cd B4_as_requested && python -m src.compute_from_r2 --symbols BTCUSDT,ETHUSDT
```
