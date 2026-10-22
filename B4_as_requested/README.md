# B4_as_requested — MFF4030 market quality

```
crypto_market_ingest (repo này) → R2 lake → compute_from_r2 → notebook / báo cáo
(+ LOBSTER sample: quoted spread)
```

## Chạy

```bash
cd B4_as_requested
cp .env.example .env   # điền AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
pip install -r requirements.txt
python -m src.r2_lake --dataset klines_1m --limit 10
python -m src.compute_from_r2 --symbols BTCUSDT,ETHUSDT --date-from 2026-10-19 --date-to 2026-10-21
```

- Notebook: `notebooks/B4_phan_tich_chat_luong_tt.ipynb`
- Báo cáo: `docs/B4_bao_cao.md`

Lake lab: **BTCUSDT, ETHUSDT**. Không commit `.env` / `data/`.
