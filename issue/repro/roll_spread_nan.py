"""Evidence for issue 01: Roll spread returns NaN almost always on 1-second bars.

Run:  python issue/repro/roll_spread_nan.py
No credentials, no lake data needed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "B4_analysis"))

from src.metrics_roll_amihud import roll_effective_spread  # noqa: E402

N_HOURS = 200
N_SECONDS = 3600


def scenario(name: str, step_sampler) -> None:
    vals = []
    for _ in range(N_HOURS):
        prices = 100.0 + np.cumsum(step_sampler())
        vals.append(roll_effective_spread(pd.Series(prices)))
    vals = np.asarray(vals, dtype=float)
    nan_rate = float(np.isnan(vals).mean())
    print(f"{name:52s} NaN rate = {nan_rate:5.1%}")


def main() -> None:
    rng = np.random.default_rng(0)

    # 1. Realistic 1s klines: ~60% of seconds have no trade -> flat close.
    scenario(
        "60% giây đứng giá + 40% bounce ±0.5",
        lambda: rng.choice([0.0, 0.5, -0.5], size=N_SECONDS, p=[0.6, 0.2, 0.2]),
    )

    # 2. Best case for Roll: pure bid-ask bounce, every second traded.
    scenario(
        "bounce thuần ±0.5 (mọi giây đều có trade)",
        lambda: rng.choice([0.5, -0.5], size=N_SECONDS),
    )

    # 3. Real 1s increments are positively autocorrelated (persistence): one move
    #    spills over several seconds. Cov > 0 -> Roll undefined by construction.
    def persistent(phi: float = 0.1):
        eps = rng.normal(0, 0.5, N_SECONDS)
        steps = np.zeros(N_SECONDS)
        for i in range(1, N_SECONDS):
            steps[i] = phi * steps[i - 1] + eps[i]
        return steps

    scenario("increments tự tương quan dương φ=0.1 (persistence)", persistent)

    print(
        "\nKết luận:\n"
        "  - Kịch bản 'sạch' (thuần bounce) vẫn ~50% NaN: ước lượng covariance trên 3600\n"
        "    quan sát là nhiễu, dấu không ổn định.\n"
        "  - Chỉ cần increments có tự tương quan dương rất nhỏ (φ=0.1) là NaN = 100%, vì\n"
        "    E[cov] > 0 -> hàm trả NaN theo đúng định nghĩa.\n"
        "Dữ liệu 1 giây thật (nhiều giây đứng giá + move lan sang giây kế tiếp) nằm ở nhóm 3,\n"
        "nên output notebook có roll_mean = NaN cho cả BTCUSDT và ETHUSDT."
    )


if __name__ == "__main__":
    main()
