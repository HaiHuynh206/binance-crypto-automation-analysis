"""Project paths rooted at group_4/."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
CLEAN = DATA / "clean"
METRICS = DATA / "metrics"
ENV_FILE = ROOT / ".env"


def ensure_data_dirs() -> None:
    for p in (RAW, CLEAN, METRICS):
        p.mkdir(parents=True, exist_ok=True)
