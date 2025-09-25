from csv import DictReader
from pathlib import Path

REQUIRED = ["player","metric","mean","lo","hi"]

def validate_csv(p: Path) -> None:
    with p.open(newline="") as f:
        rows = list(DictReader(f))
    cols = rows[0].keys() if rows else []
    missing = [c for c in REQUIRED if c not in cols]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    for r in rows:
        float(r["mean"])
        float(r["lo"])
        float(r["hi"])
