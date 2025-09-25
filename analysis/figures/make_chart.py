import csv, pathlib
import matplotlib.pyplot as plt

csv_path = pathlib.Path("report/tables/sample_metrics.csv")
rows = list(csv.DictReader(csv_path.open()))
names = [r["player"] for r in rows]
vals  = [float(r["mean"]) for r in rows]

out = pathlib.Path("report/figures/defence_plus.png")
out.parent.mkdir(parents=True, exist_ok=True)

plt.figure()
plt.bar(names, vals)
plt.title("Defence+ (mean)")
plt.tight_layout()
plt.savefig(out, dpi=160)
