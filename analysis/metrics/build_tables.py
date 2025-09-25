import csv, pathlib
out = pathlib.Path("report/tables/sample_metrics.csv")
out.parent.mkdir(parents=True, exist_ok=True)
rows = [
    ["player","metric","mean","lo","hi"],
    ["Toby","Defence+",0.15,0.10,0.20],
    ["Ryan","Defence+",0.12,0.08,0.17],
]
with out.open("w", newline="") as f:
    csv.writer(f).writerows(rows)
print(out.resolve())
