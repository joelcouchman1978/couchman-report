import csv, pathlib
csv_path = pathlib.Path("report/tables/sample_metrics.csv")
rows_path = pathlib.Path("report/tables/sample_metrics_rows.tex")
rows_path.parent.mkdir(parents=True, exist_ok=True)
rows = [
    ["player","metric","mean","lo","hi"],
    ["Toby","Defence+",0.15,0.10,0.20],
    ["Ryan","Defence+",0.12,0.08,0.17],
]
with csv_path.open("w", newline="") as f:
    csv.writer(f).writerows(rows)
with rows_path.open("w") as f:
    for r in rows[1:]:
        f.write(f"{r[0]} & {r[1]} & {r[2]} & {r[3]} & {r[4]} \\\\\n")
print(csv_path.resolve())
print(rows_path.resolve())
