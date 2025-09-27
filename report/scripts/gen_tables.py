import csv, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
data_dir = ROOT / "data"
build_dir = ROOT / "build" / "tables"
build_dir.mkdir(parents=True, exist_ok=True)

def emit(csv_name: str, tex_name: str):
    rows = []
    with open(data_dir / csv_name, newline='', encoding='utf-8') as f:
        rd = csv.DictReader(f)
        for r in rd:
            m = r['metric'].strip().replace('%', '\\%')
            v = r['value'].strip()
            lo = r['ci_low'].strip()
            hi = r['ci_high'].strip()
            note = r.get('note', '').strip()
            rows.append(f"\\metricrow{{{m}}}{{{v}}}{{{lo}}}{{{hi}}}{{{note}}}\n")
    (build_dir / tex_name).write_text(''.join(rows), encoding='utf-8')

emit('metrics_toby.csv', 'metrics_toby.tex')
emit('metrics_ryan.csv', 'metrics_ryan.tex')
print("Wrote tex fragments in:", build_dir)
