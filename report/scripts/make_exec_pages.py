import csv, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
data = {
    "toby": ROOT / "data" / "metrics_toby.csv",
    "ryan": ROOT / "data" / "metrics_ryan.csv",
}

def rows(path):
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        return [{k.strip().lower(): v.strip() for k,v in row.items()} for row in r]

def pick(d, name):
    for r in d:
        if r.get("metric","").lower()==name:
            return r
    return None

def bullet(r, lead):
    v = r.get("value","")
    lo = r.get("ci_low","")
    hi = r.get("ci_high","")
    note = r.get("note","")
    ci = f"{lo}–{hi}" if lo and hi else ""
    if ci:
        return f"{lead} {v} (95\\% CI {ci}); {note}."
    return f"{lead} {v}; {note}."

def write_exec(player, title, picks):
    d = rows(data[player])
    parts = []
    for metric, lead in picks:
        r = pick(d, metric.lower())
        if r:
            parts.append(bullet(r, lead))
    tex = [
        f"\\PlayerOnePager{{{title}}}{{",
        parts[0] if len(parts)>0 else "Add bullet 1",
        "}{" ,
        parts[1] if len(parts)>1 else "Add bullet 2",
        "}{" ,
        parts[2] if len(parts)>2 else "Add bullet 3",
        "}{ }",
        "",
        "\\begin{coachnotes}",
        "Auto-generated from CSV; edit phrasing as needed.",
        "\\end{coachnotes}",
        ""
    ]
    out = ROOT / "tex" / "sections" / f"executive_{'toby' if player=='toby' else 'ryan'}.tex"
    out.write_text("\n".join(tex), encoding="utf-8")

write_exec("toby", "Toby Couchman", [
    ("tackles/80", "Defensive work-rate"),
    ("run metres/80", "Ball-carry volume"),
    ("tackle eff. %", "Tackle efficiency"),
])

write_exec("ryan", "Ryan Couchman", [
    ("metres/carry", "Carry efficiency"),
    ("line breaks/80", "Line-breaking rate"),
    ("tackle eff. %", "Defensive efficiency"),
])
print("Wrote executive one-pagers.")
