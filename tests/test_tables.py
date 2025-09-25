from pathlib import Path
def test_metrics_csv_exists_and_has_rows():
    p=Path("report/tables/sample_metrics.csv")
    assert p.exists()
    lines=p.read_text().strip().splitlines()
    assert len(lines) >= 3
