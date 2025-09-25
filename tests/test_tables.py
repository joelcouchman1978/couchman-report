from pathlib import Path
def test_metrics_csv_exists_and_has_rows():
    p = Path("report/tables/sample_metrics.csv")
    assert p.exists()
    assert len(p.read_text().strip().splitlines()) >= 3
