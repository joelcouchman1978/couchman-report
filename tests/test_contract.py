from pathlib import Path
from analysis.metrics.contract import validate_csv
def test_contract_holds():
    p = Path("report/tables/sample_metrics.csv")
    validate_csv(p)
