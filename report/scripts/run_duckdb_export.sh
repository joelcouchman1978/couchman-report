set -euo pipefail
cd "$(dirname "$0")/.."
: "${PLAYER_A_NAME:=Toby Couchman}"
: "${PLAYER_B_NAME:=Ryan Couchman}"
: "${STATS_PARQUET:=~/FullDataNRL/data/curated/fact_player_match.parquet}"
python3 scripts/export_duckdb_metrics.py
python3 scripts/gen_tables.py
MAKEFLAGS= BUILD=${BUILD:-RELEASE} make
