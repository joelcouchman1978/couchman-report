set -euo pipefail
cd "$(dirname "$0")/.."
: "${PLAYER_A_NAME:=Toby Couchman}"
: "${PLAYER_B_NAME:=Ryan Couchman}"
: "${STATS_PARQUET:=$HOME/FullDataNRL/data/curated/fact_player_game.parquet}"
: "${DIM_PLAYER:=$HOME/FullDataNRL/data/curated/dim_player.parquet}"
duckdb -c ".read scripts/_export_headers.sql"
python3 scripts/gen_tables.py
bash scripts/gen_buildinfo.sh
latexmk -pdf -interaction=nonstopmode -file-line-error tex/main.tex -jobname=couchman_report -outdir=..
echo "OK: ../couchman_report.pdf"
