# Couchman Report

Build (local):
  cd report
  PLAYER_A_NAME="Toby Couchman" PLAYER_B_NAME="Ryan Couchman" ./scripts/run_export.sh
  open ./couchman_report.pdf

Data inputs:
  export STATS_PARQUET="$HOME/FullDataNRL/data/curated/fact_player_game.parquet"
  export DIM_PLAYER="$HOME/FullDataNRL/data/curated/dim_player.parquet"

Draft vs release:
  BUILD=DRAFT make preview
  make

Pipeline:
  DuckDB -> CSVs (data/metrics_*.csv) -> scripts/gen_tables.py -> LaTeX rows (build/tables/*.tex) -> PDF.
