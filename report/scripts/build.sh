set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/gen_tables.py
MAKEFLAGS= BUILD=${BUILD:-RELEASE} make
