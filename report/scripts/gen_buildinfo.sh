set -euo pipefail
cd "$(dirname "$0")/.."
SNAPSHOT=${DATA_SNAPSHOT:-$(date +%F)}
SEED=${SEED:-$(date +%Y%m%d)}
SHA=$(git rev-parse --short HEAD 2>/dev/null || echo UNKNOWN)
mkdir -p tex/includes
cat > tex/includes/buildinfo.tex <<EOT
\newcommand{\buildSnapshot}{$SNAPSHOT}
\newcommand{\buildSeed}{$SEED}
\newcommand{\buildGit}{$SHA}
EOT
