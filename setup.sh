#!/usr/bin/env bash
# Recreate the project environment on macOS / Linux.
# Usage:  bash setup.sh          (flexible versions from requirements.txt)
#         bash setup.sh --lock   (exact versions from requirements-lock.txt; needs Python 3.14)
set -euo pipefail
cd "$(dirname "$0")"

REQ="requirements.txt"
[[ "${1:-}" == "--lock" ]] && REQ="requirements-lock.txt"

# 1. Python 3.10+ is required by PySpark 4.2
PY="${PYTHON:-python3}"
if ! command -v "$PY" >/dev/null; then echo "ERROR: python3 not found. Install Python 3.10 or newer."; exit 1; fi
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' \
  || { echo "ERROR: Python 3.10+ needed, found $("$PY" --version)."; exit 1; }
echo "Using $("$PY" --version)"

# 2. Java 17 or 21 is required by Spark (warn only)
if command -v java >/dev/null; then
  java -version 2>&1 | head -1
else
  echo "WARNING: Java not found. Install Java 17 or 21 (e.g. 'brew install openjdk@17' or Eclipse Temurin) before running Spark."
fi

# 3. Virtual environment + packages
"$PY" -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r "$REQ"

# 4. Register the Jupyter kernel the notebooks expect ("Python (fa2-olist)")
python -m ipykernel install --sys-prefix --name fa2-olist --display-name "Python (fa2-olist)"

# 5. Dataset check
if [[ ! -f datasets/olist-dataset/olist_orders_dataset.csv ]]; then
  echo
  echo "NOTE: dataset missing. Download the Olist dataset from"
  echo "  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
  echo "and unzip its 9 CSV files into datasets/olist-dataset/  (see README.md)."
fi

echo
echo "Done. Next:  source .venv/bin/activate  &&  jupyter lab fa2/notebooks/fa2_olist_spark.ipynb"
