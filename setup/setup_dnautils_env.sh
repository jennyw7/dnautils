#!/usr/bin/env bash
set -e

ENV_NAME="dnautils"
PYTHON_VERSION="3.11"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

eval "$(conda shell.bash hook)"

conda create -y -n "$ENV_NAME" "python=$PYTHON_VERSION" || true

conda activate "$ENV_NAME"

python -m pip install --upgrade pip "setuptools<81" wheel

cd "$REPO_ROOT"
python -m pip install -e .
python -m pip install ipykernel
python -m ipykernel install --user --name "$ENV_NAME" --display-name "Python ($ENV_NAME)"

python -c "import dnautils; print('dnautils import OK:', dnautils.__file__)"

echo "Setup complete in conda env: $ENV_NAME"
