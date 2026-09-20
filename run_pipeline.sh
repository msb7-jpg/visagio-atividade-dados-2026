#!/usr/bin/env bash
# Script de execução sequencial do pipeline CineData Analytics
# Ordem: 00_Setup (implícito via %run) → Landing→Bronze → Bronze→Silver → Silver→Gold

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NOTEBOOKS_DIR="$REPO_ROOT/notebooks"

log() {
    echo ""
    echo "══════════════════════════════════════════════════════"
    echo "  $1"
    echo "══════════════════════════════════════════════════════"
}

run_notebook() {
    local notebook_name="$1"
    local notebook_path="$NOTEBOOKS_DIR/$notebook_name"

    log "Executando: $notebook_name"
    uv run jupyter nbconvert \
        --to notebook \
        --execute \
        --ExecutePreprocessor.kernel_name=visagio \
        --ExecutePreprocessor.timeout=3600 \
        --output "$notebook_path" \
        "$notebook_path"
    echo "  ✅ $notebook_name concluído."
}

log "CineData Analytics — Pipeline Completo"
echo "  Repositório : $REPO_ROOT"
echo "  Notebooks   : $NOTEBOOKS_DIR"

run_notebook "01_Landing_to_Bronze.ipynb"
run_notebook "02_Bronze_to_Silver.ipynb"
run_notebook "03_Silver_to_Gold.ipynb"

log "Pipeline finalizado com sucesso 🎉"
