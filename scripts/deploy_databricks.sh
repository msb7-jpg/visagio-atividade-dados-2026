#!/usr/bin/env bash
# Deploy idempotente dos notebooks e sincronização do workflow CineData Analytics no Databricks

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Validar presença do arquivo .env ou variáveis no ambiente
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO_ROOT/.env"
    set +a
fi

if [ -z "${DATABRICKS_PROFILE:-}" ] || [ -z "${DATABRICKS_JOB_ID:-}" ] || [ -z "${DATABRICKS_WORKSPACE_DIR:-}" ]; then
    echo "Erro: Variáveis DATABRICKS_PROFILE, DATABRICKS_JOB_ID e DATABRICKS_WORKSPACE_DIR são obrigatórias."
    echo "Configure-as em seu arquivo .env com base em .env.example."
    exit 1
fi

PROFILE="$DATABRICKS_PROFILE"
JOB_ID="$DATABRICKS_JOB_ID"
WORKSPACE_DIR="$DATABRICKS_WORKSPACE_DIR"
NOTEBOOKS_DIR="$REPO_ROOT/notebooks"
JOB_YAML="$REPO_ROOT/job.yaml"

log() {
    echo ""
    echo "══════════════════════════════════════════════════════"
    echo "  $1"
    echo "══════════════════════════════════════════════════════"
}

log "1. Sincronização dos notebooks locais para o Databricks Workspace ($WORKSPACE_DIR)"
databricks -p "$PROFILE" workspace mkdirs "$WORKSPACE_DIR"

for notebook_name in "Landing_to_Bronze" "Bronze_to_Silver" "Silver_to_Gold"; do
    echo "Enviando $notebook_name.ipynb..."
    databricks -p "$PROFILE" workspace import \
        --language PYTHON \
        --format JUPYTER \
        --overwrite \
        --file "$NOTEBOOKS_DIR/$notebook_name.ipynb" \
        "$WORKSPACE_DIR/$notebook_name"
done

log "2. Atualizando definições do Job de Orquestração ($JOB_ID) a partir de job.yaml"
JOB_SETTINGS_JSON=$(uv run python3 -c "
import yaml, json

with open('$JOB_YAML', 'r') as yaml_file:
    job_definition = yaml.safe_load(yaml_file)

payload = {
    'job_id': int('$JOB_ID'),
    'new_settings': job_definition
}
print(json.dumps(payload))
")

databricks -p "$PROFILE" jobs reset --json "$JOB_SETTINGS_JSON"

log "Deploy concluído com sucesso!"
echo "Notebooks disponíveis no workspace:"
databricks -p "$PROFILE" workspace list "$WORKSPACE_DIR"
E