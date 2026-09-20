#!/usr/bin/env bash
# Execução e monitoramento em tempo real do Job CineData Lakehouse no Databricks

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Validar presença do arquivo .env ou variáveis no ambiente
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO_ROOT/.env"
    set +a
fi

if [ -z "${DATABRICKS_PROFILE:-}" ] || [ -z "${DATABRICKS_JOB_ID:-}" ]; then
    echo "Erro: Variáveis DATABRICKS_PROFILE e DATABRICKS_JOB_ID são obrigatórias."
    echo "Configure-as em seu arquivo .env com base em .env.example."
    exit 1
fi

PROFILE="$DATABRICKS_PROFILE"
JOB_ID="$DATABRICKS_JOB_ID"
POLL_INTERVAL_SECONDS=10

log() {
    echo ""
    echo "══════════════════════════════════════════════════════"
    echo "  $1"
    echo "══════════════════════════════════════════════════════"
}

log "Disparando execução do Job ID $JOB_ID no Databricks..."

RUN_RESPONSE=$(databricks -p "$PROFILE" jobs run-now "$JOB_ID" --no-wait -o json)
RUN_ID=$(echo "$RUN_RESPONSE" | uv run python3 -c "import sys, json; print(json.load(sys.stdin).get('run_id', ''))")

if [ -z "$RUN_ID" ]; then
    echo "Erro ao capturar o ID da run disparada."
    exit 1
fi

RUN_DETAILS=$(databricks -p "$PROFILE" jobs get-run "$RUN_ID" -o json)
RUN_URL=$(echo "$RUN_DETAILS" | uv run python3 -c "import sys, json; print(json.load(sys.stdin).get('run_page_url', ''))")

echo "  Run ID disparada: $RUN_ID"
echo "  Acompanhe no navegador: $RUN_URL"

log "Aguardando conclusão do pipeline..."

while true; do
    RUN_STATUS_JSON=$(databricks -p "$PROFILE" jobs get-run "$RUN_ID" -o json)
    
    RUN_STATE_INFO=$(echo "$RUN_STATUS_JSON" | uv run python3 -c "
import sys, json

data = json.load(sys.stdin)
state = data.get('state', {})
life_cycle_state = state.get('life_cycle_state', 'UNKNOWN')
result_state = state.get('result_state', '')
state_message = state.get('state_message', '')

tasks = data.get('tasks', [])
task_statuses = []
for task in tasks:
    task_name = task.get('task_key', '')
    task_state = task.get('state', {})
    task_life = task_state.get('life_cycle_state', 'PENDING')
    task_result = task_state.get('result_state', '')
    display_status = f'{task_life}:{task_result}' if task_result else task_life
    task_statuses.append(f'{task_name} ({display_status})')

summary = ' | '.join(task_statuses) if task_statuses else 'Inicializando'
print(f'{life_cycle_state};{result_state};{state_message};{summary}')
")

    LIFE_CYCLE_STATE=$(echo "$RUN_STATE_INFO" | cut -d';' -f1)
    RESULT_STATE=$(echo "$RUN_STATE_INFO" | cut -d';' -f2)
    STATE_MESSAGE=$(echo "$RUN_STATE_INFO" | cut -d';' -f3)
    TASKS_SUMMARY=$(echo "$RUN_STATE_INFO" | cut -d';' -f4)

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] Ciclo: $LIFE_CYCLE_STATE | Tarefas: $TASKS_SUMMARY"

    if [ "$LIFE_CYCLE_STATE" = "TERMINATED" ] || [ "$LIFE_CYCLE_STATE" = "SKIPPED" ] || [ "$LIFE_CYCLE_STATE" = "INTERNAL_ERROR" ]; then
        echo ""
        echo "══════════════════════════════════════════════════════"
        if [ "$RESULT_STATE" = "SUCCESS" ]; then
            echo "  Resultado: SUCESSO! 🚀"
            echo "  Todas as camadas do lakehouse foram processadas com êxito."
            echo "══════════════════════════════════════════════════════"
            exit 0
        else
            echo "  Resultado: FALHA ($RESULT_STATE) ❌"
            if [ -n "$STATE_MESSAGE" ]; then
                echo "  Detalhes: $STATE_MESSAGE"
            fi
            echo "  Consulte o log detalhado em: $RUN_URL"
            echo "══════════════════════════════════════════════════════"
            exit 1
        fi
    fi

    sleep "$POLL_INTERVAL_SECONDS"
done
