#!/bin/bash

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Funções de logging
log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
  exit 1
}

log_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Variáveis com defaults
BUNDLE=${BUNDLE:-proposicoes}
RUN_ID=${RUN_ID:-docker-run-$(date +%s)}
DEBUG_MODE=${DEBUG_MODE:-false}

# Bundles válidos
VALID_BUNDLES="proposicoes deputados votacoes orgaos partidos legislaturas blocos frentes eventos grupos"

# Validação inicial
log_info "==============================================="
log_info "Câmara dos Deputados Data Extraction - Entry Point"
log_info "==============================================="
log_info "Bundle: $BUNDLE"
log_info "Run ID: $RUN_ID"
log_info "Debug Mode: $DEBUG_MODE"
log_info "Timestamp: $(date)"
log_info ""

# Validar bundle
if ! echo "$VALID_BUNDLES" | grep -qw "$BUNDLE"; then
  log_error "Invalid bundle: '$BUNDLE'"
  log_error "Valid bundles: $VALID_BUNDLES"
  exit 1
fi

# Verificar se o módulo existe
log_info "Validating bundle module..."
if ! python -c "import bundles.${BUNDLE}.app.runner" 2>/dev/null; then
  log_error "Bundle module not found: bundles.${BUNDLE}.app.runner"
  log_error "Check if the bundle exists in /app/bundles/"
  exit 1
fi

log_success "Bundle validation passed"
log_info "Executing: python -m bundles.${BUNDLE}.app.runner"
log_info "-----------------------------------------------"
log_info ""

# Executar bundle
if [ "$DEBUG_MODE" = "true" ]; then
  # Com debug - mostra mais informações
  export PYTHONUNBUFFERED=1
  python -m "bundles.${BUNDLE}.app.runner" -v
else
  # Produção - apenas output necessário
  python -m "bundles.${BUNDLE}.app.runner"
fi

EXIT_CODE=$?

log_info ""
log_info "-----------------------------------------------"

if [ $EXIT_CODE -eq 0 ]; then
  log_success "Bundle execution completed successfully"
  log_info "Bundle: $BUNDLE"
  log_info "Duration: $(date)"
else
  log_error "Bundle execution failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE
