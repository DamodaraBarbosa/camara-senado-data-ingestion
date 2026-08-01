.PHONY: help build up down logs clean test push-ecr

# Variables
IMAGE_NAME ?= camara-ingestion
IMAGE_TAG ?= latest
AWS_ACCOUNT_ID ?= 904464083417
AWS_REGION ?= us-east-1
ECR_REGISTRY ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
DOCKER_COMPOSE_FILE ?= docker-compose.yml
COMPOSE = docker-compose -f $(DOCKER_COMPOSE_FILE)

help:
	@echo "==================================================="
	@echo "Câmara dos Deputados Data Extraction - Docker Help"
	@echo "==================================================="
	@echo ""
	@echo "Build Commands:"
	@echo "  make build              - Build Docker image"
	@echo "  make build-no-cache     - Build Docker image without cache"
	@echo ""
	@echo "Container Commands:"
	@echo "  make up                 - Start all containers"
	@echo "  make down               - Stop all containers"
	@echo "  make logs               - View logs from all containers"
	@echo "  make logs-proposicoes   - View logs from proposições extractor"
	@echo "  make logs-deputados     - View logs from deputados extractor"
	@echo "  make ps                 - List running containers"
	@echo ""
	@echo "Run Specific Bundle:"
	@echo "  make run-proposicoes    - Run proposições extraction"
	@echo "  make run-deputados      - Run deputados extraction"
	@echo "  make run-votacoes       - Run votações extraction"
	@echo "  make run-orgaos         - Run órgãos extraction"
	@echo "  make run-partidos       - Run partidos extraction"
	@echo "  make run-legislaturas   - Run legislaturas extraction"
	@echo "  make run-blocos         - Run blocos extraction"
	@echo "  make run-frentes        - Run frentes extraction"
	@echo "  make run-eventos        - Run eventos extraction"
	@echo "  make run-grupos         - Run grupos extraction"
	@echo ""
	@echo "Database Commands:"
	@echo "  make db-shell           - Open PostgreSQL shell"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test               - Run unit tests (pytest)"
	@echo ""
	@echo "AWS/ECR Commands:"
	@echo "  make push-ecr           - Push image to ECR (requires AWS credentials)"
	@echo "  make login-ecr          - Login to ECR (requires AWS credentials)"
	@echo ""
	@echo "Cleanup Commands:"
	@echo "  make clean              - Remove containers and volumes"
	@echo "  make clean-all          - Remove everything including images"
	@echo "  make prune              - Remove unused Docker resources"
	@echo ""

# Build
build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "✓ Image built successfully: $(IMAGE_NAME):$(IMAGE_TAG)"

build-no-cache:
	@echo "Building Docker image (no cache)..."
	docker build --no-cache -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "✓ Image built successfully: $(IMAGE_NAME):$(IMAGE_TAG)"

# Container lifecycle
up:
	@echo "Starting containers..."
	$(COMPOSE) up -d
	@echo "✓ Containers started"
	@echo "PostgreSQL is available at localhost:5432"
	@sleep 5
	$(COMPOSE) ps

down:
	@echo "Stopping containers..."
	$(COMPOSE) down
	@echo "✓ Containers stopped"

logs:
	$(COMPOSE) logs -f

logs-proposicoes:
	$(COMPOSE) logs -f extractor-proposicoes

logs-deputados:
	$(COMPOSE) logs -f extractor-deputados

logs-votacoes:
	$(COMPOSE) logs -f extractor-votacoes

logs-orgaos:
	$(COMPOSE) logs -f extractor-orgaos

logs-partidos:
	$(COMPOSE) logs -f extractor-partidos

logs-legislaturas:
	$(COMPOSE) logs -f extractor-legislaturas

logs-blocos:
	$(COMPOSE) logs -f extractor-blocos

logs-frentes:
	$(COMPOSE) logs -f extractor-frentes

logs-eventos:
	$(COMPOSE) logs -f extractor-eventos

logs-grupos:
	$(COMPOSE) logs -f extractor-grupos

ps:
	$(COMPOSE) ps

# Run specific bundles
run-proposicoes:
	$(COMPOSE) run --rm extractor-proposicoes

run-deputados:
	$(COMPOSE) run --rm extractor-deputados

run-votacoes:
	$(COMPOSE) run --rm extractor-votacoes

run-orgaos:
	$(COMPOSE) run --rm extractor-orgaos

run-partidos:
	$(COMPOSE) run --rm extractor-partidos

run-legislaturas:
	$(COMPOSE) run --rm extractor-legislaturas

run-blocos:
	$(COMPOSE) run --rm extractor-blocos

run-frentes:
	$(COMPOSE) run --rm extractor-frentes

run-eventos:
	$(COMPOSE) run --rm extractor-eventos

run-grupos:
	$(COMPOSE) run --rm extractor-grupos

# Database commands
db-shell:
	$(COMPOSE) exec postgres psql -U admin -d camara_data

# Tests
test:
	PYTHONPATH=src pytest tests/ -v

# AWS/ECR commands
login-ecr:
	@echo "Logging into ECR..."
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(ECR_REGISTRY)
	@echo "✓ Logged in to ECR"

push-ecr: build login-ecr
	@echo "Tagging image for ECR..."
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(ECR_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	@echo "Pushing image to ECR..."
	docker push $(ECR_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	@echo "✓ Image pushed to ECR: $(ECR_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)"

# Cleanup
clean:
	@echo "Stopping and removing containers..."
	$(COMPOSE) down
	@echo "✓ Containers removed"

clean-all:
	@echo "Removing containers, volumes, and images..."
	$(COMPOSE) down -v
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG)
	@echo "✓ Everything cleaned up"

prune:
	@echo "Pruning unused Docker resources..."
	docker system prune -f
	@echo "✓ Pruned"

# Info
info:
	@echo "==================================================="
	@echo "Project Information"
	@echo "==================================================="
	@echo "Image: $(IMAGE_NAME):$(IMAGE_TAG)"
	@echo "Compose File: $(DOCKER_COMPOSE_FILE)"
	@echo "Bundles: proposicoes, deputados, votacoes, orgaos, partidos, legislaturas, blocos, frentes, eventos, grupos"
	@echo "==================================================="
