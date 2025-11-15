.PHONY: help build up down logs test clean deploy

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all Docker images
	docker-compose build

up: ## Start all services
	docker-compose up -d
	@echo "Services started!"
	@echo "Frontend: http://localhost:3000"
	@echo "Backend API: http://localhost:8000"
	@echo "API Docs: http://localhost:8000/api/docs"
	@echo "Grafana: http://localhost:3001 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"

down: ## Stop all services
	docker-compose down

logs: ## View logs
	docker-compose logs -f

logs-backend: ## View backend logs
	docker-compose logs -f backend

logs-frontend: ## View frontend logs
	docker-compose logs -f frontend

test-backend: ## Run backend tests
	cd backend && pytest -v --cov=app

test-frontend: ## Run frontend tests
	cd frontend && npm test

lint: ## Run linters
	cd backend && black . && flake8
	cd frontend && npm run lint

clean: ## Clean up containers and volumes
	docker-compose down -v
	docker system prune -f

restart: down up ## Restart all services

shell-backend: ## Open backend shell
	docker-compose exec backend bash

shell-frontend: ## Open frontend shell
	docker-compose exec frontend sh

db-migrate: ## Run database migrations
	docker-compose exec backend alembic upgrade head

db-rollback: ## Rollback database migration
	docker-compose exec backend alembic downgrade -1

db-shell: ## Open database shell
	docker-compose exec db psql -U postgres -d arol_chatbot

redis-cli: ## Open Redis CLI
	docker-compose exec redis redis-cli

# AWS Deployment targets
aws-login: ## Login to AWS ECR
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_REGISTRY)

aws-build: ## Build for AWS deployment
	docker build -t $(ECR_BACKEND):latest ./backend
	docker build -t $(ECR_FRONTEND):latest ./frontend

aws-push: aws-login ## Push images to ECR
	docker push $(ECR_BACKEND):latest
	docker push $(ECR_FRONTEND):latest

aws-deploy: aws-push ## Deploy to AWS ECS
	aws ecs update-service --cluster arol-chatbot-cluster --service arol-chatbot-service --force-new-deployment

terraform-init: ## Initialize Terraform
	cd infrastructure/terraform && terraform init

terraform-plan: ## Plan Terraform changes
	cd infrastructure/terraform && terraform plan

terraform-apply: ## Apply Terraform changes
	cd infrastructure/terraform && terraform apply

terraform-destroy: ## Destroy Terraform infrastructure
	cd infrastructure/terraform && terraform destroy

# Development helpers
install-backend: ## Install backend dependencies
	cd backend && pip install -r requirements.txt

install-frontend: ## Install frontend dependencies
	cd frontend && npm install

dev-backend: ## Run backend in dev mode
	cd backend && uvicorn app.main:app --reload

dev-frontend: ## Run frontend in dev mode
	cd frontend && npm run dev

format: ## Format code
	cd backend && black .
	cd frontend && npm run format

type-check: ## Run type checking
	cd backend && mypy app
	cd frontend && npm run type-check

security-scan: ## Run security scan
	docker run --rm -v $(PWD):/app aquasec/trivy fs /app

health-check: ## Check service health
	@echo "Checking backend health..."
	@curl -f http://localhost:8000/api/health || echo "Backend unhealthy"
	@echo "\nChecking frontend..."
	@curl -f http://localhost:3000 || echo "Frontend unhealthy"

stats: ## Show Docker stats
	docker-compose stats

backup-db: ## Backup database
	docker-compose exec -T db pg_dump -U postgres arol_chatbot > backup_$(shell date +%Y%m%d_%H%M%S).sql

restore-db: ## Restore database from backup (usage: make restore-db FILE=backup.sql)
	docker-compose exec -T db psql -U postgres arol_chatbot < $(FILE)
