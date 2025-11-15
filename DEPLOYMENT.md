# Production Deployment Guide

## Quick Start (5 minutes)

### Option 1: Docker Compose (Recommended for testing)

```bash
# 1. Clone and setup
git clone <repo-url>
cd arol-chatbot
cp backend/.env.example backend/.env

# 2. Edit .env file - ADD YOUR KEYS:
nano backend/.env
# Required: PINECONE_API_KEY, SECRET_KEY

# 3. Start everything
docker-compose up -d

# 4. Access
# Frontend: http://localhost:3000
# API Docs: http://localhost:8000/api/docs
# Grafana: http://localhost:3001 (admin/admin)
```

### Option 2: AWS Production Deployment

## Prerequisites

✅ AWS CLI configured (`aws configure`)
✅ Terraform installed (`brew install terraform`)
✅ Docker installed
✅ Pinecone account with API key
✅ Domain name (optional)

## Step-by-Step AWS Deployment

### 1. Prepare Secrets

```bash
# Create AWS Secrets Manager entries
aws secretsmanager create-secret \
  --name arol-chatbot/pinecone-api-key \
  --secret-string "your-pinecone-api-key"

aws secretsmanager create-secret \
  --name arol-chatbot/secret-key \
  --secret-string "$(openssl rand -hex 32)"

aws secretsmanager create-secret \
  --name arol-chatbot/db-password \
  --secret-string "$(openssl rand -base64 32)"
```

### 2. Provision Infrastructure

```bash
cd infrastructure/terraform

# Initialize Terraform
terraform init

# Review plan
terraform plan \
  -var="db_password=$(aws secretsmanager get-secret-value --secret-id arol-chatbot/db-password --query SecretString --output text)"

# Apply (creates all AWS resources)
terraform apply -auto-approve
```

**Creates:**
- VPC with subnets across 2 AZs
- ECS Cluster (Fargate)
- RDS PostgreSQL database
- ElastiCache Redis
- Application Load Balancer
- ECR repositories
- Security groups
- CloudWatch log groups

**Time:** ~15 minutes

### 3. Build and Push Docker Images

```bash
# Get ECR URLs from Terraform output
export ECR_BACKEND=$(terraform output -raw ecr_backend_url)
export ECR_FRONTEND=$(terraform output -raw ecr_frontend_url)

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_BACKEND

# Build backend
cd ../../backend
docker build -t $ECR_BACKEND:latest .
docker push $ECR_BACKEND:latest

# Build frontend
cd ../frontend
docker build -t $ECR_FRONTEND:latest .
docker push $ECR_FRONTEND:latest
```

### 4. Create ECS Task Definitions

```bash
# Backend task definition
aws ecs register-task-definition \
  --cli-input-json file://infrastructure/ecs/backend-task-def.json

# Frontend task definition
aws ecs register-task-definition \
  --cli-input-json file://infrastructure/ecs/frontend-task-def.json
```

### 5. Create ECS Services

```bash
# Backend service
aws ecs create-service \
  --cluster arol-chatbot-cluster \
  --service-name arol-backend \
  --task-definition arol-backend:1 \
  --desired-count 2 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx,subnet-yyy],securityGroups=[sg-xxx]}" \
  --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:...,containerName=backend,containerPort=8000"

# Frontend service
aws ecs create-service \
  --cluster arol-chatbot-cluster \
  --service-name arol-frontend \
  --task-definition arol-frontend:1 \
  --desired-count 2 \
  --launch-type FARGATE
```

### 6. Configure DNS (Optional)

```bash
# Get ALB DNS name
export ALB_DNS=$(terraform output -raw alb_dns_name)

# Create Route53 record
aws route53 change-resource-record-sets \
  --hosted-zone-id Z123456 \
  --change-batch '{
    "Changes": [{
      "Action": "UPSERT",
      "ResourceRecordSet": {
        "Name": "chatbot.yourdomain.com",
        "Type": "CNAME",
        "TTL": 300,
        "ResourceRecords": [{"Value": "'$ALB_DNS'"}]
      }
    }]
  }'
```

### 7. Setup SSL Certificate

```bash
# Request certificate
aws acm request-certificate \
  --domain-name chatbot.yourdomain.com \
  --validation-method DNS

# Add HTTPS listener to ALB
aws elbv2 create-listener \
  --load-balancer-arn <alb-arn> \
  --protocol HTTPS \
  --port 443 \
  --certificates CertificateArn=<cert-arn> \
  --default-actions Type=forward,TargetGroupArn=<tg-arn>
```

### 8. Configure Auto-Scaling

```bash
# Backend auto-scaling
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id service/arol-chatbot-cluster/arol-backend \
  --scalable-dimension ecs:service:DesiredCount \
  --min-capacity 2 \
  --max-capacity 10

aws application-autoscaling put-scaling-policy \
  --policy-name cpu-scaling \
  --service-namespace ecs \
  --resource-id service/arol-chatbot-cluster/arol-backend \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
    "TargetValue": 70.0,
    "PredefinedMetricSpecification": {
      "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
    }
  }'
```

### 9. Setup Monitoring

```bash
# Create CloudWatch dashboard
aws cloudwatch put-dashboard \
  --dashboard-name arol-chatbot \
  --dashboard-body file://infrastructure/cloudwatch/dashboard.json

# Create alarms
aws cloudwatch put-metric-alarm \
  --alarm-name high-cpu \
  --alarm-description "Alert when CPU exceeds 80%" \
  --metric-name CPUUtilization \
  --namespace AWS/ECS \
  --statistic Average \
  --period 300 \
  --threshold 80 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 2
```

### 10. Verify Deployment

```bash
# Check ECS services
aws ecs describe-services \
  --cluster arol-chatbot-cluster \
  --services arol-backend arol-frontend

# Test health endpoint
curl https://chatbot.yourdomain.com/api/health

# Check logs
aws logs tail /ecs/arol-backend --follow
```

---

## CI/CD Setup (GitHub Actions)

### 1. Configure GitHub Secrets

Go to GitHub Repo → Settings → Secrets → New repository secret

Add:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `PINECONE_API_KEY`
- `SECRET_KEY`

### 2. Enable Actions

The workflow `.github/workflows/ci-cd.yml` will automatically:

**On Pull Request:**
- Run tests
- Check code quality
- Build Docker images (test)

**On Push to Main:**
- Run full test suite
- Build production images
- Push to ECR
- Deploy to ECS
- Run security scans

### 3. Manual Deployment Trigger

```bash
# Trigger deployment manually
gh workflow run ci-cd.yml -f environment=production
```

---

## Monitoring Setup

### Prometheus + Grafana

Already included in `docker-compose.yml`:

```bash
docker-compose up prometheus grafana -d
```

Access Grafana: http://localhost:3001 (admin/admin)

**Pre-configured dashboards:**
- API Performance
- System Resources
- Database Metrics
- Cache Analytics

### Sentry Error Tracking

1. Create Sentry project: https://sentry.io
2. Get DSN
3. Add to `.env`:
```
SENTRY_DSN=https://xxx@xxx.ingest.sentry.io/xxx
```

4. Restart services

---

## Database Migrations

### Run Migrations

```bash
# Local
cd backend
alembic upgrade head

# Production (via ECS exec)
aws ecs execute-command \
  --cluster arol-chatbot-cluster \
  --task <task-id> \
  --container backend \
  --command "alembic upgrade head" \
  --interactive
```

### Create New Migration

```bash
alembic revision -m "add new table"
# Edit the generated file
alembic upgrade head
```

---

## Scaling

### Manual Scaling

```bash
# Scale backend to 5 instances
aws ecs update-service \
  --cluster arol-chatbot-cluster \
  --service arol-backend \
  --desired-count 5

# Scale database
aws rds modify-db-instance \
  --db-instance-identifier arol-chatbot-db \
  --db-instance-class db.t3.medium \
  --apply-immediately
```

### Auto-Scaling (Already configured)

- **CPU > 70%**: Scale up
- **CPU < 30%**: Scale down
- **Min instances**: 2
- **Max instances**: 10

---

## Backup & Recovery

### Database Backups

```bash
# Enable automated backups (already configured in Terraform)
# Manual snapshot
aws rds create-db-snapshot \
  --db-instance-identifier arol-chatbot-db \
  --db-snapshot-identifier backup-$(date +%Y%m%d)

# Restore from snapshot
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier arol-chatbot-db-restored \
  --db-snapshot-identifier backup-20240101
```

### Redis Backups

```bash
# Create backup
aws elasticache create-snapshot \
  --cache-cluster-id arol-chatbot-redis \
  --snapshot-name redis-backup-$(date +%Y%m%d)
```

---

## Troubleshooting

### Check Logs

```bash
# Backend logs
docker-compose logs -f backend

# Or AWS CloudWatch
aws logs tail /ecs/arol-backend --follow

# Frontend logs
docker-compose logs -f frontend
```

### Common Issues

**Issue: Database connection failed**
```bash
# Check security group rules
aws ec2 describe-security-groups --group-ids sg-xxx

# Verify RDS status
aws rds describe-db-instances --db-instance-identifier arol-chatbot-db
```

**Issue: High response times**
```bash
# Check Redis connection
redis-cli -h <redis-endpoint> ping

# Review CloudWatch metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/ECS \
  --metric-name CPUUtilization \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-01T23:59:59Z \
  --period 3600 \
  --statistics Average
```

**Issue: Deployment failed**
```bash
# Check ECS service events
aws ecs describe-services \
  --cluster arol-chatbot-cluster \
  --services arol-backend \
  --query 'services[0].events'

# Check task logs
aws ecs describe-tasks \
  --cluster arol-chatbot-cluster \
  --tasks <task-id>
```

---

## Cost Monitoring

```bash
# Get cost estimate
aws ce get-cost-and-usage \
  --time-period Start=2024-01-01,End=2024-01-31 \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=SERVICE
```

---

## Rollback

```bash
# Rollback to previous task definition
aws ecs update-service \
  --cluster arol-chatbot-cluster \
  --service arol-backend \
  --task-definition arol-backend:1

# Or rollback via Terraform
terraform apply -var="image_tag=previous-sha"
```

---

## Cleanup (Destroy Everything)

```bash
# Delete ECS services first
aws ecs delete-service --cluster arol-chatbot-cluster --service arol-backend --force
aws ecs delete-service --cluster arol-chatbot-cluster --service arol-frontend --force

# Then destroy infrastructure
cd infrastructure/terraform
terraform destroy -auto-approve
```

**Cost:** $0 after cleanup

---

## Production Checklist

Before going live:

- [ ] Configure custom domain
- [ ] Setup SSL certificate
- [ ] Enable auto-scaling
- [ ] Configure backups
- [ ] Setup monitoring alerts
- [ ] Enable CloudWatch logs
- [ ] Configure Sentry
- [ ] Test disaster recovery
- [ ] Load testing
- [ ] Security audit
- [ ] Documentation review

---

For support: [your-email@example.com]
