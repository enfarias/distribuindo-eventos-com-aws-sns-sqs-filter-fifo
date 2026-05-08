#!/usr/bin/env bash
# run_k6.sh dispara teste de carga contra o ALB usando grafana/k6 em Docker.

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ALB_URL=$(aws cloudformation describe-stacks --region "$REGION" --stack-name sns-sqs-filter-fifo-poc-ecs \
  --query "Stacks[0].Outputs[?OutputKey=='LoadBalancerUrl'].OutputValue" --output text)

echo "==> Teste de carga contra $ALB_URL"
echo "    Stages: 30s ramp-up 80 VUs -> 120s 150 VUs -> 30s pico 200 VUs -> 30s ramp-down"
echo ""

cd "$ROOT_DIR"
# pwd -W retorna o path Windows nativo (C:/...) no GitBash. Em Linux/macOS, cai no fallback.
ROOT_MOUNT=$(pwd -W 2>/dev/null || pwd)
MSYS_NO_PATHCONV=1 docker run --rm -i \
  -v "$ROOT_MOUNT:/workspace" \
  -w /workspace \
  -e BASE_URL="$ALB_URL" \
  grafana/k6:latest run k6/load-test.js

echo ""
echo "==> Monitore em paralelo:"
echo "  aws ecs describe-services --cluster ecs-poc-cluster --services ms-poc-ingestor ms-poc-reservation-handler \\"
echo "    --query 'services[*].{Name:serviceName,Desired:desiredCount,Running:runningCount}' --output table"
echo ""
echo "  for q in sqs-poc-reservation-queue.fifo sqs-poc-notification-queue.fifo sqs-poc-fulfillment-queue.fifo sqs-poc-vip-queue.fifo; do \\"
echo "    URL=\$(aws sqs get-queue-url --queue-name \$q --query QueueUrl --output text); \\"
echo "    aws sqs get-queue-attributes --queue-url \$URL --attribute-names ApproximateNumberOfMessages \\"
echo "      --query \"Attributes.ApproximateNumberOfMessages\" --output text | xargs -I{} echo \"\$q: {} mensagens\"; \\"
echo "  done"
