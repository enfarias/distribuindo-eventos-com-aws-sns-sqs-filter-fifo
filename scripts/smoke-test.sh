#!/usr/bin/env bash
# smoke-test.sh valida o fluxo fim a fim em AWS real:
# ingestor -> reservation-queue.fifo -> reservation-handler -> SNS FIFO ->
# (notification + fulfillment + vip filtrado por ticketTier).
# Envia 1 reserva com correlation-id unico, espera as 4 filas zerarem e
# procura o id nos logs dos 4 consumidores.

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"

ALB_URL=$(aws cloudformation describe-stacks --region "$REGION" --stack-name sns-sqs-filter-fifo-poc-ecs \
  --query "Stacks[0].Outputs[?OutputKey=='LoadBalancerUrl'].OutputValue" --output text)

RESERVATION_URL=$(aws sqs get-queue-url --region "$REGION" \
  --queue-name sqs-poc-reservation-queue.fifo --query QueueUrl --output text)
NOTIF_URL=$(aws sqs get-queue-url --region "$REGION" \
  --queue-name sqs-poc-notification-queue.fifo --query QueueUrl --output text)
FULFILL_URL=$(aws sqs get-queue-url --region "$REGION" \
  --queue-name sqs-poc-fulfillment-queue.fifo --query QueueUrl --output text)
VIP_URL=$(aws sqs get-queue-url --region "$REGION" \
  --queue-name sqs-poc-vip-queue.fifo --query QueueUrl --output text)

CID="smoke-$(date +%s)"
RESERVATION_ID="res_smoke_$(date +%s)"
SHOW_ID="show_smoke_$(date +%s)"

echo "==> Health check em $ALB_URL/actuator/health"
curl -sf "$ALB_URL/actuator/health" >/dev/null && echo "    OK" || { echo "    FALHOU"; exit 1; }

echo "==> Enviando reserva de teste (correlationId=$CID, reservationId=$RESERVATION_ID, tier=PISTA)"
# tier=PISTA: notification e fulfillment recebem; vip-handler nao recebe (filtro descarta).
curl -s -X POST "$ALB_URL/api/reservations" \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: $CID" \
  -d "{\"reservationId\":\"$RESERVATION_ID\",\"showId\":\"$SHOW_ID\",\"ticketTier\":\"PISTA\",\"quantity\":2,\"unitPriceUsd\":150.00,\"buyerEmail\":\"smoke@example.com\",\"buyerName\":\"Smoke Test\",\"requestedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
  | head -c 200
echo ""

echo "==> Aguardando as 4 filas zerarem (timeout 60s)"
for i in $(seq 1 30); do
  C_RES=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$RESERVATION_URL" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text)
  C_NOT=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$NOTIF_URL" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text)
  C_FUL=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$FULFILL_URL" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text)
  C_VIP=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$VIP_URL" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text)
  echo "    tentativa $i: reservation=$C_RES notification=$C_NOT fulfillment=$C_FUL vip=$C_VIP"
  if [[ "$C_RES" == "0" && "$C_NOT" == "0" && "$C_FUL" == "0" && "$C_VIP" == "0" ]]; then break; fi
  sleep 2
done

echo ""
echo "==> Buscando correlation-id nos logs (ultimos 5 min)"
START_MS=$(( ($(date +%s) - 300) * 1000 ))

for lg in /ecs/ms-poc-reservation-handler /ecs/ms-poc-notification /ecs/ms-poc-fulfillment /ecs/ms-poc-vip-handler; do
  echo ""
  echo "--- $lg ---"
  MSYS_NO_PATHCONV=1 aws logs filter-log-events --region "$REGION" \
    --log-group-name "$lg" \
    --start-time "$START_MS" \
    --filter-pattern "$CID" \
    --query 'events[].message' --output text | head -10
done

echo ""
echo "==> Smoke-test concluido."
