#!/usr/bin/env bash
# deploy.sh orquestra o provisionamento completo na AWS.
# Etapas: ECR -> build/push (5 imagens) -> messaging (SNS FIFO + SQS FIFO) -> ECS.
# Pre-requisitos: aws cli v2 logado, docker rodando, Maven Wrapper presente em cada servico.
#
# Uso: ./scripts/deploy.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ECR_STACK="sns-sqs-filter-fifo-poc-ecr"
MSG_STACK="sns-sqs-filter-fifo-poc-messaging"
ECS_STACK="sns-sqs-filter-fifo-poc-ecs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# get_output extrai o valor de um Output especifico de um stack pelo nome.
# Substitui o pattern OUTPUTS_X=$(...) + awk: cada chamada le direto a fonte,
# sem variaveis intermediarias e sem export para o ambiente do shell.
get_output() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
    --output text
}

echo "==> Validando pre-requisitos"
command -v aws >/dev/null || { echo "aws cli nao encontrado"; exit 1; }
command -v docker >/dev/null || { echo "docker nao encontrado"; exit 1; }
aws sts get-caller-identity --region "$REGION" >/dev/null || { echo "aws cli nao autenticado"; exit 1; }

cd "$ROOT_DIR"

# ========== 1. ECR ==========
echo ""
echo "==> [1/3] Criando stack de ECR ($ECR_STACK)"
aws cloudformation create-stack \
  --region "$REGION" \
  --stack-name "$ECR_STACK" \
  --template-body file://infra/1-ecr.yml

aws cloudformation wait stack-create-complete --region "$REGION" --stack-name "$ECR_STACK"

# ACCOUNT_ID e local: usado no echo abaixo e no docker login da etapa 2 (2 referencias).
ACCOUNT_ID=$(get_output "$ECR_STACK" AccountId)

echo "    AccountId:           $ACCOUNT_ID"
echo "    Ingestor:            $(get_output "$ECR_STACK" IngestorRepositoryUri)"
echo "    Reservation Handler: $(get_output "$ECR_STACK" ReservationHandlerRepositoryUri)"
echo "    Notification:        $(get_output "$ECR_STACK" NotificationRepositoryUri)"
echo "    Fulfillment:         $(get_output "$ECR_STACK" FulfillmentRepositoryUri)"
echo "    Vip Handler:         $(get_output "$ECR_STACK" VipHandlerRepositoryUri)"

# ========== 2. Build & Push das 5 imagens ==========
echo ""
echo "==> [2/3] Autenticando Docker no ECR e enviando 5 imagens"
aws ecr get-login-password --region "$REGION" | \
  docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

./scripts/push-image.sh ms-payment-ingestor    "$(get_output "$ECR_STACK" IngestorRepositoryUri)"
./scripts/push-image.sh ms-reservation-handler "$(get_output "$ECR_STACK" ReservationHandlerRepositoryUri)"
./scripts/push-image.sh ms-notification        "$(get_output "$ECR_STACK" NotificationRepositoryUri)"
./scripts/push-image.sh ms-fulfillment         "$(get_output "$ECR_STACK" FulfillmentRepositoryUri)"
./scripts/push-image.sh ms-vip-handler         "$(get_output "$ECR_STACK" VipHandlerRepositoryUri)"

# ========== 3. Messaging (SNS FIFO + 4 SQS FIFO) ==========
echo ""
echo "==> [3a/3] Criando stack de messaging ($MSG_STACK)"
aws cloudformation create-stack \
  --region "$REGION" \
  --stack-name "$MSG_STACK" \
  --template-body file://infra/2-messaging.yml

aws cloudformation wait stack-create-complete --region "$REGION" --stack-name "$MSG_STACK"

echo "    Topic:        $(get_output "$MSG_STACK" TicketEventsTopicName)"
echo "    Reservation:  $(get_output "$MSG_STACK" ReservationQueueName)"
echo "    Notif Q:      $(get_output "$MSG_STACK" NotificationQueueName)"
echo "    Fulfill Q:    $(get_output "$MSG_STACK" FulfillmentQueueName)"
echo "    Vip Q:        $(get_output "$MSG_STACK" VipQueueName)"

# ========== 4. ECS ==========
echo ""
echo "==> [3b/3] Criando stack de ECS ($ECS_STACK) com 5 services, ALB e path routing"
aws cloudformation create-stack \
  --region "$REGION" \
  --stack-name "$ECS_STACK" \
  --template-body file://infra/3-ecs.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=IngestorImageUri,ParameterValue="$(get_output "$ECR_STACK" IngestorRepositoryUri):latest" \
    ParameterKey=ReservationHandlerImageUri,ParameterValue="$(get_output "$ECR_STACK" ReservationHandlerRepositoryUri):latest" \
    ParameterKey=NotificationImageUri,ParameterValue="$(get_output "$ECR_STACK" NotificationRepositoryUri):latest" \
    ParameterKey=FulfillmentImageUri,ParameterValue="$(get_output "$ECR_STACK" FulfillmentRepositoryUri):latest" \
    ParameterKey=VipHandlerImageUri,ParameterValue="$(get_output "$ECR_STACK" VipHandlerRepositoryUri):latest" \
    ParameterKey=ReservationQueueName,ParameterValue="$(get_output "$MSG_STACK" ReservationQueueName)" \
    ParameterKey=ReservationQueueArn,ParameterValue="$(get_output "$MSG_STACK" ReservationQueueArn)" \
    ParameterKey=NotificationQueueName,ParameterValue="$(get_output "$MSG_STACK" NotificationQueueName)" \
    ParameterKey=NotificationQueueArn,ParameterValue="$(get_output "$MSG_STACK" NotificationQueueArn)" \
    ParameterKey=FulfillmentQueueName,ParameterValue="$(get_output "$MSG_STACK" FulfillmentQueueName)" \
    ParameterKey=FulfillmentQueueArn,ParameterValue="$(get_output "$MSG_STACK" FulfillmentQueueArn)" \
    ParameterKey=VipQueueName,ParameterValue="$(get_output "$MSG_STACK" VipQueueName)" \
    ParameterKey=VipQueueArn,ParameterValue="$(get_output "$MSG_STACK" VipQueueArn)" \
    ParameterKey=TicketEventsTopicArn,ParameterValue="$(get_output "$MSG_STACK" TicketEventsTopicArn)" \
    ParameterKey=TicketEventsTopicName,ParameterValue="$(get_output "$MSG_STACK" TicketEventsTopicName)"

aws cloudformation wait stack-create-complete --region "$REGION" --stack-name "$ECS_STACK"

echo ""
echo "============================================================"
echo "Deploy concluido!"
echo "  ALB:           $(get_output "$ECS_STACK" LoadBalancerUrl)"
echo "  SSE Stream:    $(get_output "$ECS_STACK" NotificationStreamUrl)"
echo "  Cluster:       ecs-poc-cluster"
echo "  Topic:         $(get_output "$MSG_STACK" TicketEventsTopicName)"
echo "============================================================"
echo ""
echo "Proximos passos:"
echo "  ./scripts/smoke-test.sh                        # envia uma reserva e valida fan-out"
echo "  ./scripts/run_k6.sh                            # dispara o teste de carga"
echo "  ./scripts/cleanup.sh                           # destroi tudo ao final"
