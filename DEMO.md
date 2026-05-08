# Demo: Distribuindo Eventos com AWS SNS + SQS

## Sumário

- [1. Introdução](#1-introdução)
- [2. Provisionamento da Infraestrutura](#2-provisionamento-da-infraestrutura)
- [3. Smoke test fim a fim do fan-out](#3-smoke-test-fim-a-fim-do-fan-out)
- [4. Stream SSE em tempo real](#4-stream-sse-em-tempo-real)
- [5. Cenário FIFO: ordem por show](#5-cenário-fifo-ordem-por-show)
- [6. Cenário Filtro VIP](#6-cenário-filtro-vip)
- [7. Cenário Deduplicação](#7-cenário-deduplicação)
- [8. Teste de carga com k6](#8-teste-de-carga-com-k6)
- [9. Cleanup paralelizado](#9-cleanup-paralelizado)
- [10. Resumo da configuração](#10-resumo-da-configuração)

---

## 1. Introdução

Esta demo valida o projeto completo em **AWS real** com cenário de **reserva de ingressos** sob alta concorrência. Cinco microsserviços Spring Boot conversam via 4 filas **SQS FIFO em High Throughput Mode** e um **SNS FIFO topic** com **payload-based filter** na subscription do `ms-vip-handler`. Os serviços rodam em ECS Fargate atrás de um Application Load Balancer com path routing.

Você vai provisionar a infraestrutura, validar fan-out fim a fim, observar o stream SSE, demonstrar ordem dentro de um show, paralelização entre shows distintos, dedup automática via `ContentBasedDeduplication` e o filtro VIP descartando tiers que não interessam ao consumidor.

**O que vamos fazer:**

- Provisionar a infraestrutura completa via CloudFormation (ECR, SNS FIFO + 4 SQS FIFO + filter policy, ECS Fargate + ALB).
- Build e push de 5 imagens Docker para o ECR.
- Validar o fan-out completo com 5 microsserviços, com filtro VIP funcionando.
- Demonstrar **ordem** dentro de um show com FIFO.
- Demonstrar **paralelização** entre shows distintos.
- Demonstrar **deduplicação automática** via `ContentBasedDeduplication`.
- Disparar carga com k6 distribuída em vários `showIds` e observar o paralelismo.

**Ambiente:**

- **Região:** `us-east-1`
- **Cluster ECS:** `ecs-poc-cluster`
- **Services ECS:** `ms-poc-ingestor`, `ms-poc-reservation-handler`, `ms-poc-notification`, `ms-poc-fulfillment`, `ms-poc-vip-handler`
- **Tópico SNS FIFO:** `sns-poc-ticket-events.fifo`
- **Filas SQS FIFO:** `sqs-poc-reservation-queue.fifo`, `sqs-poc-notification-queue.fifo`, `sqs-poc-fulfillment-queue.fifo`, `sqs-poc-vip-queue.fifo`
- **Log groups:** `/ecs/ms-poc-ingestor`, `/ecs/ms-poc-reservation-handler`, `/ecs/ms-poc-notification`, `/ecs/ms-poc-fulfillment`, `/ecs/ms-poc-vip-handler`

---

## 2. Provisionamento da Infraestrutura

### 2.1 Autenticar a AWS CLI

Antes de qualquer chamada AWS, autentique o CLI na conta alvo. Se o token expirar no meio da demo, este passo é o primeiro a refazer.

```bash
aws login
```

*Se solicitada uma região, digite `us-east-1`.*

Confirme que as credenciais estão ativas:

```bash
aws sts get-caller-identity
```

**Saída esperada:**

```json
{
    "UserId": "AIDA...",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/seu-usuario"
}
```

Verifique o acesso à região:

```bash
aws ec2 describe-regions --region-names us-east-1 --output table
```

**Saída:**

```
-----------------------------------------------------------------------------------------
|                                     DescribeRegions                                   |
+---------------------------------------------------------------------------------------+
||                                       Regions                                       ||
|+-------------+-------------------------+-----------------------------+----------------+|
|| Endpoint    |        OptInStatus      |          RegionName         |       ...      ||
|+-------------+-------------------------+-----------------------------+----------------+|
|| ec2.us-east-1.amazonaws.com | opt-in-not-required | us-east-1                       ||
|+-------------+-------------------------+-----------------------------+----------------+|
```

Se a tabela com a região aparecer, o CLI está autenticado.

Defina também uma helper que extrai um output qualquer de um stack pelo nome. Ela é reutilizada em todos os comandos abaixo, evitando que outputs vazem para o ambiente como variáveis exportadas e mantendo cada `create-stack` lendo direto da fonte:

```bash
get_output() {
  aws cloudformation describe-stacks --stack-name "$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
    --output text
}
```
*Basta copiar e colar no mesmo terminal bash que vai executar os comandos abaixo.*

### 2.2 Criar os Repositórios ECR

Subimos o ECR primeiro porque o ECS depende das imagens para iniciar as tasks. São 5 repositórios, um por serviço, todos com a mesma lifecycle policy (expira untagged em 1 dia, mantém últimas 5 tagged).

📁 [`infra/1-ecr.yml`](infra/1-ecr.yml)

```bash
aws cloudformation create-stack \
  --stack-name sns-sqs-filter-fifo-poc-ecr \
  --template-body file://infra/1-ecr.yml

aws cloudformation wait stack-create-complete \
  --stack-name sns-sqs-filter-fifo-poc-ecr
```

Confira que os 5 repositórios foram criados, lendo cada output via `get_output`:

```bash
echo "Account:             $(get_output sns-sqs-filter-fifo-poc-ecr AccountId)"
echo "MS Ingestor:            $(get_output sns-sqs-filter-fifo-poc-ecr IngestorRepositoryUri)"
echo "MS Reservation Handler: $(get_output sns-sqs-filter-fifo-poc-ecr ReservationHandlerRepositoryUri)"
echo "MS Notification:        $(get_output sns-sqs-filter-fifo-poc-ecr NotificationRepositoryUri)"
echo "MS Fulfillment:         $(get_output sns-sqs-filter-fifo-poc-ecr FulfillmentRepositoryUri)"
echo "MS VIP Handler:         $(get_output sns-sqs-filter-fifo-poc-ecr VipHandlerRepositoryUri)"
```

**Saída:**

```
Account:             ****************
MS Ingestor:            ****************.dkr.ecr.us-east-1.amazonaws.com/ms-poc-ingestor
MS Reservation Handler: ****************.dkr.ecr.us-east-1.amazonaws.com/ms-poc-reservation-handler
MS Notification:        ****************.dkr.ecr.us-east-1.amazonaws.com/ms-poc-notification
MS Fulfillment:         ****************.dkr.ecr.us-east-1.amazonaws.com/ms-poc-fulfillment
MS VIP Handler:         ****************.dkr.ecr.us-east-1.amazonaws.com/ms-poc-vip-handler
```

### 2.3 Build e Push das 5 imagens Docker

O login via `aws ecr get-login-password` gera um token temporário para o Docker autenticar no ECR. O `AccountId` é resolvido uma vez para a variável local `ACCOUNT_ID`. Os URIs dos repositórios são resolvidos via `get_output` no momento do tag e push de cada imagem.

```bash
ACCOUNT_ID=$(get_output sns-sqs-filter-fifo-poc-ecr AccountId)

aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com"

INGESTOR_URI=$(get_output sns-sqs-filter-fifo-poc-ecr IngestorRepositoryUri)
RESERVATION_URI=$(get_output sns-sqs-filter-fifo-poc-ecr ReservationHandlerRepositoryUri)
NOTIFICATION_URI=$(get_output sns-sqs-filter-fifo-poc-ecr NotificationRepositoryUri)
FULFILLMENT_URI=$(get_output sns-sqs-filter-fifo-poc-ecr FulfillmentRepositoryUri)
VIP_URI=$(get_output sns-sqs-filter-fifo-poc-ecr VipHandlerRepositoryUri)

(cd ms-payment-ingestor && ./mvnw clean package -DskipTests -q)
docker build -t ms-payment-ingestor ms-payment-ingestor
docker tag ms-payment-ingestor:latest "${INGESTOR_URI}:latest"
docker push "${INGESTOR_URI}:latest"
echo "==> ms-payment-ingestor publicada em ${INGESTOR_URI}:latest"

(cd ms-reservation-handler && ./mvnw clean package -DskipTests -q)
docker build -t ms-reservation-handler ms-reservation-handler
docker tag ms-reservation-handler:latest "${RESERVATION_URI}:latest"
docker push "${RESERVATION_URI}:latest"
echo "==> ms-reservation-handler publicada em ${RESERVATION_URI}:latest"

(cd ms-notification && ./mvnw clean package -DskipTests -q)
docker build -t ms-notification ms-notification
docker tag ms-notification:latest "${NOTIFICATION_URI}:latest"
docker push "${NOTIFICATION_URI}:latest"
echo "==> ms-notification publicada em ${NOTIFICATION_URI}:latest"

(cd ms-fulfillment && ./mvnw clean package -DskipTests -q)
docker build -t ms-fulfillment ms-fulfillment
docker tag ms-fulfillment:latest "${FULFILLMENT_URI}:latest"
docker push "${FULFILLMENT_URI}:latest"
echo "==> ms-fulfillment publicada em ${FULFILLMENT_URI}:latest"

(cd ms-vip-handler && ./mvnw clean package -DskipTests -q)
docker build -t ms-vip-handler ms-vip-handler
docker tag ms-vip-handler:latest "${VIP_URI}:latest"
docker push "${VIP_URI}:latest"
echo "==> ms-vip-handler publicada em ${VIP_URI}:latest"
```

**Saída:** cada serviço imprime "publicada em {repo}:latest" no fim do push.

### 2.4 Criar a stack de messaging (SNS FIFO + 4 SQS FIFO + filtro)

A stack de messaging é independente do ECS, por isso fica em arquivo separado. Aqui estão:

- O tópico SNS FIFO `sns-poc-ticket-events.fifo` (com `ContentBasedDeduplication=true`)
- As 4 filas SQS FIFO em High Throughput Mode (`DeduplicationScope=messageGroup`, `FifoThroughputLimit=perMessageGroupId`)
- As 3 subscriptions SNS para SQS com `RawMessageDelivery=true`
- A `vip-queue` com `FilterPolicy={"ticketTier":["VIP","CAMAROTE"]}` e `FilterPolicyScope=MessageBody`
- As 3 queue policies que liberam o SNS a entregar nas filas assinantes.

📁 [`infra/2-messaging.yml`](infra/2-messaging.yml)

```bash
aws cloudformation create-stack \
  --stack-name sns-sqs-filter-fifo-poc-messaging \
  --template-body file://infra/2-messaging.yml

aws cloudformation wait stack-create-complete \
  --stack-name sns-sqs-filter-fifo-poc-messaging
```

Confirme que o topic FIFO e as 4 filas FIFO existem:

```bash
aws sns list-topics --output text | grep ticket-events
aws sqs list-queues --queue-name-prefix sqs-poc --output text
```

**Saída:**

```
arn:aws:sns:us-east-1:****************:sns-poc-ticket-events.fifo
QUEUEURLS  https://sqs.us-east-1.amazonaws.com/****************/sqs-poc-fulfillment-queue.fifo
QUEUEURLS  https://sqs.us-east-1.amazonaws.com/****************/sqs-poc-notification-queue.fifo
QUEUEURLS  https://sqs.us-east-1.amazonaws.com/****************/sqs-poc-reservation-queue.fifo
QUEUEURLS  https://sqs.us-east-1.amazonaws.com/****************/sqs-poc-vip-queue.fifo
```

E as 3 subscriptions, com filtro só na vip:

//TODO aqui não precisa pegar do output, pegue do list-topics passando o nome do topico
```bash
aws sns list-subscriptions-by-topic \
  --topic-arn "$(get_output sns-sqs-filter-fifo-poc-messaging TicketEventsTopicArn)" \
  --query "Subscriptions[].{Endpoint:Endpoint,Protocol:Protocol}" --output table
```

**Saída:**

```
+----------------------------------------------------------------+----------+
|                            Endpoint                            | Protocol |
+----------------------------------------------------------------+----------+
| arn:aws:sqs:us-east-1:****:sqs-poc-notification-queue.fifo | sqs      |
| arn:aws:sqs:us-east-1:****:sqs-poc-fulfillment-queue.fifo  | sqs      |
| arn:aws:sqs:us-east-1:****:sqs-poc-vip-queue.fifo          | sqs      |
+----------------------------------------------------------------+----------+
```

### 2.5 Criar a stack ECS

Esta stack cria:
- O cluster ECS
- VPC com 2 subnets multi-AZ
- Security Groups por serviço
- IAM Roles granulares (ingestor só `sqs:SendMessage`, reservation-handler com `sqs:Receive` + `sns:Publish`, notification, fulfillment e vip-handler cada um com `sqs:Receive` na própria fila)
- 5 task definitions
- 5 services Fargate
- ALB com path routing (`/api/reservations/*` para o ingestor, `/api/notifications/*` para o stream SSE com stickiness habilitado)
- 5 log groups com retenção 7 dias.

A flag `CAPABILITY_NAMED_IAM` é obrigatória porque criamos 5 roles com nomes fixos para facilitar auditoria.

📁 [`infra/3-ecs.yml`](infra/3-ecs.yml)

Os 16 parâmetros vêm direto dos outputs dos dois stacks predecessores via `get_output` inline. Não há nenhum `export` no shell: cada valor é resolvido no momento do `create-stack` e descartado. O `ALB_URL` no fim é a única variável local que sobrevive, porque é referenciada em todas as próximas seções (smoke test, SSE, FIFO, filtro VIP, dedup).

```bash
aws cloudformation create-stack \
  --stack-name sns-sqs-filter-fifo-poc-ecs \
  --template-body file://infra/3-ecs.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=IngestorImageUri,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-ecr IngestorRepositoryUri):latest \
    ParameterKey=ReservationHandlerImageUri,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-ecr ReservationHandlerRepositoryUri):latest \
    ParameterKey=NotificationImageUri,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-ecr NotificationRepositoryUri):latest \
    ParameterKey=FulfillmentImageUri,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-ecr FulfillmentRepositoryUri):latest \
    ParameterKey=VipHandlerImageUri,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-ecr VipHandlerRepositoryUri):latest \
    ParameterKey=ReservationQueueName,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging ReservationQueueName) \
    ParameterKey=ReservationQueueArn,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging ReservationQueueArn) \
    ParameterKey=NotificationQueueName,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging NotificationQueueName) \
    ParameterKey=NotificationQueueArn,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging NotificationQueueArn) \
    ParameterKey=FulfillmentQueueName,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging FulfillmentQueueName) \
    ParameterKey=FulfillmentQueueArn,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging FulfillmentQueueArn) \
    ParameterKey=VipQueueName,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging VipQueueName) \
    ParameterKey=VipQueueArn,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging VipQueueArn) \
    ParameterKey=TicketEventsTopicArn,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging TicketEventsTopicArn) \
    ParameterKey=TicketEventsTopicName,ParameterValue=$(get_output sns-sqs-filter-fifo-poc-messaging TicketEventsTopicName)

aws cloudformation wait stack-create-complete --stack-name sns-sqs-filter-fifo-poc-ecs

ALB_URL=$(get_output sns-sqs-filter-fifo-poc-ecs LoadBalancerUrl)

echo "ALB URL: $ALB_URL"
echo "SSE URL: $(get_output sns-sqs-filter-fifo-poc-ecs NotificationStreamUrl)/{reservationId}"
```

**Saída:**

```
ALB URL: http://alb-poc-**********.us-east-1.elb.amazonaws.com
SSE URL: http://alb-poc-**********.us-east-1.elb.amazonaws.com/api/notifications/stream/{reservationId}
```

> O script [`scripts/deploy.sh`](scripts/deploy.sh) encadeia todos os comandos acima em uma única execução. Use o passo a passo manual quando quiser inspecionar cada etapa ou validar parâmetros.

### 2.6 Verificar os 5 services rodando

```bash
aws ecs describe-services \
  --cluster ecs-poc-cluster \
  --services ms-poc-ingestor ms-poc-reservation-handler ms-poc-notification ms-poc-fulfillment ms-poc-vip-handler \
  --query "services[*].{Name:serviceName,Desired:desiredCount,Running:runningCount,Pending:pendingCount}" \
  --output table
```

**Saída:**

```
+---------+-------------------------------------+---------+---------+
| Desired |                Name                 | Pending | Running |
+---------+-------------------------------------+---------+---------+
|  2      |  ms-poc-ingestor                    |  0      |  2      |
|  1      |  ms-poc-reservation-handler         |  0      |  1      |
|  1      |  ms-poc-notification                |  0      |  1      |
|  1      |  ms-poc-fulfillment                 |  0      |  1      |
|  1      |  ms-poc-vip-handler                 |  0      |  1      |
+---------+-------------------------------------+---------+---------+
```

### 2.7 Health check do ALB

O ALB encaminha `/actuator/health` para o `IngestorTargetGroup`, então um GET nesse path valida que o ingestor responde através do load balancer.

```bash
curl -s $ALB_URL/actuator/health
```

**Saída:**

```json
{"groups":["liveness","readiness"],"status":"UP"}
```

O `status:"UP"` é o que importa; o `groups` é apenas a listagem dos health groups que o Actuator consolida (Spring Boot 4 inclui isso por padrão).

---

## 3. Smoke test fim a fim do fan-out

Antes de qualquer carga, valide que o fluxo `/api/reservations -> reservation-queue.fifo -> reservation-handler -> SNS topic FIFO -> (notification + fulfillment + vip filtrado)` funciona ponta a ponta com 1 reserva controlada.

A reserva tem `ticketTier=PISTA`, então o filtro do `vip-queue` deve descartar a mensagem antes da entrega: `ms-vip-handler` **não** deve logar nada para esta reserva. Os outros 3 consumidores (reservation-handler, notification, fulfillment) recebem.

```bash
CID="smoke-$(date +%s)"
RESERVATION_ID="res_smoke_$(date +%s)"
SHOW_ID="show_smoke_$(date +%s)"

printf '\n\n\n'
echo "============================================================"
echo "  SMOKE TEST: fan-out fim a fim (1 reserva PISTA)"
echo "  correlationId : $CID"
echo "  reservationId : $RESERVATION_ID"
echo "  showId        : $SHOW_ID"
echo "============================================================"
echo ""

echo "==> Health check em $ALB_URL/actuator/health"
curl -s "$ALB_URL/actuator/health"
echo ""

echo "==> Enviando reserva de teste (correlationId=$CID, tier=PISTA)"
curl -s -X POST "$ALB_URL/api/reservations" \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: $CID" \
  -d '{"reservationId":"'"$RESERVATION_ID"'","showId":"'"$SHOW_ID"'","ticketTier":"PISTA","quantity":2,"unitPriceUsd":150.00,"buyerEmail":"smoke@example.com","buyerName":"Smoke Test","requestedAt":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"}'
echo ""

echo "==> Aguardando consumo das 4 filas (20s)"
sleep 20

echo "==> Aguardando entrega ao CloudWatch Logs (20s)"
sleep 20

# Janela de busca: ultimos 5 minutos
END_S=$(date +%s)
START_S=$(( END_S - 300 ))

# Disparamos a mesma query do Logs Insights via CLI (start-query e
# get-query-results). Usamos o @timestamp do CloudWatch e fazemos
# parse apenas da mensagem final (tudo depois do ultimo " - " da
# linha do logback).
QUERY_STRING="parse @message / - (?<msg>[^\\n]*)/
| filter @message like /$CID/
| sort @timestamp asc
| display @timestamp, msg
| limit 40"

echo "==> Iniciando query Logs Insights nos 4 log groups"
QUERY_ID=$(MSYS_NO_PATHCONV=1 aws logs start-query \
  --log-group-names \
    /ecs/ms-poc-reservation-handler \
    /ecs/ms-poc-notification \
    /ecs/ms-poc-fulfillment \
    /ecs/ms-poc-vip-handler \
  --start-time "$START_S" \
  --end-time "$END_S" \
  --query-string "$QUERY_STRING" \
  --query 'queryId' --output text)

echo "==> Aguardando execucao da query (15s)"
sleep 15

echo ""
echo "--- resultados (timestamp | msg) ---"
MSYS_NO_PATHCONV=1 aws logs get-query-results \
  --query-id "$QUERY_ID" \
  --query 'results[*].[[?field==`@timestamp`]|[0].value, [?field==`msg`]|[0].value]' \
  --output text

echo ""
echo "==> Smoke-test concluido."
```

> **Dica para o vivo:** essa é exatamente a query que o bloco acima dispara via CLI. Para ver no console, abra **CloudWatch > Logs Insights**, selecione os 4 log groups `/ecs/ms-poc-*` e cole a query abaixo (substitua o `CID_AQUI` pelo `correlationId` impresso no banner). Usamos o `@timestamp` que o CloudWatch já fornece e um `parse` simples para extrair `msg` (tudo depois do último ` - ` da linha do logback):
>
> ```
> parse @message / - (?<msg>[^\n]*)/
> | filter @message like /CID_AQUI/
> | sort @timestamp asc
> | display @timestamp, msg
> | limit 40
> ```

**Saída:**

```
==> Health check em http://alb-poc-*.us-east-1.elb.amazonaws.com/actuator/health
    OK
==> Enviando reserva de teste (correlationId=smoke-1746728546, tier=PISTA)
{"status":"accepted","correlationId":"smoke-1746728546","reservationId":"res_smoke_1746728546"}
==> Aguardando as 4 filas zerarem (timeout 60s)
    tentativa 1: reservation=1 notification=0 fulfillment=0 vip=0
    tentativa 2: reservation=0 notification=0 fulfillment=0 vip=0
==> Aguardando entrega ao CloudWatch (15s)

--- /ecs/ms-poc-reservation-handler ---
01:59:55.055 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-payment-ingestor] c.d.r.l.ReservationQueueListener - Reserva recebida da fila: reservationId=res_smoke_1746728546 showId=show_smoke_1746728546
01:59:57.967 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-payment-ingestor] c.d.r.s.ReservationProcessorService - Calculo de valor para reserva res_smoke_1746728546: subtotal=300.00 taxa=24.00 total=324.00
01:59:58.355 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-payment-ingestor] c.d.r.s.ReservationProcessorService - Reserva persistida: reservationId=res_smoke_1746728546 total=324.00 USD
01:59:58.356 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-payment-ingestor] c.d.r.service.TicketEventPublisher - Publicando TicketReservedEvent reservationId=res_smoke_1746728546 showId=show_smoke_1746728546 tier=PISTA no topico sns-poc-ticket-events.fifo
01:59:59.600 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-payment-ingestor] c.d.r.l.ReservationQueueListener - Reserva res_smoke_1746728546 processada com sucesso

--- /ecs/ms-poc-notification ---
02:00:01.179 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-reservation-handler] c.d.n.l.NotificationQueueListener - Notificacao recebida da fila para reserva res_smoke_1746728546 (show show_smoke_1746728546, tier PISTA)
02:00:01.179 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-reservation-handler] c.d.n.s.NotificationBroadcaster - Nenhum cliente conectado ao stream da reserva res_smoke_1746728546, evento descartado para SSE

--- /ecs/ms-poc-fulfillment ---
02:00:02.339 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-reservation-handler] c.d.f.l.FulfillmentQueueListener - Fulfillment recebido para reserva res_smoke_1746728546
02:00:02.340 INFO  [io.awspring.cloud.sqs.sqsListenerEndpointContainer#0-1] [cid=smoke-1746728546 src=ms-reservation-handler] c.d.f.service.FulfillmentService - Liberando ingressos da reserva res_smoke_1746728546 (show show_smoke_1746728546, tier PISTA, 2 ingressos, total 324.00 USD)

--- /ecs/ms-poc-vip-handler ---
(sem resultados: reserva PISTA filtrada pelo broker antes da entrega na vip-queue)
==> Smoke-test concluido.
```

O `correlationId` aparece nos 3 log groups dos consumidores que receberam (reservation-handler, notification, fulfillment) e **não aparece** no log do `vip-handler`, confirmando que o `FilterPolicyScope=MessageBody` aplicado pelo broker descartou a mensagem antes da entrega na fila.

---

## 4. Stream SSE em tempo real

O `ms-notification` mantém um mapa de listas de `SseEmitter` por `reservationId`. Conecte o terminal ao stream do `reservationId` que vai disparar a seguir e crie a reserva em outro terminal: o JSON aparece imediatamente, sem polling, e somente nesse stream.

Em um terminal, conecte o stream do `reservationId` que vai usar logo a seguir:

```bash
RID=a1b2c3d4-e5f6-7890-1234-567890abcdef
curl -N $ALB_URL/api/notifications/stream/$RID
```

O endpoint exige o `reservationId` como path param, então o stream entrega apenas eventos daquele reserva e ignora os demais. A conexão fica aberta e silenciosa, esperando eventos.

Em outro terminal, dispare a reserva com o mesmo `reservationId`:

```bash
curl -X POST $ALB_URL/api/reservations \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-sse-001" \
  -d '{
        "reservationId":"a1b2c3d4-e5f6-7890-1234-567890abcdef",
        "showId":"show_taylor_2026_sao_paulo_2026_05_15",
        "ticketTier":"VIP",
        "quantity":2,
        "unitPriceUsd":380.00,
        "buyerEmail":"buyer@example.com",
        "requestedAt":"2026-05-02T14:31:22Z"
      }'
```

**Saída no terminal do `curl -N`:**

```
event:ticket-reserved
data:{"reservationId":"a1b2c3d4-e5f6-7890-1234-567890abcdef","showId":"show_taylor_2026_sao_paulo_2026_05_15","ticketTier":"VIP","quantity":2,"totalAmountUsd":820.80,"buyerEmail":"buyer@example.com","reservedAt":"2026-05-02T14:31:22.987Z"}
```

A latência típica do POST ao SSE fica em torno de 1 a 2 segundos: tempo de o reservation-handler processar a reserva (validação + cálculo + persistência), publicar no SNS topic e o notification consumir e empurrar pelo emitter.

---

## 5. Cenário FIFO: ordem por show

Aqui é onde o FIFO faz diferença sobre o Standard. Vamos disparar 5 reservas para o **mesmo showId** em rápida sucessão e verificar nos logs do `reservation-handler` que as 5 reservas foram processadas **na ordem em que chegaram**, sem intercalação.

```bash
SHOW=show_taylor_2026_sao_paulo_2026_05_15
for i in $(seq 1 5); do
  RID="rsv-$(date +%s)-$RANDOM$RANDOM"
  curl -s -X POST $ALB_URL/api/reservations \
    -H "Content-Type: application/json" \
    -H "X-Correlation-ID: fifo-test-$i" \
    -d "{\"reservationId\":\"$RID\",\"showId\":\"$SHOW\",\"ticketTier\":\"PISTA\",\"quantity\":1,\"unitPriceUsd\":300.00,\"buyerEmail\":\"buyer$i@example.com\",\"requestedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
  echo "enviada reserva $i: $RID"
done
```

Confira nos logs do reservation-handler que as 5 reservas saíram em ordem sequencial (timestamps crescentes, mesmo `MessageGroupId`):

```bash
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/ms-poc-reservation-handler \
  --filter-pattern '"fifo-test"' \
  --start-time $(($(date +%s%N)/1000000 - 300000)) \
  --query "events[*].[timestamp,message]" --output text
```

**Saída:**

```
1746201082000  ... [cid=fifo-test-1] Reserva recebida da fila: <uuid-1> (show=show_taylor_..., tier=PISTA)
1746201082300  ... [cid=fifo-test-2] Reserva recebida da fila: <uuid-2> (show=show_taylor_..., tier=PISTA)
1746201082600  ... [cid=fifo-test-3] Reserva recebida da fila: <uuid-3> (show=show_taylor_..., tier=PISTA)
1746201082900  ... [cid=fifo-test-4] Reserva recebida da fila: <uuid-4> (show=show_taylor_..., tier=PISTA)
1746201083200  ... [cid=fifo-test-5] Reserva recebida da fila: <uuid-5> (show=show_taylor_..., tier=PISTA)
```

Mesma verificação no `ms-fulfillment`: as 5 mensagens chegam preservando a ordem dentro do `MessageGroupId=showId` (FIFO + `RawMessageDelivery=true` propagam o group id automaticamente do topic para as filas assinantes).

Agora o teste de **paralelização entre shows distintos**: dispare 5 reservas para o `show_coldplay_2026_rio_2026_06_01` em paralelo com 5 reservas para o `show_taylor_2026_sao_paulo_2026_05_15`. O padrão abaixo usa subshells `( ... )` em background com PIDs capturados, mais previsível em Git Bash do que `{ ... } &` aninhado.

```bash
( for i in $(seq 1 5); do
    RID="rsv-$(date +%s)-$RANDOM$RANDOM-taylor-$i"
    curl -s -X POST "$ALB_URL/api/reservations" -H "Content-Type: application/json" \
      -H "X-Correlation-ID: paralelo-taylor-$i" \
      -d "{\"reservationId\":\"$RID\",\"showId\":\"show_taylor_2026_sao_paulo_2026_05_15\",\"ticketTier\":\"PISTA\",\"quantity\":1,\"unitPriceUsd\":300.00,\"buyerEmail\":\"taylor$i@example.com\",\"requestedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
  done ) &
PID_TAYLOR=$!

( for i in $(seq 1 5); do
    RID="rsv-$(date +%s)-$RANDOM$RANDOM-coldplay-$i"
    curl -s -X POST "$ALB_URL/api/reservations" -H "Content-Type: application/json" \
      -H "X-Correlation-ID: paralelo-coldplay-$i" \
      -d "{\"reservationId\":\"$RID\",\"showId\":\"show_coldplay_2026_rio_2026_06_01\",\"ticketTier\":\"PISTA\",\"quantity\":1,\"unitPriceUsd\":280.00,\"buyerEmail\":\"coldplay$i@example.com\",\"requestedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
  done ) &
PID_COLDPLAY=$!

wait $PID_TAYLOR $PID_COLDPLAY
echo "Os 10 envios paralelos terminaram (5 Taylor + 5 Coldplay)."
```

Nos logs do reservation-handler, observe que as 10 mensagens **intercalam** entre os 2 shows (paralelização entre `MessageGroupId` distintos), mas dentro de cada show a ordem das 5 reservas é preservada. É exatamente isso que `FifoThroughputLimit=perMessageGroupId` habilita: cada show vira uma fila lógica independente, processada em paralelo aos demais.

---

## 6. Cenário Filtro VIP

Vamos disparar 1 reserva com `ticketTier=PISTA` e 1 com `ticketTier=VIP`, e validar que o broker SNS aplica o `FilterPolicy={"ticketTier":["VIP","CAMAROTE"]}` da subscription do `vip-queue.fifo`. O `ms-vip-handler` deve processar **apenas a VIP**, e o `ms-fulfillment` deve processar **as duas** (sem filtro).

```bash
SHOW=show_metallica_2026_curitiba_2026_06_15

RID_PISTA="rsv-$(date +%s)-$RANDOM$RANDOM"
curl -s -X POST $ALB_URL/api/reservations -H "Content-Type: application/json" \
  -H "X-Correlation-ID: filter-pista-001" \
  -d "{\"reservationId\":\"$RID_PISTA\",\"showId\":\"$SHOW\",\"ticketTier\":\"PISTA\",\"quantity\":1,\"unitPriceUsd\":250.00,\"buyerEmail\":\"pista@example.com\",\"requestedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
echo

RID_VIP="rsv-$(date +%s)-$RANDOM$RANDOM"
curl -s -X POST $ALB_URL/api/reservations -H "Content-Type: application/json" \
  -H "X-Correlation-ID: filter-vip-001" \
  -d "{\"reservationId\":\"$RID_VIP\",\"showId\":\"$SHOW\",\"ticketTier\":\"VIP\",\"quantity\":2,\"unitPriceUsd\":480.00,\"buyerEmail\":\"vip@example.com\",\"requestedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
echo

echo "Reserva PISTA: $RID_PISTA"
echo "Reserva VIP:   $RID_VIP"
```

Aguarde uns 5 segundos e busque os correlation-ids no log do vip-handler:

```bash
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/ms-poc-vip-handler \
  --filter-pattern '?"filter-vip-001" ?"filter-pista-001"' \
  --start-time $(($(date +%s%N)/1000000 - 120000)) \
  --query "events[*].message" --output text
```

**Saída:**

```
... [cid=filter-vip-001] VipQueueListener - Credencial VIP emitida para reserva <RID_VIP> (show=show_metallica_..., tier=VIP)
... [cid=filter-vip-001] VipHandlerService - Concierge agendado para reserva <RID_VIP>, contato vip@example.com
```

Apenas a reserva VIP aparece. A `filter-pista-001` foi descartada pelo broker antes mesmo de tocar a `vip-queue.fifo`, então não chegou no `ms-vip-handler`.

Já no `ms-fulfillment`, ambas devem aparecer:

```bash
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/ms-poc-fulfillment \
  --filter-pattern '?"filter-vip-001" ?"filter-pista-001"' \
  --start-time $(($(date +%s%N)/1000000 - 120000)) \
  --query "events[*].message" --output text
```

**Saída:**

```
... [cid=filter-pista-001] FulfillmentQueueListener - Liberando ingressos da reserva <RID_PISTA> (...)
... [cid=filter-vip-001]   FulfillmentQueueListener - Liberando ingressos da reserva <RID_VIP> (...)
```

Para conferir a contagem de mensagens filtradas pelo broker, consulte a métrica `NumberOfNotificationsFilteredOut` do tópico SNS:

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/SNS \
  --metric-name NumberOfNotificationsFilteredOut \
  --dimensions Name=TopicName,Value=sns-poc-ticket-events.fifo \
  --start-time $(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum --output table
```

**Saída:**

```
+----------------+-------+
|   Timestamp    |  Sum  |
+----------------+-------+
|  2026-05-02T...|  1.0  |
+----------------+-------+
```

A `Sum=1` confirma que o broker filtrou exatamente 1 mensagem (a `filter-pista-001`), exatamente o esperado.

---

## 7. Cenário Deduplicação

O FIFO HT com `ContentBasedDeduplication=true` (na fila e no topic) descarta mensagens duplicadas dentro de uma janela de 5 minutos. Para validar, dispare a **mesma reserva** (mesmo `reservationId`, mesmo body) duas vezes em sequência, com ~2 segundos entre as duas:

```bash
SHOW=show_beyonce_2026_brasilia_2026_07_01
RID="rsv-$(date +%s)-$RANDOM$RANDOM"
PAYLOAD='{"reservationId":"'$RID'","showId":"'$SHOW'","ticketTier":"PISTA","quantity":2,"unitPriceUsd":350.00,"buyerEmail":"dedup@example.com","requestedAt":"2026-05-02T14:31:22Z"}'

echo "==> 1a tentativa"
curl -s -X POST $ALB_URL/api/reservations -H "Content-Type: application/json" \
  -H "X-Correlation-ID: dedup-test-001" -d "$PAYLOAD"
echo

sleep 2

echo "==> 2a tentativa (mesmo reservationId, mesmo body)"
curl -s -X POST $ALB_URL/api/reservations -H "Content-Type: application/json" \
  -H "X-Correlation-ID: dedup-test-002" -d "$PAYLOAD"
echo
```

A API do ingestor é idempotente do ponto de vista do cliente: as 2 chamadas retornam `202 Accepted`. Mas o SQS FIFO descarta a segunda mensagem (mesmo body, mesmo `MessageDeduplicationId` calculado pelo `ContentBasedDeduplication`), então o `reservation-handler` deve processar **apenas 1**:

```bash
sleep 5

MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /ecs/ms-poc-reservation-handler \
  --filter-pattern "\"$RID\"" \
  --start-time $(($(date +%s%N)/1000000 - 120000)) \
  --query "events[*].message" --output text
```

**Saída:**

```
... [cid=dedup-test-001] ReservationQueueListener - Reserva recebida da fila: <RID> (show=show_beyonce_..., tier=PISTA)
... [cid=dedup-test-001] ReservationProcessorService - Reserva persistida...
```

Apenas 1 entrada com aquele `reservationId`. Confirmou: a 2ª mensagem foi descartada pelo SQS antes mesmo de tocar o consumer. Note que o `correlationId` que aparece nos logs é o da primeira tentativa, porque foi a primeira que entrou na fila.

Mesmo padrão acontece nos consumidores downstream (notification, fulfillment), porque o reservation-handler só publicou no topic 1 vez (idempotência via `existsByReservationId`), e o topic FIFO + filas assinantes também propagam dedup.

---

## 8. Teste de carga com k6

O k6 dispara o stress contra o endpoint do ingestor e cobre todo o fan-out automaticamente. O script distribui as reservas entre 5 `showIds` distintos, evidenciando paralelização visível por show: a `reservation-queue.fifo` (com `FifoThroughputLimit=perMessageGroupId`) processa 5 fluxos paralelos no reservation-handler.

📁 [`k6/load-test.js`](k6/load-test.js)

```bash
ROOT_MOUNT=$(pwd -W 2>/dev/null || pwd)

echo "==> Teste de carga contra $ALB_URL"
echo "    Stages: 30s ramp-up 80 VUs -> 120s 150 VUs -> 30s pico 200 VUs -> 30s ramp-down"

MSYS_NO_PATHCONV=1 docker run --rm -i \
  -v "$ROOT_MOUNT:/workspace" \
  -w /workspace \
  -e BASE_URL="$ALB_URL" \
  grafana/k6:latest run k6/load-test.js
```

**Saída:**

```
█ TOTAL RESULTS
  checks_succeeded................: 100.00% 37440 out of 37440
  http_req_failed.................: 0.00%   0 out of 18720
  http_reqs.......................: 18720   89.12/s
  http_req_duration...............: p(95)=4.03s
  reservation_published_total.....: 18720
  reservation_response_time.......: p(95)=4.03s
```

Em paralelo, observe as 4 filas FIFO drenarem:

```bash
for q in sqs-poc-reservation-queue.fifo sqs-poc-notification-queue.fifo sqs-poc-fulfillment-queue.fifo sqs-poc-vip-queue.fifo; do
  URL=$(aws sqs get-queue-url --queue-name $q --query QueueUrl --output text)
  echo "$q: $(aws sqs get-queue-attributes --queue-url $URL \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text) mensagens"
done
```

**Saída (durante o pico):**

```
sqs-poc-reservation-queue.fifo: 4382 mensagens
sqs-poc-notification-queue.fifo: 1837 mensagens
sqs-poc-fulfillment-queue.fifo: 1845 mensagens
sqs-poc-vip-queue.fifo: 462 mensagens
```

Note que a `vip-queue.fifo` carrega cerca de 25% do volume das demais (o k6 distribui as 4 tiers `VIP`, `CAMAROTE`, `PISTA`, `ARQUIBANCADA` uniformemente, e o filtro só aceita `VIP` + `CAMAROTE`, ou seja, 50% das mensagens; somado ao tempo de filtro do broker, o backlog do vip fica menor que o das outras 2 filas que recebem tudo).

Para visualizar a paralelização por show no CloudWatch, abra o `Container Insights` do ECS Cluster e observe as métricas `ApproximateNumberOfMessagesVisible` da `reservation-queue.fifo` e `notification-queue.fifo`. Em FIFO HT, o número de tasks consumidoras consegue processar até 5 fluxos paralelos (1 por `showId`), enquanto em FIFO standard a fila processaria sequencialmente.

---

## 9. Cleanup paralelizado

O projeto mantém 5 services ECS rodando 24/7 enquanto provisionado. Mesmo com Fargate elegível para free-tier, ALB, log groups e métricas continuam gerando custo. Sempre limpe o ambiente ao final.

As 3 stacks são independentes no CloudFormation, então as deletes podem rodar em paralelo. Lembre de limpar as imagens do ECR antes, pois o CloudFormation se recusa a deletar repositório com imagens dentro.

```bash
# 1. Limpa imagens dos 5 ECRs (force delete)
aws ecr delete-repository --repository-name ms-poc-ingestor             --force
aws ecr delete-repository --repository-name ms-poc-reservation-handler  --force
aws ecr delete-repository --repository-name ms-poc-notification         --force
aws ecr delete-repository --repository-name ms-poc-fulfillment          --force
aws ecr delete-repository --repository-name ms-poc-vip-handler          --force

# 2. Dispara as 3 deletes em paralelo
aws cloudformation delete-stack --stack-name sns-sqs-filter-fifo-poc-ecs &
aws cloudformation delete-stack --stack-name sns-sqs-filter-fifo-poc-messaging &
aws cloudformation delete-stack --stack-name sns-sqs-filter-fifo-poc-ecr &
wait

# 3. Aguarda cada uma completar
aws cloudformation wait stack-delete-complete --stack-name sns-sqs-filter-fifo-poc-ecs
aws cloudformation wait stack-delete-complete --stack-name sns-sqs-filter-fifo-poc-messaging
aws cloudformation wait stack-delete-complete --stack-name sns-sqs-filter-fifo-poc-ecr

# 4. Log groups remanescentes (5)
MSYS_NO_PATHCONV=1 aws logs delete-log-group --log-group-name /ecs/ms-poc-ingestor             2>/dev/null || true
MSYS_NO_PATHCONV=1 aws logs delete-log-group --log-group-name /ecs/ms-poc-reservation-handler  2>/dev/null || true
MSYS_NO_PATHCONV=1 aws logs delete-log-group --log-group-name /ecs/ms-poc-notification         2>/dev/null || true
MSYS_NO_PATHCONV=1 aws logs delete-log-group --log-group-name /ecs/ms-poc-fulfillment          2>/dev/null || true
MSYS_NO_PATHCONV=1 aws logs delete-log-group --log-group-name /ecs/ms-poc-vip-handler          2>/dev/null || true
```

Confirme que nada sobrou:

```bash
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
  --query "StackSummaries[?contains(StackName, 'sns-sqs-poc')].StackName" --output text
```

A saída deve ser vazia. Ambiente zerado.

---

## 10. Resumo da Configuração

Para referência rápida, a configuração efetiva aplicada pelos templates:

| Componente | Parâmetro | Valor |
|---|---|---|
| **ECS Ingestor** | CPU / Memory | 256 / 512 |
| | DesiredCount | 2 |
| **ECS Reservation Handler** | CPU / Memory | 512 / 1024 |
| | DesiredCount | 1 |
| **ECS Notification** | CPU / Memory | 256 / 512 |
| | DesiredCount | 1 |
| **ECS Fulfillment** | CPU / Memory | 256 / 512 |
| | DesiredCount | 1 |
| **ECS VIP Handler** | CPU / Memory | 256 / 512 |
| | DesiredCount | 1 |
| **SNS Topic** | Tipo | FIFO |
| | ContentBasedDeduplication | true |
| **SQS principais** | Tipo | FIFO HT |
| | DeduplicationScope | messageGroup |
| | FifoThroughputLimit | perMessageGroupId |
| | ContentBasedDeduplication | true |
| | VisibilityTimeout | 60s |
| | MessageRetentionPeriod | 4 dias |
| | ReceiveMessageWaitTimeSeconds | 20s (long polling) |
| **Subscription vip** | FilterPolicy | `{"ticketTier":["VIP","CAMAROTE"]}` |
| | FilterPolicyScope | MessageBody |
| **Outras subs** | RawMessageDelivery | true |
| **MessageGroupId** | Valor | `showId` (cada show e fluxo FIFO independente) |
| **MessageDeduplicationId** | Valor | `reservationId` (UUID por reserva) |
| **ALB** | Path /api/reservations/* | Ingestor (8081) |
| | Path /api/notifications/* | Notification (8083, com stickiness) |
| **Log Groups** | Retenção | 7 dias |
| **Listener SQS (cada serviço)** | max-messages-per-poll | 10 |
| | poll-timeout | 20s |
| | max-concurrent-messages | 10 |
