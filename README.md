# pi-backend-queue

Java consumer side for the pi-env sensor pipeline. Consumes `sensor.reading`
events from RabbitMQ produced by the Django collector in `pi-env-backend`
(`feature/rabbitmq-publisher`).

## Layout

```
docker-compose.yml          # RabbitMQ broker (with management UI)
realtime-service/           # Spring Boot 3 / Java 21 consumer
  pom.xml
  src/main/java/com/piqueue/realtime/
    RealtimeServiceApplication.java
    config/        RabbitProperties, RabbitTopologyConfig
    consumer/      ReadingRealtimeListener, ReadingAlertListener
    event/         SensorReadingEvent
    state/         LatestReadingCache
    ws/            ReadingWebSocketHandler, WebSocketConfig, ReadingBroadcaster
  src/main/resources/application.yml
```

## RabbitMQ topology

Declared on startup by `RabbitTopologyConfig`:

- exchange `sensors` (topic, durable)
- exchange `sensors.dlx` (topic, durable)
- queue `q.realtime.reading` bound to `sensors` with `sensor.reading`
- queue `q.alert.reading` bound to `sensors` with `sensor.reading`,
  dead-lettered to `sensors.dlx` / `sensor.reading.dead`
- queue `q.alert.reading.dlq` bound to `sensors.dlx` with `sensor.reading.dead`

Matches the Python publisher's exchange/routing key in
`pi-env-backend/sensors/messaging.py`.

## Run locally

```bash
# 1. broker
docker compose up -d rabbitmq
# management UI: http://127.0.0.1:15672  (guest / guest)

# 2. consumer
cd realtime-service
mvn spring-boot:run

# 3. publisher (in pi-env-backend on feature/rabbitmq-publisher)
export RABBIT_ENABLED=true
export RABBIT_URL=amqp://guest:guest@127.0.0.1:5672/%2F
python manage.py collect_sensor_data --provider mock --interval 5
```

WebSocket endpoint: `ws://127.0.0.1:8080/ws/readings`. New connections receive a
snapshot from `LatestReadingCache` immediately, then live updates.

## Status

POC skeleton. What's done:

- Topology declared from code (idempotent on RabbitMQ restart)
- `q.realtime.reading` -> log + cache + WebSocket fan-out
- `q.alert.reading` -> log only (rules + Slack pending)
- Auto ack after successful listener execution + retry (3 attempts, exponential)
  wired via `application.yml`; exhausted attempts on the alert queue land in
  `q.alert.reading.dlq`

Pending (per plan section 14, steps 5-9):

- Cold-start bootstrap from Django `/latest`
- Threshold / stale / spike rules
- Slack notifier and `sensor.alert` event
- DLQ replay job

## Future services (not in this repo yet)

- `assistant-service` (Go): chatbot / LLM + vector DB. Splits at the natural
  service boundary in plan section 5. Talks to Django REST for history and to
  this service's REST for the latest cache.
