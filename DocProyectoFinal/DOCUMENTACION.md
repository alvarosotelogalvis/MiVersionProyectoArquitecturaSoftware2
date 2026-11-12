# DOCUMENTACIÓN TÉCNICA COMPLETA - FLASHTIX

> **Sistema de venta de entradas de alta concurrencia (500K usuarios)**
> **Versión:** 1.1.0 | **Fecha:** 2026-11-12 | **Rama:** `feat/phase-4-observability`

---

## 1. VISIÓN GENERAL DEL PROYECTO

### 1.1 Propósito y Alcance

**FlashTix** es una plataforma de venta de entradas diseñada para manejar **500,000 usuarios concurrentes** durante eventos de alta demanda (conciertos, festivales, eventos deportivos). El sistema resuelve los desafíos clásicos de la venta de entradas *online*: **sobreventa (overselling)**, **latencia bajo carga**, **equidad en el acceso** y **consistencia de datos** en un entorno distribuido.

### 1.2 Problemas que Resuelve

| Problema | Solución FlashTix |
|----------|-------------------|
| **Sobreventa de asientos** | Bloqueos atómicos en Redis con scripts Lua (RES-1 a RES-7) + verificación final en MySQL con PK única |
| **Thundering herd / colapso al abrir venta** | Sala de espera virtual (Waiting Room) con colas en Redis Sorted Sets + admisión por lotes computados (WR-1 a WR-6) |
| **Latencia en catálogo público** | Cache-aside en Redis (TTL 30s) + invalidación dirigida por claves (CAT-1 a CAT-4) |
| **Pagos lentos/bloqueantes** | Procesamiento asíncrono con RabbitMQ + Circuit Breaker + Reintentos exponenciales (PAY-1 a PAY-6) |
| **Consistencia distribuida** | Saga de checkout con outbox transaccional + consumidores idempotentes (ORD-1 a ORD-7) |
| **Observabilidad** | Métricas Prometheus, logs estructurados Loki, trazas OpenTelemetry, dashboards Grafana |

### 1.3 Arquitectura de Alto Nivel (C4 - Contexto)

Ver Imagenes C4

### 1.4 Decisiones Arquitectónicas Clave (ADRs)

| ADR | Título | Decisión | Estado |
|-----|--------|----------|--------|
| **001** | Dual Redis Instance | Separar **Redis Catalog** (cache + colas) de **Redis Hold Store** (holds atómicos) para aislar fallos y cargas | ✅ Aceptado |
| **002** | MySQL over PostgreSQL | Mantener MySQL 8 por experiencia del equipo, JSON support, y compatibilidad con tooling existente | ✅ Aceptado (rechazado PG) |
| **003** | Async Payments with RabbitMQ | Pagos fuera del request HTTP via RabbitMQ + Circuit Breaker + Outbox pattern | ✅ Aceptado |

---

## 2. ESTRUCTURA DEL REPOSITORIO

```
flashtix/
├── .claude/                          # Configuración Claude Code (agents, commands, memory)
├── .vscode/                          # Configuración VS Code (tasks, launch, extensions)
├── docs/
│   ├── adr/
│   │   ├── 001-dual-redis-instance.md
│   │   ├── 002-mysql-over-postgresql.md
│   │   └── 003-async-payments-rabbitmq.md
│   ├── spec.md                       # OpenSpec v1.1.0 - Contrato completo APIs + Eventos
│   ├── SPEC_CHECKLIST.md             # Checklist de alineación spec vs implementación
│   └── DOCUMENTACION.md              # Este archivo
├── infra/
│   ├── kong/
│   │   └── kong.yml                  # Declarative config Kong (routes, plugins, consumers)
│   └── observability/
│       ├── prometheus/
│       │   ├── prometheus.yml        # Scrape config + alerting rules
│       │   └── rules/
│       │       └── flashtix-alerts.yml
│       ├── grafana/
│       │   ├── dashboards/
│       │   │   ├── flashtix-overview.json
│       │   │   ├── flashtix-business.json
│       │   │   ├── flashtix-infrastructure.json
│       │   │   └── flashtix-slo.json
│       │   └── provisioning/
│       │       ├── dashboards.yml
│       │       └── datasources.yml
│       ├── loki/
│       │   └── loki.yml
│       ├── tempo/
│       │   └── tempo.yml
│       └── promtail/
│           └── promtail.yml
├── libs/
│   ├── shared/                       # Librería compartida (TypeScript/Node)
│   │   ├── src/
│   │   │   ├── correlation/          # Correlation ID propagation
│   │   │   ├── envelope.ts           # Event Envelope v1 (spec §5)
│   │   │   ├── publisher.ts          # EventPublisher (RabbitMQ, confirm channels)
│   │   │   ├── consumer.ts           # EventConsumer + MySqlProcessedEvents (idempotencia)
│   │   │   ├── topology.ts           # RabbitMQ topology declaration (exchanges, queues, DLQ)
│   │   │   ├── processed_events.ts   # Tabla processed_events + dedupKey
│   │   │   ├── errors.ts             # AppError + ProblemDetails (RFC 9457)
│   │   │   ├── admission-token.ts    # HMAC-SHA256 admission tokens (WR)
│   │   │   ├── ticket-qr.ts          # QR payload signing/verification (TKT)
│   │   │   └── metrics.ts            # Prometheus metrics helpers (Histogram, Counter, Gauge)
│   │   ├── package.json
│   │   └── tsconfig.json
│   └── nest-support/                 # Utilidades NestJS
│       ├── src/
│       │   ├── problem-details.filter.ts  # Exception filter → RFC 9457
│       │   ├── current-user.ts             # Extracción userId/email/roles de headers Kong
│       │   └── index.ts
│       ├── package.json
│       └── tsconfig.json
├── services/
│   ├── catalog/                      # Servicio Catálogo (puerto 3000)
│   │   ├── src/
│   │   │   ├── events/               # Eventos: CRUD admin + public read
│   │   │   ├── seats/                # Mapa de asientos + cache-aside Redis
│   │   │   ├── categories/           # Categorías de precio
│   │   │   ├── health/               # Health checks + readiness
│   │   │   ├── metrics/              # Prometheus metrics
│   │   │   └── main.ts
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── tsconfig.json
│   ├── waiting-room/                 # Servicio Waiting Room (puerto 3000)
│   │   ├── src/
│   │   │   ├── queue/                # Cola virtual (Redis Sorted Sets)
│   │   │   ├── admission/            # Admisión por lotes + HMAC tokens
│   │   │   ├── scheduler/            # Scheduler apertura venta (saleStartsAt)
│   │   │   ├── health/
│   │   │   └── main.ts
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── tsconfig.json
│   ├── reservation/                  # Servicio Reservas (puerto 3000 + gRPC 50051)
│   │   ├── src/
│   │   │   ├── holds/                # Holds REST + Lua scripts atómicos
│   │   │   ├── grpc/                 # LockHold gRPC service (RES-4)
│   │   │   ├── sweeper/              # Sweeper TTL expiración holds (RES-5)
│   │   │   ├── lua/                  # Scripts Lua: ACQUIRE, RELEASE, LOCK, CONSUME, SWEEP
│   │   │   ├── health/
│   │   │   └── main.ts
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── tsconfig.json
│   ├── order/                        # Servicio Pedidos (puerto 3000)
│   │   ├── src/
│   │   │   ├── orders/               # Saga checkout + outbox + idempotencia
│   │   │   ├── outbox/               # Transactional outbox poller
│   │   │   ├── sse/                  # Server-Sent Events para estado orden
│   │   │   ├── consumers/            # Consumers: payment.succeeded, payment.failed, hold.expired
│   │   │   ├── health/
│   │   │   └── main.ts
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── tsconfig.json
│   ├── payment-worker/               # Worker Pagos (background, sin puerto HTTP)
│   │   ├── src/
│   │   │   ├── gateway/              # Cliente gateway pago (timeout 20s, 3 retries)
│   │   │   ├── circuit-breaker/      # Circuit Breaker (50% errors/30s → OPEN)
│   │   │   ├── consumers/            # Consume payment.initiated
│   │   │   ├── payments/             # Persistencia pagos + idempotencia
│   │   │   ├── health/
│   │   │   └── main.ts
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── tsconfig.json
│   ├── ticket/                       # Servicio Tickets (puerto 3000)
│   │   ├── src/
│   │   │   ├── tickets/              # Emisión tickets + PDF + QR
│   │   │   ├── pdf/                  # PDFKit rendering (ticket-pdf.ts)
│   │   │   ├── storage/              # S3/MinIO storage (TicketStorage)
│   │   │   ├── clients/              # CatalogClient (getEvent para PDF)
│   │   │   ├── consumers/            # Consume order.confirmed
│   │   │   ├── health/
│   │   │   └── main.ts
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── tsconfig.json
│   └── notification/                 # Servicio Notificaciones (puerto 3000)
│       ├── src/
│       │   ├── consumers/            # Consume ticket.issued, order.failed, order.refunded
│       │   ├── email/                # EmailService (Mailpit/SMTP + adjuntos PDF)
│       │   ├── deduplication/        # processed_events table dedup
│       │   ├── templates/            # Plantillas HTML emails
│       │   ├── health/
│       │   └── main.ts
│       ├── Dockerfile
│       ├── package.json
│       └── tsconfig.json
├── scripts/
│   ├── smoke-phase2.mjs              # Pruebas humo Fase 2 (happy path, failure, double sale, sweeper)
│   ├── smoke-phase3.mjs              # Pruebas humo Fase 3 (tickets, emails, organizer console, p95)
│   ├── dev-setup.sh                  # Setup entorno desarrollo
│   └── seed.mjs                      # Datos semilla para testing
├── docker-compose.yml                # Orquestación completa (17 contenedores)
├── docker-compose.override.yml       # Overrides desarrollo local
├── .env.example                      # Variables de entorno template
├── package.json                      # Workspace root (npm workspaces)
├── tsconfig.base.json                # TypeScript base config
├── turbo.json                        # Turborepo config (build, test, lint)
└── README.md
```

### 2.2 Convenciones de Nombrado

| Elemento | Convención | Ejemplo |
|----------|------------|---------|
| **Servicios** | `kebab-case` | `waiting-room`, `payment-worker` |
| **Módulos NestJS** | `PascalCase` + `Module` | `EventsModule`, `HoldsModule` |
| **Clases** | `PascalCase` | `TicketsService`, `OrderConsumers` |
| **Interfaces/Types** | `PascalCase` | `OrderConfirmed`, `TicketView` |
| **Funciones/métodos** | `camelCase` | `issueOne()`, `getEvent()` |
| **Constantes** | `UPPER_SNAKE_CASE` | `QR_SECRET`, `CONSUMER` |
| **Variables entorno** | `UPPER_SNAKE_CASE` | `RABBITMQ_URL`, `TICKET_QR_SECRET` |
| **Tablas MySQL** | `snake_case` | `sold_seats`, `processed_events` |
| **Columnas MySQL** | `snake_case` | `order_id`, `seat_id`, `created_at` |
| **Eventos RabbitMQ** | `kebab-case` | `order.confirmed`, `payment.succeeded` |
| **Métricas Prometheus** | `snake_case` + sufijo `_total`/`_seconds` | `http_requests_total`, `hold_acquire_duration_seconds` |

---

## 3. CONFIGURACIÓN Y DESPLIEGUE

### 3.1 Requisitos Previos

| Herramienta | Versión Mínima | Propósito |
|-------------|----------------|-----------|
| **Docker** | 24.0+ | Contenedores |
| **Docker Compose** | 2.20+ | Orquestación |
| **Node.js** | 20.x LTS | Runtime servicios |
| **npm** | 10.x | Gestión dependencias |
| **Make** | 4.3+ | Automatización (opcional) |

> **Nota Windows**: Asegurar Docker Desktop en PATH. `jq` y `python` no requeridos (scripts usan `curl -w` y Node nativo).

### 3.2 Variables de Entorno Principales (`.env.example`)

```bash
# ──────────────────────────────────────────────
# CORE
# ──────────────────────────────────────────────
NODE_ENV=development
LOG_LEVEL=debug

# ──────────────────────────────────────────────
# KEYCLOAK (Identity Provider)
# ──────────────────────────────────────────────
KEYCLOAK_URL=http://localhost:8080
KEYCLOAK_REALM=flashtix
KEYCLOAK_CLIENT_ID=flashtix-web
KEYCLOAK_ADMIN_USER=admin
KEYCLOAK_ADMIN_PASSWORD=admin

# ──────────────────────────────────────────────
# RABBITMQ (Message Broker)
# ──────────────────────────────────────────────
RABBITMQ_URL=amqp://flashtix:flashtix@rabbitmq:5672
RABBITMQ_USER=flashtix
RABBITMQ_PASSWORD=flashtix

# ──────────────────────────────────────────────
# REDIS (Dual Instance - ADR-001)
# ──────────────────────────────────────────────
# Catalog: cache + waiting room queues
REDIS_CATALOG_URL=redis://redis-catalog:6379
# Hold Store: atomic holds + sweeper
REDIS_HOLD_URL=redis://redis-hold:6379

# ──────────────────────────────────────────────
# MYSQL (5 Databases - uno por servicio owner)
# ──────────────────────────────────────────────
MYSQL_CATALOG_URL=mysql://root:root@catalog-db:3306/catalog
MYSQL_BOOKING_URL=mysql://root:root@booking-db:3306/booking
MYSQL_PAYMENT_URL=mysql://root:root@payment-db:3306/payment
MYSQL_TICKET_URL=mysql://root:root@ticket-db:3306/ticket
MYSQL_NOTIFICATION_URL=mysql://root:root@notification-db:3306/notification
MYSQL_ROOT_PASSWORD=root

# ──────────────────────────────────────────────
# KONG API GATEWAY
# ──────────────────────────────────────────────
KONG_DECLARATIVE_CONFIG=/kong/kong.yml
KONG_PROXY_LISTEN=0.0.0.0:8000
KONG_ADMIN_LISTEN=0.0.0.0:8001

# ──────────────────────────────────────────────
# S3 / MINIO (Ticket PDFs)
# ──────────────────────────────────────────────
S3_ENDPOINT=http://minio:9000
S3_REGION=us-east-1
S3_ACCESS_KEY=flashtix
S3_SECRET_KEY=flashtix123
S3_BUCKET=tickets
S3_PUBLIC_URL=http://localhost:9000

# ──────────────────────────────────────────────
# TICKET SERVICE
# ──────────────────────────────────────────────
TICKET_QR_SECRET=super-secret-qr-signing-key-change-in-prod
TICKET_CONCURRENCY=10

# ──────────────────────────────────────────────
# PAYMENT WORKER
# ──────────────────────────────────────────────
PAYMENT_GATEWAY_URL=http://mock-gateway:8090
PAYMENT_GATEWAY_TIMEOUT_MS=20000
PAYMENT_MAX_RETRIES=3
PAYMENT_RETRY_BASE_DELAY_MS=2000
PAYMENT_CIRCUIT_BREAKER_THRESHOLD=0.5
PAYMENT_CIRCUIT_BREAKER_WINDOW_MS=30000

# ──────────────────────────────────────────────
# NOTIFICATION SERVICE
# ──────────────────────────────────────────────
SMTP_HOST=mailpit
SMTP_PORT=1025
SMTP_USER=
SMTP_PASSWORD=
EMAIL_FROM=noreply@flashtix.test

# ──────────────────────────────────────────────
# OBSERVABILITY
# ──────────────────────────────────────────────
OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318/v1/traces
OTEL_SERVICE_NAME=flashtix
PROMETHEUS_METRICS_PORT=9464
```

### 3.3 Puertos Expuestos (Host ↔ Container)

| Servicio | Puerto Host | Puerto Container | Protocolo | Descripción |
|----------|-------------|------------------|-----------|-------------|
| **API Gateway (Kong)** | `8000` | `8000` | HTTP/HTTPS | Punto de entrada único |
| **Kong Admin API** | `8001` | `8001` | HTTP | Configuración declarativa |
| **Keycloak** | `8080` | `8080` | HTTP | Identity Provider |
| **Mock Payment Gateway** | `8090` | `8090` | HTTP | Simulador pasarela pagos |
| **Mailpit (Email UI)** | `8025` | `8025` | HTTP | Interfaz emails + API |
| **Grafana** | `3000` | `3000` | HTTP | Dashboards |
| **Prometheus** | `9090` | `9090` | HTTP | Métricas + Alertas |
| **Loki** | `3100` | `3100` | HTTP | Logs |
| **Tempo** | `3200` | `3200` | HTTP | Trazas |
| **MinIO Console** | `9001` | `9001` | HTTP | UI S3 |
| **MinIO API** | `9000` | `9000` | HTTP | S3 API |
| **Catalog Service** | `3001` | `3000` | HTTP | API Catálogo |
| **Waiting Room** | `3002` | `3000` | HTTP | API Waiting Room |
| **Reservation** | `3003` | `3000` | HTTP | API Reservas |
| **Reservation gRPC** | `50051` | `50051` | gRPC | LockHold |
| **Order Service** | `3004` | `3000` | HTTP | API Pedidos |
| **Payment Worker** | — | — | — | Background worker |
| **Ticket Service** | `3005` | `3000` | HTTP | API Tickets |
| **Notification** | `3006` | `3000` | HTTP | API Notificaciones |
| **MySQL Catalog** | `3306` | `3306` | MySQL | DB Catálogo |
| **MySQL Booking** | `3307` | `3306` | MySQL | DB Reservas/Pedidos |
| **MySQL Payment** | `3308` | `3306` | MySQL | DB Pagos |
| **MySQL Ticket** | `3309` | `3306` | MySQL | DB Tickets |
| **MySQL Notification** | `3310` | `3306` | MySQL | DB Notificaciones |
| **Redis Catalog** | `6379` | `6379` | Redis | Cache + Colas WR |
| **Redis Hold Store** | `6380` | `6379` | Redis | Holds atómicos |
| **RabbitMQ** | `5672` | `5672` | AMQP | Message Broker |
| **RabbitMQ Management** | `15672` | `15672` | HTTP | UI Management |

### 3.4 Comandos de Despliegue

```bash
# 1. Levantar toda la infraestructura
docker compose up -d --build

# 2. Verificar salud de todos los servicios
docker compose ps

# 3. Ver logs de un servicio específico
docker compose logs -f catalog

# 4. Ejecutar smoke tests Fase 2 (happy path, failure, double sale)
node scripts/smoke-phase2.mjs

# 5. Ejecutar smoke tests Fase 3 (tickets, emails, organizer, p95)
node scripts/smoke-phase3.mjs

# 6. Recargar configuración Kong (tras cambios en kong.yml)
docker compose restart api-gateway

# 7. Apagar todo (preservando volúmenes)
docker compose down

# 8. Apagar todo + eliminar volúmenes (reset completo)
docker compose down -v
```

### 3.5 Health Checks y Readiness

Todos los servicios exponen:
- **`GET /health/live`** — Liveness probe (proceso vivo)
- **`GET /health/ready`** — Readiness probe (dependencias OK: DB, Redis, RabbitMQ)

Kong y Docker Compose usan `/health/ready` para routing y dependencias.

---

## 4. SERVICIOS Y PUERTOS

### 4.1 Catálogo (`catalog`) — Puerto 3000/3001

**Responsabilidad**: Gestión de eventos, asientos, categorías de precio. Lecturas públicas cacheadas, escrituras admin.

| Endpoint | Método | Auth | Descripción | Spec Ref |
|----------|--------|------|-------------|----------|
| `/v1/events` | GET | Público | Lista eventos publicados (paginado) | CAT-1 |
| `/v1/events/{eventId}` | GET | Público | Detalle evento público | CAT-2 |
| `/v1/events/{eventId}/seats` | GET | Público | Mapa asientos con disponibilidad (cache 30s) | CAT-3 |
| `/v1/admin/events` | POST | ORGANIZER | Crear evento DRAFT | ADM-1 |
| `/v1/admin/events/{eventId}/publish` | POST | ORGANIZER | Publicar evento (DRAFT → PUBLISHED) | ADM-2 |
| `/v1/admin/events/{eventId}` | PATCH | ORGANIZER | Actualizar evento (solo DRAFT) | — |
| `/v1/admin/events/{eventId}` | DELETE | ORGANIZER | Eliminar evento (solo DRAFT) | — |
| `/v1/admin/events/{eventId}/seats` | POST | ORGANIZER | Añadir asientos a evento | — |
| `/v1/admin/categories` | POST | ORGANIZER | Crear categoría precio | — |

**Patrón Cache-Aside (CAT-4)**:
```
GET /seats → Redis GET "seats:{eventId}"
  ├─ HIT → Return cached (30s TTL)
  └─ MISS → MySQL query → Redis SETEX 30s → Return
```
Invalidación: `DEL seats:{eventId}` en cambios de asientos (admin, hold acquire/release, sweeper).

**Métricas clave**: `catalog_seats_cache_hit_total`, `catalog_seats_cache_miss_total`, `http_request_duration_seconds{handler="/v1/events/{eventId}/seats"}`.

### 4.2 Waiting Room (`waiting-room`) — Puerto 3000/3002

**Responsabilidad**: Cola virtual justa, admisión por lotes, tokens de admisión HMAC.

| Endpoint | Método | Auth | Descripción | Spec Ref |
|----------|--------|------|-------------|----------|
| `/v1/events/{eventId}/queue` | POST | FAN | Unirse a cola (idempotente por usuario) | WR-2 |
| `/v1/events/{eventId}/queue/{ticketId}` | GET | FAN | Consultar turno (posición, status, admissionToken) | WR-3 |
| `/v1/events/{eventId}/queue/{ticketId}` | DELETE | FAN | Abandonar cola | WR-4 |

**Algoritmo Cola (WR-1, WR-5)**:
- Redis Sorted Set: `queue:{eventId}` → score = timestamp join, member = `{userId}:{queueTicketId}`
- Admisión por lotes: Scheduler cada 5s computa `batchSize = min(availableSeats * 1.2, maxBatch)`
- `ZRANGE queue:{eventId} 0 batchSize-1` → emite `admission.granted` a RabbitMQ + `ZREM`
- Token HMAC: `sign({queueTicketId, eventId, userId, admittedAt, exp}, WR_ADMISSION_SECRET)`

**Métricas clave**: `waiting_room_queue_size{eventId}`, `waiting_room_admitted_total{eventId}`, `waiting_room_admission_batch_duration_seconds`.

### 4.3 Reservas (`reservation`) — Puerto 3000/3003 + gRPC 50051

**Responsabilidad**: Holds atómicos con TTL (10 min), scripts Lua, gRPC LockHold para checkout.

| Endpoint | Método | Auth | Descripción | Spec Ref |
|----------|--------|------|-------------|----------|
| `/v1/holds` | POST | FAN + Admission Token | Crear hold (asientos → HELD) | RES-1 |
| `/v1/holds/{holdId}` | DELETE | FAN | Liberar hold (→ AVAILABLE) | RES-2 |
| `/v1/holds/{holdId}` | GET | FAN | Consultar hold | RES-3 |
| gRPC `LockHold` | — | Interno (Order) | Bloqueo definitivo (HELD → LOCKED) | RES-4 |

**Scripts Lua Atómicos (RES-1, RES-6, RES-7)** — Todos en `services/reservation/src/lua/`:

| Script | Clave Redis | Operación | Retorna |
|--------|-------------|-----------|---------|
| `ACQUIRE` | `hold:{holdId}`, `seat:{eventId}:{seatId}` | `SETNX` hold + `SETNX` seat → HELD | `{ok: true}` o `{ok: false, reason}` |
| `RELEASE` | `hold:{holdId}`, `seat:{eventId}:{seatId}` | `DEL` hold + `DEL` seat → AVAILABLE | `{released: N}` |
| `LOCK` | `hold:{holdId}` | `GET` hold → verifica owner → `SET` LOCKED | `{locked: true}` |
| `CONSUME` | `hold:{holdId}`, `seat:{eventId}:{seatId}` | `GET` hold LOCKED → `DEL` hold + `SET` seat SOLD | `{consumed: true}` |
| `SWEEP` | `holds:by-expiry` (ZSET score=expiry) | `ZRANGEBYSCORE` expirados → `RELEASE` cada uno | `{swept: N}` |

**Sweeper (RES-5)**: Job cada 30s → `EVALSHA SWEEP` → publica `hold.expired` a RabbitMQ por cada hold barrido.

**Métricas clave**: `reservation_hold_acquire_duration_seconds`, `reservation_hold_acquire_total{result="ok|conflict|expired"}`, `reservation_sweeper_swept_total`.

### 4.4 Pedidos (`order`) — Puerto 3000/3004

**Responsabilidad**: Saga checkout, idempotencia, outbox transaccional, SSE estado tiempo real.

| Endpoint | Método | Auth | Descripción | Spec Ref |
|----------|--------|------|-------------|----------|
| `/v1/orders` | POST | FAN + Idempotency-Key | Iniciar checkout (→ PENDING) | ORD-3 |
| `/v1/orders/{orderId}` | GET | FAN (owner) | Detalle orden | ORD-4 |
| `/v1/orders/{orderId}/stream` | GET | FAN (owner) | SSE: estado orden tiempo real | ORD-5 |
| `/v1/admin/orders/{orderId}/refund` | POST | ORGANIZER | Reembolsar orden CONFIRMED | ADM-3 |

**Flujo Saga (ORD-1 a ORD-7)**:

```
POST /orders (idempotencyKey)
  │
  ├─ 1. Validar hold existe + owner + status=HELD
  ├─ 2. Calcular total (precios catálogo)
  ├─ 3. INSERT orders (PENDING) + outbox (payment.initiated)
  ├─ 4. gRPC LockHold(holdId) → HELD → LOCKED
  ├─ 5. RETURN 202 PENDING + Idempotent-Replayed header
  │
  ├─ Outbox Poller (cada 500ms)
  │   └─ PUBLICA payment.initiated → RabbitMQ
  │
  ├─ Payment Worker procesa
  │   ├─ SUCCESS → payment.succeeded
  │   │   └─ Order Consumer: CONFIRMED + outbox(order.confirmed)
  │   │       └─ Ticket Service → ticket.issued
  │   │       └─ Notification → email
  │   └─ FAILURE → payment.failed
  │       └─ Order Consumer: FAILED + RELEASE hold + outbox(order.failed)
  │           └─ Notification → email
  │
  └─ Double Sale Protection (ORD-5, ORD-7)
      └─ UNIQUE (event_id, seat_id) en sold_seats
          └─ Conflicto → CANCELLED + REFUND + outbox(order.cancelled)
```

**Idempotencia (ORD-3)**: `orderId = SHA256(userId + ':' + idempotencyKey)` → `UNIQUE (order_id)` + `request_hash` para detectar body cambiado.

**Métricas clave**: `order_checkout_duration_seconds`, `order_status_transitions_total{from,to}`, `order_sse_connections_active`.

### 4.5 Pagos Worker (`payment-worker`) — Background (sin puerto HTTP)

**Responsabilidad**: Procesamiento asíncrono de pagos, circuit breaker, reintentos, mock gateway.

| Componente | Configuración | Spec Ref |
|------------|---------------|----------|
| **Timeout HTTP** | 20 segundos | PAY-2 |
| **Reintentos** | 3 (backoff: 2s, 10s, 30s) | PAY-3 |
| **Circuit Breaker** | 50% errores en 30s → OPEN 60s | PAY-4 |
| **Idempotencia** | `payment_idempotency_key` UNIQUE en `payment.payments` | PAY-5 |
| **Mock Gateway** | `/v1/charge` + config `/mock/config` | PAY-6 |

**Estados Pago**: `INITIATED` → `PROCESSING` → `SUCCEEDED` | `FAILED` | `REFUNDED`

**Métricas clave**: `payment_gateway_request_duration_seconds`, `payment_circuit_breaker_state{gauge}`, `payment_retries_total`, `payment_outcome_total{result="succeeded|failed|refunded"}`.

### 4.6 Tickets (`ticket`) — Puerto 3000/3005

**Responsabilidad**: Emisión tickets (1 por asiento), PDF + QR firmado HMAC, almacenamiento S3, URLs firmadas.

| Endpoint | Método | Auth | Descripción | Spec Ref |
|----------|--------|------|-------------|----------|
| `/v1/tickets/{ticketId}` | GET | FAN (holder) | Detalle ticket + downloadUrl firmada (15 min) | TKT-4 |

**Flujo Emisión (TKT-2, TKT-3)**:
```
Consumer order.confirmed
  │
  ├─ Para cada seat en order.seats
  │   ├─ ticketId = UUIDv4
  │   ├─ qrPayload = signTicketQr({ticketId, eventId, seatId}, TICKET_QR_SECRET)
  │   │   → "FTX1.{ticketId}.{base64url(HMAC-SHA256)}"
  │   ├─ renderTicketPdf({ticketId, orderId, qrPayload, seatId, category, event})
  │   ├─ S3 PUT `tickets/{eventId}/{orderId}/{ticketId}.pdf`
  │   ├─ INSERT tickets (UNIQUE order_id, seat_id)
  │   │   └─ Si ER_DUP_ENTRY → DELETE PDF S3 → reusar existente
  │   └─ PUBLICA ticket.issued (aggregateId=orderId, dedupKey)
  │
  └─ Idempotente por UNIQUE (order_id, seat_id) + dedupKey en notification
```

**QR Payload**: `FTX1.{ticketId}.{signature}` — Verificación en entrada: `verifyTicketQr(payload, secret)`.

**Métricas clave**: `ticket_issue_duration_seconds`, `ticket_pdf_render_duration_seconds`, `ticket_s3_upload_duration_seconds`, `ticket_issued_total`.

### 4.7 Notificaciones (`notification`) — Puerto 3000/3006

**Responsabilidad**: Email transaccional, deduplicación via `processed_events`, adjuntos PDF.

| Evento Consumido | Acción | Dedup Key | Spec Ref |
|------------------|--------|-----------|----------|
| `ticket.issued` | Email confirmación + PDFs adjuntos | `ticket.issued:{orderId}` | NOT-2 |
| `order.failed` | Email fallo pago | `order.failed:{orderId}` | NOT-3 |
| `order.refunded` | Email reembolso | `order.refunded:{orderId}` | NOT-4 |

**Deduplicación**: Tabla `processed_events(event_id, consumer, dedup_key, created_at)` + `INSERT IGNORE` → solo procesa si no existe.

**Email**: Nodemailer + SMTP (Mailpit dev, SMTP prod). Plantillas HTML en `src/templates/`.

**Métricas clave**: `notification_email_sent_total{type}`, `notification_deduplicated_total`, `notification_email_duration_seconds`.

---

## 5. CLASES, MÓDULOS Y FUNCIONES PRINCIPALES

### 5.1 Librería Compartida (`libs/shared`)

#### `envelope.ts` — Event Envelope v1
```typescript
interface EventEnvelope<T = unknown> {
  eventId: string;           // UUIDv4 único por evento
  eventType: string;         // "order.confirmed", "ticket.issued", etc.
  eventVersion: number;      // 1
  occurredAt: string;        // ISO 8601
  producer: string;          // "order-service", "ticket-service"
  correlationId?: string;    // X-Correlation-Id propagado
  aggregateId: string;       // orderId, holdId, eventId
  aggregateVersion: number;  // Optimistic locking
  data: T;                   // Payload tipado por eventType
}
```

#### `publisher.ts` — EventPublisher
```typescript
class EventPublisher {
  // Publica con confirm channel (wait for broker ack)
  async publish(envelope: EventEnvelope): Promise<void>
  
  // Declara topología (exchanges, queues, bindings, DLQ)
  static async declareTopology(channel: Channel): Promise<void>
}
```
**Topología RabbitMQ** (`topology.ts`):
- Exchanges: `catalog`, `waiting-room`, `reservation`, `order`, `payment`, `ticket`, `notification` (topic, durable)
- Queues: `{consumer}.{eventType}` (ej: `ticket.order.confirmed`)
- DLQ: `{queue}.dlq` + TTL 7 días + max retries 3
- Bindings: `exchange → queue` con routing key `eventType`

#### `consumer.ts` — EventConsumer + MySqlProcessedEvents
```typescript
class EventConsumer<T> {
  static async start(options: {
    url: string;
    queue: string;
    consumer: string;           // "ticket", "notification", etc.
    processedEvents: MySqlProcessedEvents;
    prefetch: number;
    handler: (envelope: EventEnvelope<T>) => Promise<void>;
    onDeadLetter: (envelope, error) => void;
  }): Promise<EventConsumer>
  
  async close(): Promise<void>
}

class MySqlProcessedEvents {
  // INSERT IGNORE INTO processed_events (event_id, consumer, dedup_key) VALUES (?, ?, ?)
  // Retorna true si se insertó (nuevo), false si ya existía (duplicado)
  async tryClaim(eventId: string, consumer: string, dedupKey: string): Promise<boolean>
}
```

#### `admission-token.ts` — HMAC Admission Tokens (WR)
```typescript
function signAdmissionToken(payload: AdmissionPayload, secret: string): string
function verifyAdmissionToken(token: string, secret: string): AdmissionPayload | null

interface AdmissionPayload {
  queueTicketId: string;
  eventId: string;
  userId: string;
  admittedAt: string;    // ISO 8601
  exp: number;           // Unix timestamp (admittedAt + 10 min)
}
```

#### `ticket-qr.ts` — QR Payload Signing (TKT)
```typescript
function signTicketQr(payload: QrPayload, secret: string): string
// Retorna: "FTX1.{ticketId}.{base64url(HMAC-SHA256(ticketId+eventId+seatId))}"

function verifyTicketQr(qrPayload: string, secret: string): QrPayload | null

interface QrPayload {
  ticketId: string;
  eventId: string;
  seatId: string;
}
```

#### `metrics.ts` — Prometheus Helpers
```typescript
// Histogram con buckets por defecto para latencias HTTP/DB/RPC
function createHttpHistogram(name: string, help: string, buckets?: number[]): Histogram

// Counter genérico
function createCounter(name: string, help: string, labelNames?: string[]): Counter

// Gauge para valores instantáneos
function createGauge(name: string, help: string, labelNames?: string[]): Gauge

// Middleware Express/NestJS para instrumentar automáticamente
function prometheusMiddleware(options?: { defaultLabels?: Record<string, string> })
```

#### `errors.ts` — AppError + ProblemDetails (RFC 9457)
```typescript
class AppError extends Error {
  constructor(
    public readonly code: string,           // "VALIDATION_ERROR", "SEAT_CONFLICT"
    public readonly message: string,
    public readonly statusCode: number = 400,
    public readonly details?: Record<string, unknown>
  )
}

// ProblemDetails response (RFC 9457)
interface ProblemDetails {
  type: string;           // "https://api.flashtix.com/errors/{code}"
  title: string;          // Human-readable
  status: number;         // HTTP status
  code: string;           // Machine-readable code
  detail: string;         // Specific description
  instance?: string;      // Request path
  correlationId?: string; // X-Correlation-Id
}
```

#### `correlation.ts` — Correlation ID Propagation
```typescript
// Header: X-Correlation-Id (UUIDv4)
// Generado por Kong (correlation-id plugin) o servicio origen
function getCorrelationId(): string | undefined
function setCorrelationId(id: string): void
function runWithCorrelationId<T>(id: string, fn: () => T): T
```

### 5.2 NestJS Support (`libs/nest-support`)

#### `problem-details.filter.ts` — Exception Filter
```typescript
@Catch()
export class ProblemDetailsFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void
  // Convierte AppError → ProblemDetails (RFC 9457)
  // HttpException → ProblemDetails
  // Error genérico → 500 INTERNAL_ERROR
}
```

#### `current-user.ts` — Headers Kong (GW-4)
```typescript
export function currentUserId(request: Request): string
export function currentUserEmail(request: Request): string
export function currentUserRoles(request: Request): string[]
// Lee: X-User-Id, X-User-Email, X-User-Roles (inyectados por Kong post-function)
```

### 5.3 Servicios — Clases Principales

#### `services/catalog/src/seats/seats.service.ts`
```typescript
@Injectable()
export class SeatsService {
  // Cache-aside: Redis GET → MySQL fallback → Redis SETEX 30s
  async getSeatMap(eventId: string): Promise<SeatMapDto>
  
  // Invalidación dirigida
  async invalidateCache(eventId: string): Promise<void>
  
  // Overlay disponibilidad: combina MySQL (SOLD) + Redis (HELD/LOCKED)
  private applyAvailabilityOverlay(seats: SeatRow[], eventId: string): SeatMapDto
}
```

#### `services/waiting-room/src/queue/queue.service.ts`
```typescript
@Injectable()
export class QueueService {
  // Unirse a cola (idempotente por userId)
  async join(eventId: string, userId: string): Promise<JoinResponse>
  
  // Consultar turno
  async getTurn(eventId: string, queueTicketId: string, userId: string): Promise<TurnResponse>
  
  // Abandonar cola
  async leave(eventId: string, queueTicketId: string, userId: string): Promise<void>
}
```

#### `services/waiting-room/src/admission/admission.service.ts`
```typescript
@Injectable()
export class AdmissionService {
  // Computa lote admisión y emite admission.granted
  async computeAndAdmitBatch(eventId: string): Promise<AdmissionBatchResult>
  
  // Firma token HMAC
  private signAdmissionToken(payload: AdmissionPayload): string
}
```

#### `services/reservation/src/holds/holds.service.ts`
```typescript
@Injectable()
export class HoldsService {
  // ACQUIRE via Lua script (atómico)
  async acquire(holdId: string, eventId: string, seatIds: string[], userId: string, ttlMs: number): Promise<AcquireResult>
  
  // RELEASE via Lua script
  async release(holdId: string, eventId: string, userId: string): Promise<ReleaseResult>
  
  // CONSUME via Lua script (checkout success)
  async consume(holdId: string, eventId: string): Promise<ConsumeResult>
  
  // Consulta estado
  async get(holdId: string): Promise<HoldView | null>
}
```

#### `services/reservation/src/grpc/lock-hold.service.ts`
```typescript
@GrpcService('LockHold')
export class LockHoldGrpcService {
  // LOCK hold para checkout (HELD → LOCKED)
  @GrpcMethod('LockHoldService', 'LockHold')
  async lockHold(request: LockHoldRequest): Promise<LockHoldResponse>
}
```

#### `services/order/src/orders/orders.service.ts`
```typescript
@Injectable()
export class OrdersService {
  // Checkout saga entry point
  async checkout(userId: string, dto: CheckoutDto, idempotencyKey: string): Promise<CheckoutResponse>
  
  // Idempotency key → orderId determinista
  private computeOrderId(userId: string, idempotencyKey: string): string
  
  // Outbox: INSERT orders + outbox en misma transacción
  private async persistOrderAndOutbox(...): Promise<OrderEntity>
  
  // Consumers: payment.succeeded, payment.failed, hold.expired
  async handlePaymentSucceeded(event: PaymentSucceeded): Promise<void>
  async handlePaymentFailed(event: PaymentFailed): Promise<void>
  async handleHoldExpired(event: HoldExpired): Promise<void>
}
```

#### `services/order/src/sse/sse.controller.ts`
```typescript
@Controller('orders')
export class OrdersController {
  @Get(':orderId/stream')
  @Sse()
  async stream(@Param('orderId') orderId: string, @Req() req: Request): Promise<Observable<MessageEvent>> {
    // EventSource: push status changes (PENDING → CONFIRMED/FAILED/CANCELLED)
  }
}
```

#### `services/payment-worker/src/gateway/payment-gateway.client.ts`
```typescript
@Injectable()
export class PaymentGatewayClient {
  // POST /v1/charge con timeout 20s + 3 retries (2s, 10s, 30s)
  async charge(request: ChargeRequest): Promise<ChargeResponse>
  
  // Circuit Breaker: 50% errors/30s → OPEN 60s
  private circuitBreaker: CircuitBreaker
}
```

#### `services/ticket/src/tickets/tickets.service.ts`
```typescript
@Injectable()
export class TicketsService {
  // Emisión tickets (idempotente por UNIQUE order_id, seat_id)
  async issue(order: OrderConfirmed, correlationId?: string): Promise<void>
  
  // Consulta ticket (solo holder)
  async get(ticketId: string, userId: string): Promise<TicketDetail>
  
  // QR signing
  private qrSecret = process.env.TICKET_QR_SECRET ?? ''
}
```

#### `services/ticket/src/pdf/ticket-pdf.ts`
```typescript
export async function renderTicketPdf(params: {
  ticketId: string;
  orderId: string;
  qrPayload: string;
  seatId: string;
  category: string;
  event: EventDetails;
}): Promise<Buffer> {
  // PDFKit: header, QR code (QRCode.toBuffer), seat info, event details, footer
}
```

#### `services/notification/src/consumers/notification.consumers.ts`
```typescript
@Injectable()
export class NotificationConsumers implements OnModuleInit {
  // ticket.issued → email con PDFs adjuntos
  async handleTicketIssued(event: TicketIssued): Promise<void>
  
  // order.failed → email fallo
  async handleOrderFailed(event: OrderFailed): Promise<void>
  
  // order.refunded → email reembolso
  async handleOrderRefunded(event: OrderRefunded): Promise<void>
  
  // Deduplicación via MySqlProcessedEvents
  private processedEvents: MySqlProcessedEvents
}
```

---

## 6. FLUJOS DE DATOS Y ALGORITMOS

### 6.1 Flujo Completo: Compra Exitosa (Happy Path)

```mermaid
sequenceDiagram
    actor Fan
    participant GW as Kong Gateway
    participant WR as Waiting Room
    participant CAT as Catalog
    participant RES as Reservation
    participant ORD as Order
    participant PAY as Payment Worker
    participant TKT as Ticket
    participant NOT as Notification
    participant RB as RabbitMQ
    participant DB as MySQL
    participant RD as Redis

    Note over Fan,GW: 1. AUTENTICACIÓN (Keycloak JWT)
    Fan->>GW: GET /v1/events/{eventId}/seats (Bearer token)
    GW->>CAT: Forward + X-User-Id headers
    CAT->>RD: GET seats:{eventId} (cache 30s)
    alt Cache HIT
        RD-->>CAT: Seat map
    else Cache MISS
        CAT->>DB: SELECT seats + categories
        DB-->>CAT: Rows
        CAT->>RD: SETEX seats:{eventId} 30s
    end
    CAT-->>GW: Seat map JSON
    GW-->>Fan: 200 OK + seats

    Note over Fan,GW: 2. UNIRSE A COLA (Waiting Room)
    Fan->>GW: POST /v1/events/{eventId}/queue
    GW->>WR: Forward
    WR->>RD: ZADD queue:{eventId} {ts} {userId}:{ticketId}
    WR-->>GW: 202 {queueTicketId}
    GW-->>Fan: 202 {queueTicketId}

    Note over Fan,WR: 3. ADMISIÓN (Scheduler cada 5s)
    loop Cada 5s (WR Scheduler)
        WR->>RD: ZRANGE queue:{eventId} 0 batchSize
        WR->>RB: PUBLIC admission.granted {queueTicketId, userId, eventId}
        WR->>RD: ZREM admitted users
    end
    Fan->>GW: GET /v1/events/{eventId}/queue/{ticketId}
    GW->>WR: Forward
    WR-->>GW: {status: ADMITTED, admissionToken: HMAC(...)}
    GW-->>Fan: 200 + admissionToken

    Note over Fan,GW: 4. CREAR HOLD (Reservation)
    Fan->>GW: POST /v1/holds {eventId, seatIds} + X-Admission-Token
    GW->>RES: Forward + headers
    RES->>RD: EVALSHA ACQUIRE(holdId, seats, userId, TTL=600s)
    alt Éxito
        RD-->>RES: {ok: true}
        RES->>DB: INSERT holds (HELD)
        RES-->>GW: 201 {holdId, expiresAt}
    else Conflicto
        RD-->>RES: {ok: false, reason: "SEAT_TAKEN"}
        RES-->>GW: 409 SEAT_CONFLICT
    end
    GW-->>Fan: 201/409

    Note over Fan,GW: 5. CHECKOUT (Order Saga)
    Fan->>GW: POST /v1/orders {holdId, paymentMethodToken} + Idempotency-Key
    GW->>ORD: Forward
    ORD->>DB: BEGIN TRANSACTION
    ORD->>DB: INSERT orders (PENDING, orderId=SHA256(user+idemKey))
    ORD->>DB: INSERT outbox (payment.initiated)
    ORD->>DB: COMMIT
    ORD->>RES: gRPC LockHold(holdId) → HELD→LOCKED
    ORD-->>GW: 202 PENDING + Idempotent-Replayed header
    GW-->>Fan: 202 {orderId}

    Note over ORD,RB: 6. OUTBOX POLLER (cada 500ms)
    ORD->>DB: SELECT * FROM outbox WHERE published=false LIMIT 100
    ORD->>RB: PUBLIC payment.initiated (confirm channel)
    ORD->>DB: UPDATE outbox SET published=true

    Note over PAY,RB: 7. PAYMENT WORKER (Async)
    PAY->>RB: CONSUME payment.initiated
    PAY->>PAY: Circuit Breaker check
    PAY->>Mock Gateway: POST /v1/charge (timeout 20s, 3 retries)
    alt SUCCESS
        PAY->>DB: INSERT payments (SUCCEEDED)
        PAY->>RB: PUBLIC payment.succeeded {orderId, paymentId}
    else FAILURE (decline/error/timeout)
        PAY->>DB: INSERT payments (FAILED)
        PAY->>RB: PUBLIC payment.failed {orderId, reason}
    end

    Note over ORD,RB: 8. ORDER CONSUMERS
    ORD->>RB: CONSUME payment.succeeded
    ORD->>DB: UPDATE orders SET status=CONFIRMED
    ORD->>RES: gRPC ConsumeHold(holdId) → LOCKED→CONSUMED (SOLD)
    ORD->>DB: INSERT sold_seats (eventId, seatId, orderId) -- UNIQUE PK
    ORD->>DB: INSERT outbox (order.confirmed)
    ORD->>RB: PUBLIC order.confirmed

    Note over TKT,RB: 9. TICKET SERVICE
    TKT->>RB: CONSUME order.confirmed
    TKT->>CAT: GET /v1/events/{eventId} (para PDF)
    loop Por cada seat
        TKT->>TKT: ticketId=UUID, qrPayload=signTicketQr(...)
        TKT->>TKT: renderTicketPdf(...)
        TKT->>S3: PUT tickets/{eventId}/{orderId}/{ticketId}.pdf
        TKT->>DB: INSERT tickets (UNIQUE order_id, seat_id)
    end
    TKT->>RB: PUBLIC ticket.issued {orderId, tickets[]}

    Note over NOT,RB: 10. NOTIFICATION SERVICE
    NOT->>RB: CONSUME ticket.issued
    NOT->>DB: INSERT IGNORE processed_events (dedupKey)
    NOT->>S3: GET signed URLs para PDFs
    NOT->>SMTP: SEND email + PDF attachments
```

### 6.2 Flujo: Pago Fallido → Liberación Asientos

```mermaid
sequenceDiagram
    participant PAY as Payment Worker
    participant RB as RabbitMQ
    participant ORD as Order
    participant RES as Reservation
    participant RD as Redis
    participant DB as MySQL
    participant NOT as Notification

    PAY->>RB: PUBLIC payment.failed {orderId, reason: PAYMENT_DECLINED}
    ORD->>RB: CONSUME payment.failed
    ORD->>DB: UPDATE orders SET status=FAILED, failureReason=PAYMENT_DECLINED
    ORD->>RES: gRPC ReleaseHold(holdId) -- o llama DELETE /v1/holds/{holdId}
    RES->>RD: EVALSHA RELEASE(holdId, seats) → AVAILABLE
    RES->>DB: UPDATE holds SET status=RELEASED
    ORD->>DB: INSERT outbox (order.failed)
    ORD->>RB: PUBLIC order.failed
    NOT->>RB: CONSUME order.failed
    NOT->>SMTP: SEND email "No pudimos completar tu compra"
```

### 6.3 Flujo: Doble Venta (Race Condition) → Reembolso Automático

```mermaid
sequenceDiagram
    participant ORD1 as Order (Fan A)
    participant ORD2 as Order (Fan B / Ghost)
    participant DB as MySQL (booking.sold_seats)
    participant PAY as Payment Worker
    participant RB as RabbitMQ
    participant RES as Reservation

    Note over ORD1,PAY: Fan A: Checkout lento (mock latency 4s)
    ORD1->>RES: gRPC LockHold → LOCKED
    ORD1->>PAY: payment.initiated (slow)
    
    Note over ORD2,DB: Fan B / Ghost: Asiento vendido "por detrás"
    ORD2->>DB: INSERT orders (CONFIRMED) -- ghost order
    ORD2->>DB: INSERT sold_seats (eventId, seatId, ghostOrderId) -- PK UNIQUE!
    
    Note over PAY,ORD1: Pago Fan A termina
    PAY->>RB: PUBLIC payment.succeeded (Fan A)
    ORD1->>RB: CONSUME payment.succeeded
    ORD1->>DB: INSERT sold_seats (eventId, seatId, orderId_A)
    alt PK Conflict (ORD-5, ORD-7)
        DB-->>ORD1: ER_DUP_ENTRY
        ORD1->>DB: UPDATE orders SET status=CANCELLED, failureReason=SEAT_CONFLICT
        ORD1->>PAY: REFUND payment (POST /v1/refund)
        PAY->>DB: UPDATE payments SET status=REFUNDED
        ORD1->>DB: INSERT outbox (order.cancelled + payment.refunded)
        ORD1->>RB: PUBLIC order.cancelled + payment.refunded
    else Sin conflicto
        ORD1->>DB: UPDATE orders SET status=CONFIRMED
        ORD1->>RB: PUBLIC order.confirmed
    end
```

### 6.4 Algoritmo: Admisión Waiting Room (WR-5)

```typescript
// Ejecutado por Scheduler cada 5 segundos por evento
async function computeAndAdmitBatch(eventId: string): Promise<void> {
  // 1. Obtener asientos disponibles reales (CAT-3 overlay)
  const availableSeats = await catalog.getAvailableSeatCount(eventId);
  
  // 2. Calcular tamaño lote: 1.2x asientos disponibles, máx 1000
  const batchSize = Math.min(Math.ceil(availableSeats * 1.2), 1000);
  if (batchSize === 0) return; // Sin cupo
  
  // 3. Tomar siguientes en cola (Redis ZRANGE + ZREM atómico via Lua)
  const admitted = await redis.evalsha(ADMIT_BATCH_LUA, 1, 
    `queue:${eventId}`, batchSize, Date.now()
  );
  // admitted = [{queueTicketId, userId, admittedAt}]
  
  // 4. Para cada admitido: firmar token HMAC + publicar admission.granted
  for (const user of admitted) {
    const token = signAdmissionToken({
      queueTicketId: user.queueTicketId,
      eventId,
      userId: user.userId,
      admittedAt: new Date().toISOString(),
      exp: Math.floor(Date.now() / 1000) + 600 // 10 min
    }, WR_ADMISSION_SECRET);
    
    await rabbitmq.publish('waiting-room', 'admission.granted', {
      queueTicketId: user.queueTicketId,
      userId: user.userId,
      eventId,
      admissionToken: token,
      admittedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 600000).toISOString(),
    });
  }
}
```

**Lua Script `ADMIT_BATCH`** (atómico):
```lua
-- KEYS[1] = queue:{eventId}
-- ARGV[1] = batchSize, ARGV[2] = now
local batch = tonumber(ARGV[1])
local admitted = redis.call('ZRANGE', KEYS[1], 0, batch - 1)
if #admitted > 0 then
  redis.call('ZREM', KEYS[1], unpack(admitted))
end
return admitted
```

### 6.5 Algoritmo: Sweeper Holds Expirados (RES-5)

```typescript
// Ejecutado cada 30 segundos
async function sweepExpiredHolds(): Promise<void> {
  // ZRANGEBYSCORE holds:by-expiry 0 now → holdIds expirados
  const expiredHoldIds = await redis.zrangebyscore('holds:by-expiry', 0, Date.now());
  
  for (const holdId of expiredHoldIds) {
    // Obtener eventId + seatIds del hold (Redis hash hold:{holdId})
    const holdData = await redis.hgetall(`hold:${holdId}`);
    if (!holdData) continue; // Ya consumido/liberado
    
    const { eventId, seatIds } = holdData;
    
    // RELEASE atómico via Lua (libera seats → AVAILABLE)
    await redis.evalsha(RELEASE_LUA, 1, holdId, eventId, seatIds);
    
    // Publicar hold.expired para Order Service
    await rabbitmq.publish('reservation', 'hold.expired', {
      holdId,
      eventId,
      seatIds: JSON.parse(seatIds),
      reason: 'TTL_EXPIRED',
    });
  }
  
  // Limpiar ZSET
  await redis.zremrangebyscore('holds:by-expiry', 0, Date.now());
}
```

### 6.6 Idempotencia: Claves y Estrategias

| Componente | Clave Idempotencia | Mecanismo | Spec |
|------------|-------------------|-----------|------|
| **Order Checkout** | `Idempotency-Key` header | `orderId = SHA256(userId + ':' + key)` + `UNIQUE(order_id)` + `request_hash` | ORD-3 |
| **Payment** | `payment_idempotency_key` | `UNIQUE(payment_idempotency_key)` en `payment.payments` | PAY-5 |
| **Ticket Issue** | `UNIQUE(order_id, seat_id)` | MySQL PK + `INSERT IGNORE` pattern | TKT-2 |
| **Notification** | `dedupKey` en evento | `processed_events` table + `INSERT IGNORE` | NOT-2 |
| **Waiting Room Join** | `userId` por evento | `ZADD` solo si no existe (Lua) | WR-2 |

---

## 7. MANEJO DE ERRORES Y CÓDIGOS

### 7.1 Formato de Error Estándar (RFC 9457 - ProblemDetails)

Todas las respuestas de error siguen **RFC 9457** con extensión `code`:

```json
{
  "type": "https://api.flashtix.com/errors/SEAT_CONFLICT",
  "title": "Conflict",
  "status": 409,
  "code": "SEAT_CONFLICT",
  "detail": "Seat A-01-01 is no longer available",
  "instance": "/v1/holds",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 7.2 Códigos de Error por Servicio

#### Catálogo (CAT)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `EVENT_NOT_FOUND` | 404 | Evento no existe o no está publicado |
| `SEAT_NOT_FOUND` | 404 | Asiento no existe en el evento |
| `INVALID_EVENT_STATUS` | 409 | Operación inválida para estado actual (ej: modificar evento PUBLISHED) |

#### Waiting Room (WR)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `QUEUE_NOT_FOUND` | 404 | Cola no existe para evento |
| `TICKET_NOT_FOUND` | 404 | Ticket de cola no existe o no pertenece al usuario |
| `ADMISSION_EXPIRED` | 410 | Token de admisión expirado (10 min) |
| `ALREADY_IN_QUEUE` | 409 | Usuario ya en cola (idempotente) |

#### Reservas (RES)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `SEAT_CONFLICT` | 409 | Asiento ya en HOLD/LOCKED/SOLD por otro usuario |
| `HOLD_NOT_FOUND` | 404 | Hold no existe |
| `HOLD_EXPIRED` | 410 | Hold expirado (TTL 10 min) |
| `INVALID_HOLD_STATUS` | 409 | Operación inválida (ej: liberar hold LOCKED) |
| `ADMISSION_TOKEN_INVALID` | 403 | Token HMAC inválido o expirado |
| `ADMISSION_TOKEN_MISMATCH` | 403 | Token pertenece a otro usuario/evento |

#### Pedidos (ORD)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `HOLD_NOT_FOUND` | 404 | Hold no existe |
| `HOLD_NOT_OWNER` | 403 | Hold pertenece a otro usuario |
| `INVALID_HOLD_STATUS` | 409 | Hold no está HELD (expirado, consumido) |
| `IDEMPOTENCY_KEY_REQUIRED` | 400 | Falta header `Idempotency-Key` |
| `IDEMPOTENCY_KEY_CONFLICT` | 409 | Misma key con body diferente (`request_hash` mismatch) |
| `ORDER_NOT_FOUND` | 404 | Orden no existe o no pertenece al usuario |
| `SEAT_CONFLICT` | 409 | Doble venta detectada (PK sold_seats) |
| `PAYMENT_DECLINED` | 402 | Tarjeta rechazada por gateway |
| `PAYMENT_TIMEOUT` | 504 | Gateway no respondió en 20s + retries |

#### Pagos (PAY)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `GATEWAY_TIMEOUT` | 504 | Gateway no respondió (20s + 3 retries) |
| `GATEWAY_ERROR` | 502 | Gateway retornó 5xx |
| `CIRCUIT_OPEN` | 503 | Circuit breaker OPEN (50% errors/30s) |
| `IDEMPOTENCY_CONFLICT` | 409 | Duplicate payment idempotency key |

#### Tickets (TKT)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `TICKET_NOT_FOUND` | 404 | Ticket no existe o no pertenece al usuario (mismo 404) |
| `QR_VERIFICATION_FAILED` | 400 | QR payload firma inválida |

#### Notificaciones (NOT)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `EMAIL_SEND_FAILED` | 500 | SMTP error (reintentado por consumer) |

#### Gateway (GW)
| Código | HTTP | Descripción |
|--------|------|-------------|
| `UNAUTHENTICATED` | 401 | JWT faltante, expirado, malformado |
| `FORBIDDEN` | 403 | Rol insuficiente (ej: FAN en endpoint ORGANIZER) |
| `RATE_LIMITED` | 429 | Límite rate-limiting excedido |
| `UPSTREAM_TIMEOUT` | 504 | Servicio downstream timeout |

### 7.3 Códigos de Evento (RabbitMQ) para Consumers

| Evento | Producer | Consumers | Deduplicación |
|--------|----------|-----------|---------------|
| `admission.granted` | Waiting Room | — (fan polling) | N/A |
| `hold.expired` | Reservation (Sweeper) | Order | `hold.expired:{holdId}` |
| `payment.initiated` | Order (Outbox) | Payment Worker | `payment.initiated:{orderId}` |
| `payment.succeeded` | Payment Worker | Order | `payment.succeeded:{paymentId}` |
| `payment.failed` | Payment Worker | Order | `payment.failed:{paymentId}` |
| `order.confirmed` | Order | Ticket, Notification | `order.confirmed:{orderId}` |
| `order.failed` | Order | Notification | `order.failed:{orderId}` |
| `order.cancelled` | Order | Notification | `order.cancelled:{orderId}` |
| `ticket.issued` | Ticket | Notification | `ticket.issued:{orderId}` |
| `payment.refunded` | Order/Payment | Notification | `payment.refunded:{paymentId}` |

---

## 8. BASE DE DATOS

### 8.1 Esquema por Base de Datos (5 MySQL 8 Instances)

#### `catalog` — Eventos y Asientos
```sql
-- catalog.events
CREATE TABLE events (
  event_id        CHAR(36)     NOT NULL PRIMARY KEY,
  name            VARCHAR(255) NOT NULL,
  artist          VARCHAR(255) NOT NULL,
  venue_name      VARCHAR(255) NOT NULL,
  venue_city      VARCHAR(100) NOT NULL,
  starts_at       DATETIME(3)  NOT NULL,
  sale_starts_at  DATETIME(3)  NOT NULL,
  status          ENUM('DRAFT','PUBLISHED','ON_SALE','ENDED','CANCELLED') NOT NULL DEFAULT 'DRAFT',
  created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  INDEX idx_status_sale (status, sale_starts_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- catalog.seats
CREATE TABLE seats (
  seat_id         VARCHAR(64)  NOT NULL,
  event_id        CHAR(36)     NOT NULL,
  section         VARCHAR(64)  NOT NULL,
  row_label       VARCHAR(16)  NOT NULL,
  number          INT          NOT NULL,
  category        VARCHAR(64)  NOT NULL,
  status          ENUM('AVAILABLE','HELD','LOCKED','SOLD') NOT NULL DEFAULT 'AVAILABLE',
  hold_id         CHAR(36)     NULL,
  locked_at       DATETIME(3)  NULL,
  PRIMARY KEY (event_id, seat_id),
  FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE,
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- catalog.price_categories
CREATE TABLE price_categories (
  category_id     CHAR(36)     NOT NULL PRIMARY KEY,
  event_id        CHAR(36)     NOT NULL,
  category        VARCHAR(64)  NOT NULL,
  price_amount    BIGINT       NOT NULL,  -- en centavos (COP)
  price_currency  CHAR(3)      NOT NULL DEFAULT 'COP',
  FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `booking` — Órdenes, Holds, Outbox
```sql
-- booking.orders
CREATE TABLE orders (
  order_id           CHAR(36)     NOT NULL PRIMARY KEY,
  user_id            CHAR(36)     NOT NULL,
  user_email         VARCHAR(255) NOT NULL,
  event_id           CHAR(36)     NOT NULL,
  hold_id            CHAR(36)     NOT NULL,
  idempotency_key    CHAR(36)     NOT NULL,
  request_hash       CHAR(64)     NOT NULL,  -- SHA256(body)
  status             ENUM('PENDING','CONFIRMED','FAILED','CANCELLED','REFUND_PENDING','REFUNDED') NOT NULL DEFAULT 'PENDING',
  total_amount       BIGINT       NOT NULL,  -- centavos
  failure_reason     VARCHAR(64)  NULL,      -- PAYMENT_DECLINED, SEAT_CONFLICT, etc.
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_idempotency (user_id, idempotency_key),
  INDEX idx_user_status (user_id, status),
  INDEX idx_event_status (event_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- booking.holds
CREATE TABLE holds (
  hold_id       CHAR(36)     NOT NULL PRIMARY KEY,
  event_id      CHAR(36)     NOT NULL,
  user_id       CHAR(36)     NOT NULL,
  seat_ids      JSON         NOT NULL,      -- array de seatId
  status        ENUM('HELD','LOCKED','CONSUMED','RELEASED','EXPIRED') NOT NULL DEFAULT 'HELD',
  expires_at    DATETIME(3)  NOT NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  INDEX idx_event_status (event_id, status),
  INDEX idx_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- booking.sold_seats (PK compuesta evita doble venta - ORD-5)
CREATE TABLE sold_seats (
  event_id    CHAR(36)    NOT NULL,
  seat_id     VARCHAR(64) NOT NULL,
  order_id    CHAR(36)    NOT NULL,
  sold_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (event_id, seat_id),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- booking.outbox (Transactional Outbox - ORD-2)
CREATE TABLE outbox (
  outbox_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  aggregate_id  CHAR(36)        NOT NULL,
  aggregate_type VARCHAR(64)    NOT NULL,  -- "order", "payment"
  event_type    VARCHAR(128)    NOT NULL,  -- "order.confirmed", "payment.initiated"
  payload       JSON            NOT NULL,
  created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  published_at  DATETIME(3)     NULL,
  INDEX idx_unpublished (published_at, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `payment` — Pagos y Reembolsos
```sql
-- payment.payments
CREATE TABLE payments (
  payment_id              CHAR(36)     NOT NULL PRIMARY KEY,
  order_id                CHAR(36)     NOT NULL,
  payment_idempotency_key CHAR(64)     NOT NULL,  -- orderId + attempt
  gateway                 VARCHAR(64)  NOT NULL DEFAULT 'mock',
  gateway_payment_id      VARCHAR(128) NULL,
  status                  ENUM('INITIATED','PROCESSING','SUCCEEDED','FAILED','REFUNDED') NOT NULL DEFAULT 'INITIATED',
  amount                  BIGINT       NOT NULL,  -- centavos
  currency                CHAR(3)      NOT NULL DEFAULT 'COP',
  failure_reason          VARCHAR(128) NULL,
  retry_count             TINYINT      NOT NULL DEFAULT 0,
  created_at              DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at              DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_idempotency (payment_idempotency_key),
  INDEX idx_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `ticket` — Tickets Emitidos
```sql
-- ticket.tickets
CREATE TABLE tickets (
  ticket_id      CHAR(36)     NOT NULL PRIMARY KEY,
  order_id       CHAR(36)     NOT NULL,
  event_id       CHAR(36)     NOT NULL,
  user_id        CHAR(36)     NOT NULL,
  seat_id        VARCHAR(64)  NOT NULL,
  category       VARCHAR(64)  NOT NULL,
  qr_payload     VARCHAR(512) NOT NULL,  -- "FTX1.{ticketId}.{sig}"
  pdf_object_key VARCHAR(256) NOT NULL,  -- S3 key
  issued_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_order_seat (order_id, seat_id),  -- Idempotencia TKT-2
  INDEX idx_user (user_id),
  INDEX idx_event (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `notification` — Deduplicación Eventos
```sql
-- notification.processed_events
CREATE TABLE processed_events (
  event_id    CHAR(36)     NOT NULL,      -- envelope.eventId
  consumer    VARCHAR(64)  NOT NULL,      -- "notification"
  dedup_key   VARCHAR(256) NOT NULL,      -- "ticket.issued:{orderId}"
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (event_id, consumer),
  UNIQUE KEY uk_dedup (consumer, dedup_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 8.2 Redis — Estructuras de Datos

#### Redis Catalog (Cache + Waiting Room)
| Clave | Tipo | TTL | Descripción |
|-------|------|-----|-------------|
| `seats:{eventId}` | String (JSON) | 30s | Seat map cacheado (CAT-3) |
| `queue:{eventId}` | Sorted Set | — | Cola virtual: score=timestamp, member=`{userId}:{queueTicketId}` (WR-1) |
| `admission:{queueTicketId}` | String (JSON) | 10m | Token admisión + metadata (WR-3) |

#### Redis Hold Store (Holds Atómicos)
| Clave | Tipo | TTL | Descripción |
|-------|------|-----|-------------|
| `hold:{holdId}` | Hash | 10m | `{eventId, userId, seatIds[], status, createdAt}` (RES-1) |
| `seat:{eventId}:{seatId}` | String | 10m | `holdId` si HELD/LOCKED, vacío si AVAILABLE (RES-1) |
| `holds:by-expiry` | Sorted Set | — | Score=expiry timestamp, member=holdId (RES-5 Sweeper) |

---

## 9. TESTING, CI/CD Y OBSERVABILIDAD

### 9.1 Smoke Tests (Validación E2E)

#### Fase 2 — `scripts/smoke-phase2.mjs`
Valida: Happy path (CONFIRMED), Failure path (FAILED → asiento libre), Double sale (CANCELLED + REFUND), LockHold gRPC, Sweeper TTL, Idempotencia checkout.

```bash
# Requiere: docker compose up -d --build
node scripts/smoke-phase2.mjs
```

**Checks p95 (spec.md §6)**:
- `catalog p95 < 50ms` — GET `/v1/events/{eventId}/seats`
- `hold p95 < 100ms` — POST `/v1/holds`
- `POST /orders p95 < 300ms` — Checkout completo

#### Fase 3 — `scripts/smoke-phase3.mjs`
Valida: Organizer console (GW-3), Tickets PDF+QR (TKT-2 a TKT-4), Emails deduplicados (NOT-2), Duplicate delivery, Failed order email (NOT-3), Organizer refund (ADM-3), p95 latencia.

```bash
node scripts/smoke-phase3.mjs
```

### 9.2 Unit / Integration Tests (por Servicio)

```bash
# En cada servicio:
cd services/catalog && npm run test          # Jest unit tests
cd services/catalog && npm run test:cov      # Coverage
cd services/catalog && npm run test:e2e      # Integration (Testcontainers)
```

### 9.3 CI/CD Pipeline (GitHub Actions - Implícito)

```yaml
# .github/workflows/ci.yml (referencia conceptual)
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: npm ci
      - run: npm run lint        # ESLint + Prettier
  
  typecheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: npm ci
      - run: npm run typecheck   # tsc --noEmit
  
  test:
    runs-on: ubuntu-latest
    services:
      mysql: { image: mysql:8, env: {...} }
      redis: { image: redis:7 }
      rabbitmq: { image: rabbitmq:3.13-management }
    steps:
      - uses: actions/checkout@v4
      - run: npm ci
      - run: npm run test        # Unit tests all workspaces
  
  build:
    needs: [lint, typecheck, test]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose -f docker-compose.ci.yml build
      - run: docker compose -f docker-compose.ci.yml push
  
  smoke:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose up -d
      - run: sleep 30 && node scripts/smoke-phase2.mjs
      - run: node scripts/smoke-phase3.mjs
```

### 9.4 Observabilidad Stack (Phase 4)

#### Prometheus (`infra/observability/prometheus/`)
- **Scrape targets**: Todos los servicios en `:9464/metrics` + Kong `:8001/metrics` + Node Exporter + Cadvisor
- **Alerting Rules** (`flashtix-alerts.yml`):
  - `HighErrorRate` > 5% en 5m
  - `HighLatencyP95` > SLO (catalog 50ms, hold 100ms, order 300ms)
  - `CircuitBreakerOpen` payment worker
  - `QueueDepth` waiting room > 10000
  - `HoldSweeperLag` > 60s
  - `OutboxLag` > 100 eventos sin publicar

#### Grafana Dashboards (`infra/observability/grafana/dashboards/`)
| Dashboard | Paneles Clave |
|-----------|---------------|
| **flashtix-overview** | Request rate, error rate, p50/p95/p99 latency por servicio, active SSE connections |
| **flashtix-business** | Orders/min (CONFIRMED/FAILED/CANCELLED), Tickets issued, Emails sent, Revenue (COP) |
| **flashtix-infrastructure** | CPU/Memory/Network/Disk por contenedor, MySQL connections, Redis memory, RabbitMQ queue depths |
| **flashtix-slo** | SLI/SLO tracking: Availability > 99.9%, Latency p95 < targets, Error rate < 0.1% |

#### Loki + Promtail (`infra/observability/loki/`, `promtail/`)
- **Log format**: JSON estructurado con `level`, `timestamp`, `service`, `traceId`, `spanId`, `message`, `context`
- **Labels**: `service`, `namespace`, `pod`, `level`
- **Retention**: 30 días

#### Tempo (`infra/observability/tempo/`)
- **Receiver**: OTLP HTTP `:4318/v1/traces`
- **Sampling**: Tail-based (always sample errors, 10% success)
- **Retention**: 7 días

#### Métricas Prometheus por Servicio (ejemplos)

**Catalog**:
```prometheus
# HELP catalog_seats_cache_hit_total Cache hits for seat map
# TYPE catalog_seats_cache_hit_total counter
catalog_seats_cache_hit_total{event_id="..."} 12450

# HELP http_request_duration_seconds HTTP request latency
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{handler="/v1/events/{eventId}/seats",le="0.05"} 11800
http_request_duration_seconds_bucket{handler="/v1/events/{eventId}/seats",le="0.1"} 12300
```

**Reservation**:
```prometheus
reservation_hold_acquire_duration_seconds_bucket{result="ok",le="0.01"} 5000
reservation_hold_acquire_total{result="conflict"} 23
reservation_sweeper_swept_total 150
```

**Order**:
```prometheus
order_checkout_duration_seconds_bucket{le="0.3"} 8500
order_status_transitions_total{from="PENDING",to="CONFIRMED"} 4200
order_status_transitions_total{from="PENDING",to="FAILED"} 180
order_status_transitions_total{from="PENDING",to="CANCELLED"} 12
order_sse_connections_active 150
```

**Payment Worker**:
```prometheus
payment_gateway_request_duration_seconds_bucket{le="20"} 3800
payment_circuit_breaker_state 0  # 0=closed, 1=open, 2=half-open
payment_retries_total 45
payment_outcome_total{result="succeeded"} 3900
payment_outcome_total{result="failed"} 180
```

**Ticket**:
```prometheus
ticket_issue_duration_seconds_bucket{le="1"} 4000
ticket_pdf_render_duration_seconds_bucket{le="0.5"} 3900
ticket_s3_upload_duration_seconds_bucket{le="0.2"} 3850
ticket_issued_total 8400
```

**Notification**:
```prometheus
notification_email_sent_total{type="ticket_issued"} 4200
notification_email_sent_total{type="order_failed"} 180
notification_deduplicated_total 5
```

### 9.5 SLOs Definidos (spec.md §6)

| SLI | Objetivo (SLO) | Ventana |
|-----|----------------|---------|
| **Disponibilidad** | 99.9% | 30 días |
| **Latencia p95 Catálogo** | < 50 ms | 5 min |
| **Latencia p95 Hold** | < 100 ms | 5 min |
| **Latencia p95 Checkout** | < 300 ms | 5 min |
| **Tasa de Error** | < 0.1% | 5 min |
| **Ticket Issuance** | 100% orders confirmed → tickets issued | 1 min |
| **Email Delivery** | 100% events → email sent | 5 min |

---

## 10. GLOSARIO TÉCNICO

| Término | Definición |
|---------|------------|
| **Admission Token** | JWT-like token firmado con HMAC-SHA256 que autoriza a un fan a crear holds. Contiene: `queueTicketId`, `eventId`, `userId`, `admittedAt`, `exp`. Validez: 10 minutos. |
| **Cache-Aside** | Patrón de caché: la aplicación consulta Redis primero; si miss, consulta BD, guarda en Redis y retorna. Usado en Catálogo (TTL 30s). |
| **Circuit Breaker** | Patrón de resiliencia: tras umbral de errores (50% en 30s), abre el circuito y rechaza requests rápidos por 60s antes de half-open. Usado en Payment Worker. |
| **Consumer (RabbitMQ)** | Servicio que consume mensajes de una cola. En FlashTix: cada servicio tiene consumers tipados (OrderConsumers, NotificationConsumers, etc.). |
| **Correlation ID** | UUIDv4 propagado via header `X-Correlation-Id` a través de todos los servicios y eventos. Generado por Kong (plugin correlation-id). |
| **Dead Letter Queue (DLQ)** | Cola donde van mensajes que fallan tras reintentos máximos. En FlashTix: `{queue}.dlq` con TTL 7 días. |
| **Deduplicación** | Mecanismo para procesar un evento exactamente una vez. Implementado con tabla `processed_events` + `INSERT IGNORE` en consumer. |
| **Event Envelope** | Estructura estándar v1 para todos los eventos RabbitMQ: `eventId`, `eventType`, `eventVersion`, `occurredAt`, `producer`, `correlationId`, `aggregateId`, `aggregateVersion`, `data`. |
| **Hold** | Reserva temporal de asientos (estado HELD/LOCKED/CONSUMED/RELEASED/EXPIRED). TTL 10 minutos. Gestionado por Reservation Service con Lua atómico. |
| **Idempotency Key** | Header `Idempotency-Key` (UUIDv4) enviado por cliente en POST `/orders`. Genera `orderId` determinista: `SHA256(userId + ':' + key)`. |
| **LockHold (gRPC)** | RPC interno Order→Reservation para cambiar hold de HELD a LOCKED antes de iniciar pago. Previene race conditions. |
| **Outbox Pattern** | Patrón para publicar eventos de forma confiable: INSERT evento en tabla `outbox` dentro de misma transacción que el negocio; poller publica a RabbitMQ. |
| **ProblemDetails (RFC 9457)** | Formato estándar de error HTTP: `type`, `title`, `status`, `code`, `detail`, `instance`, `correlationId`. Usado en toda la API. |
| **QR Payload** | String firmado HMAC: `FTX1.{ticketId}.{base64url(HMAC-SHA256(ticketId+eventId+seatId))}`. Verificable en entrada sin BD. |
| **Rate Limiting (Kong)** | Plugin rate-limiting: Público 100 req/s/IP (minute:6000), Protegido 20 req/s/usuario (minute:1200, consumer-based). |
| **Saga** | Secuencia de operaciones locales con compensaciones. Checkout: PENDING → (pago) → CONFIRMED/FAILED → (compensación: release hold, refund). |
| **Seat Map Overlay** | Combinación de estado BD (SOLD) + Redis (HELD/LOCKED) para mostrar disponibilidad real en GET `/seats`. |
| **SSE (Server-Sent Events)** | Endpoint `/orders/{orderId}/stream` para push de cambios de estado orden en tiempo real (PENDING → CONFIRMED/FAILED/CANCELLED). |
| **Sweeper** | Job periódico (30s) en Reservation que escanea `holds:by-expiry` y libera holds expirados via Lua `RELEASE` + publica `hold.expired`. |
| **Topología RabbitMQ** | Declarativa en `libs/shared/topology.ts`: 7 exchanges (topic), queues por consumer/eventType, DLQs, bindings. |
| **Transactional Outbox** | Tabla `booking.outbox` escrita en misma transacción que la orden. Poller cada 500ms publica a RabbitMQ con confirm channel. |
| **Virtual Queue (Waiting Room)** | Cola justa en Redis Sorted Set (`queue:{eventId}`). Admisión por lotes computados (1.2x asientos libres). Emite `admission.granted`. |

---

## ANEXO: COMANDOS ÚTILES DE DESARROLLO

```bash
# Ver logs estructurados (JSON) de un servicio
docker compose logs -f catalog | jq -c '.'

# Inspeccionar cola RabbitMQ
docker compose exec rabbitmq rabbitmqadmin list queues name messages consumers

# Ver keys Redis Catalog
docker compose exec redis-catalog redis-cli KEYS "*"

# Ver keys Redis Hold Store
docker compose exec redis-hold redis-cli KEYS "*"

# Conectar a MySQL Booking
docker compose exec booking-db mysql -uroot -proot booking

# Ver outbox sin publicar
docker compose exec booking-db mysql -uroot -proot booking -e "SELECT * FROM outbox WHERE published_at IS NULL;"

# Ver processed_events (notification)
docker compose exec notification-db mysql -uroot -proot notification -e "SELECT * FROM processed_events ORDER BY created_at DESC LIMIT 20;"

# Recargar Kong tras cambio kong.yml
docker compose restart api-gateway

# Ejecutar migraciones (si usa Prisma/TypeORM)
# docker compose exec catalog npx prisma migrate deploy

# Ver métricas Prometheus de un servicio
curl -s http://localhost:3001/metrics | grep -E "^catalog_|^http_request"

# Ver dashboards Grafana
open http://localhost:3000 (admin/admin)

# Ver trazas Tempo
open http://localhost:3200

# Ver logs Loki
open http://localhost:3100
```

---

> **Documentación generada automáticamente** a partir del código fuente, especificación OpenSpec v1.1.0 y configuración de infraestructura.
> **Última actualización**: 2026-11-12 | **Commit**: `feat/phase-4-observability` (8089a63)