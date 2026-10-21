# Scheduler

**Scheduler** — фоновый планировщик задач на Go 1.27 с очередью в PostgreSQL: API принимает задачи, воркеры выполняют их и повторяют при ошибке; неисправимые задачи складываются в DLQ (`dead`). Два типа задач:

- `http` — вызов внешнего URL (webhook), успех = ответ `2xx`;
- `noop` — мгновенный успех без внешних действий (удобен для smoke-тестов).

Ключевые свойства:

- **At-least-once**: задача гарантированно выполняется хотя бы раз после постановки в очередь. Возможны повторы (например, если воркер упал во время выполнения) — **обработчик должен быть идемпотентным**.
- **DLQ**: после исчерпания попыток (`max_attempts`) задача получает статус `dead`; вернуть её в работу можно через `POST /jobs/:id/retry`.
- Миграции встроены в бинарник (`embed`, SQL лежит в `migrations/`); при `AUTO_MIGRATE=true` применяются на старте.
- Метрики в формате Prometheus доступны на том же порту, что и API, — `/metrics`.

## Архитектура

```
   клиенты (curl)
       │  POST /jobs · GET /jobs(/:id) · POST /jobs/:id/retry · /health
       ▼
   ┌──────────── API :8080 (HTTP_ADDR) ────────────┐
   │ единый порт: API, /health, /metrics           │
   │ POST /jobs → 201 {"job":{…}}                  │
   │ GET /jobs(/:id) → 200 / 404                   │
   │ POST /jobs/:id/retry → 200/409 (только dead)  │
   │ GET /health → 200 / 503  ·  GET /metrics      │
   └───────┬───────────────────────────────────────┘
           │  enqueue / claim (UPDATE → running) / retry
           ▼
   ┌────────── PostgreSQL: таблица jobs — очередь ──────────┐
   │ pending → running → completed; noop — мгновенный успех │
   │ ошибка → pending (backoff 2s, 4s, 8s… cap 5m)          │
   │ исчерпание max_attempts → dead (DLQ)                   │
   └───────┬─────────────────────────────────────────────▲──┘
           │ poll + lease (POLL_INTERVAL)                │ reaper: stale running
           ▼                                             │ → pending, attempts+1;
   ┌──── workers × 3 (WORKER_COUNT) ───┐
   │ claim → exec → ack/fail           │
   └───────┬───────────────────────────┘
           │  http: url/method/headers/body · таймаут 10s · успех = 2xx
           ▼
      HTTP-цели (targets)

   Redis — вспомогательный сервис; /health проверяет его, если задан REDIS_ADDR
   Prometheus: scrape scheduler:8080/metrics каждые 5s → UI на :9090
```

Воркеры «захватывают» задачу атомарным `UPDATE` (переводом в `running`) из очереди в PostgreSQL — Postgres здесь и хранилище, и брокер. Отдельная фоновая процедура (**reaper**) возвращает в очередь протухшие `running`-задачи, если воркер умер до завершения.

## Быстрый старт

```bash
cd scheduler
docker compose up --build -d
```

Compose прочитает `.env` при его наличии (порты хоста — см. `.env.example`). Если порт занят локальным стеком, переопределите его:

```bash
SCHEDULER_PORT=18080 docker compose up --build -d
# и дальше все запросы — на http://127.0.0.1:18080
```

Проверка здоровья (200 — зависимости доступны; 503 — `degraded`):

```bash
curl -fsS http://127.0.0.1:8080/health | jq
# 200 {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}
# Redis не задан (REDIS_ADDR пуст) → checks.redis = "disabled".
# При сбое — 503 {"status":"degraded","checks":{"postgres":"error",…}};
# детали ошибки в ответ не попадают, они только в логах.
```

Постановка noop-задачи (мгновенный успех):

```bash
curl -s -X POST http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"noop"}' | jq
# 201
# {"job":{"id":1,"type":"noop","status":"pending","max_attempts":3,"attempts":0}}
# (id — число; полный объект задачи приходит под ключом "job")
```

Просмотр статуса задачи:

```bash
curl -s http://127.0.0.1:8080/jobs/<ID> | jq
```

Логи и остановка:

```bash
docker compose logs -f scheduler
docker compose down          # остановить и удалить контейнеры (данные — в volume)
docker compose down -v       # то же + удалить volumes (pgdata, promdata)
```

## Локальный запуск без Docker

Поднять Postgres и Redis любым способом, затем запустить приложение напрямую:

```bash
# 1. Зависимости (пример с docker; подойдёт и локальная установка)
docker run -d --name scheduler-pg -p 5432:5432 \
  -e POSTGRES_USER=scheduler -e POSTGRES_PASSWORD=scheduler -e POSTGRES_DB=scheduler \
  postgres:17-alpine
docker run -d --name scheduler-redis -p 6379:6379 redis:7-alpine

# 2. Переменные окружения (полный список — в разделе «Конфигурация»)
export DATABASE_URL='postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable'
export REDIS_ADDR='localhost:6379'   # можно не задавать: пусто = Redis не проверяется
export HTTP_ADDR=':8080'

# 3. Запуск (AUTO_MIGRATE=true по умолчанию применит миграции на старте)
go run ./cmd/scheduler
```

## API

| Метод | Путь | Коды | Описание |
|---|---|---|---|
| `POST` | `/jobs` | `201` / `400` / `413` | Создать задачу → `{"job":{…}}`. Тело: `{"type":"http\|noop","payload":{…},"max_attempts":3}` (`max_attempts` — необязательно, 1–100, по умолчанию 3); ошибки валидации → `400`/`413` |
| `GET` | `/jobs/:id` | `200` / `404` | Объект задачи → `{"job":{…}}`; не найдена → `404 {"error":"job not found"}` (`id` — число) |
| `GET` | `/jobs?status=pending&limit=50&offset=0` | `200` / `400` | Список с фильтром по статусу и пагинацией → `{"jobs":[…],"count":N,"limit":50,"offset":0}` |
| `POST` | `/jobs/:id/retry` | `200` / `409` | Вернуть задачу в очередь → `{"job":{…}}`. Только для `dead` (DLQ); иначе `409 {"error":"job is not dead"}` |
| `GET` | `/health` | `200` / `503` | Проверяет Postgres и Redis → `{"status":"ok","checks":{"postgres":"ok","redis":"ok"}}`; Redis → `"disabled"`, если `REDIS_ADDR` пуст. При сбое `503 {"status":"degraded",…}`, детали — только в логах |
| `GET` | `/metrics` | `200` | Метрики в формате Prometheus (тот же порт, что и API) |

Ошибки возвращаются в виде `{"error":"…"}`. Пустой `payload` сохраняется как `{}`.

### Создание http-задачи

```bash
curl -s -X POST http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "http",
    "payload": {
      "url": "https://example.com/hook",
      "method": "POST",
      "headers": {"X-Key": "value"},
      "body": "hello"
    },
    "max_attempts": 3
  }' | jq
# 201 {"job":{"id":2,"type":"http","status":"pending",…}}
```

- `method` по умолчанию — `POST`;
- успех — любой ответ `2xx`;
- таймаут вызова — `10s`.

### Создание noop-задачи

```bash
curl -s -X POST http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"noop","max_attempts":3}' | jq
# noop завершается мгновенно со статусом completed — удобно для smoke-тестов
```

### Список задач с фильтрацией и пагинацией

```bash
curl -s 'http://127.0.0.1:8080/jobs?status=pending&limit=50&offset=0' | jq
# {"jobs":[…],"count":12,"limit":50,"offset":0}

# DLQ — все неисправимые задачи:
curl -s 'http://127.0.0.1:8080/jobs?status=dead&limit=50&offset=0' | jq
```

### Retry из DLQ

```bash
JOB_ID=$(curl -s -X POST http://127.0.0.1:8080/jobs -H 'Content-Type: application/json' \
  -d '{"type":"http","payload":{"url":"https://example.com/unreachable"},"max_attempts":1}' \
  | jq -r '.job.id')

# ...получаем 409 {"error":"job is not dead"} для не-dead задачи:
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8080/jobs/$JOB_ID/retry

# после исчерпания попыток (статус dead) — 200, задача снова в очереди:
curl -s -X POST http://127.0.0.1:8080/jobs/$JOB_ID/retry | jq
```

## TUI

Интерактивный клиент к API: таблица задач, retry и создание задач без `curl`.

```bash
go run ./cmd/scheduler tui --api http://localhost:8080
# или через переменную окружения
SCHEDULER_API_URL=http://localhost:8080 ./scheduler tui
```

```
Scheduler   задач: 3
n — создать · r — retry · R — обновить · ↑/↓ — выбор · q — выход

   ID     TYPE       STATUS      ATTEMPTS   CREATED
▶ 42     http       pending     0/3        09:42:50
  41     noop       completed   0/1        09:42:12

╭─ Создание задачи ──────────────────────────────────╮
│ ▶ Type:     http                                   │
│   Payload:  {"url":"https://example.com"}          │
│   Attempts: 3                                      │
│                                                    │
│ Tab: следующее поле · Enter: создать · Esc: отмена │
╰────────────────────────────────────────────────────╯
```

| Клавиша | Действие |
|---|---|
| `n` | открыть форму создания задачи |
| `Tab` / `Shift+Tab` (или `↓`/`↑`) | переключение поля: Type → Payload → Attempts |
| `Enter` | отправить `POST /jobs`; при ошибке форма остаётся открытой и показывает текст ошибки API |
| `Esc` | закрыть форму без отправки |
| `↑`/`↓` (`k`/`j`) | выбор задачи в таблице |
| `r` | `POST /jobs/:id/retry` для выбранной задачи |
| `R` | перечитать список |
| `q` / `Ctrl+C` | выход |

Форма проверяет ввод до отправки: `payload` должен быть валидным JSON, для типа `http` — содержать `{"url":"http(s)://…"}`, `attempts` — целое число 1–100. Пустые поля означают значения по умолчанию: тип `http`, payload `{}`, attempts `3`.

TUI работает только через HTTP API (Postgres и Redis ему не нужны), поэтому его можно запускать с любой машины, где доступен порт API.

Тесты: `go test ./internal/tui` (модель и HTTP-клиент, покрытие ~90%). Живой прогон против запущенного API:

```bash
SCHEDULER_API_URL=http://localhost:18080 go test ./internal/tui -run TestLiveAPI -v
```

## Жизненный цикл задачи

```
        ┌──────────────────────────── экспоненциальный backoff:
        │                              2s, 4s, 8s… (потолок BACKOFF_MAX=5m)
        ▼
  ┌─────────┐   claim     ┌─────────┐   успех (2xx)   ┌───────────┐
  │ pending │────────────►│ running │────────────────►│ completed │
  └─────────┘             └─────────┘                 └───────────┘
        ▲                      │                            noop завершается
        │ ошибка:              │                            мгновенно
        │ attempts < max ──────┤
        │                      └─ attempts = max ──► dead (DLQ)
        │
        └── reaper: running дольше STALE_TIMEOUT=60s → pending, attempts+1
            (если после reclaim попытки исчерпаны → dead)
```

- **Backoff**: после неуспешной попытки задача возвращается в `pending` и переотправляется с экспоненциальной задержкой (`BACKOFF_BASE=2s`, затем `4s`, `8s`…, потолок `BACKOFF_MAX=5m`).
- **DLQ**: после `max_attempts` задача переходит в `dead`. Её можно вытащить через `POST /jobs/:id/retry` — счётчик попыток начнётся заново (см. API).
- **Reclaim**: если воркер упал, не завершив задачу, reaper через `STALE_TIMEOUT` возвращает её в `pending` и инкрементирует `attempts`; при исчерпании — в `dead`.
- **Идемпотентность**: из-за at-least-once одна задача может быть выполнена более одного раза (упавший воркер не подтверждает результат → задача переотправится). Обработчики должны выдерживать повторные вызовы — например, опираться на бизнес-ключи/дедупликацию на стороне цели.

## Конфигурация

Все переменные окружения (значения по умолчанию — в `.env.example`). Длительности задаются в формате Go (`500ms`, `10s`, `5m`).

| Переменная | По умолчанию | Описание |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Адрес HTTP-сервера (API + `/health` + `/metrics`) |
| `DATABASE_URL` | `postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable` | Строка подключения к PostgreSQL |
| `REDIS_ADDR` | пусто | Адрес Redis (`host:port`); пусто = `/health` не проверяет Redis |
| `WORKER_COUNT` | `3` | Количество воркеров |
| `POLL_INTERVAL` | `500ms` | Интервал опроса очереди |
| `JOB_TIMEOUT` | `10s` | Таймаут выполнения задачи |
| `STALE_TIMEOUT` | `60s` | Через сколько `running`-задача считается протухшей (reclaim reaper'ом) |
| `BACKOFF_BASE` | `2s` | База экспоненциального backoff |
| `BACKOFF_MAX` | `5m` | Максимальная пауза между попытками |
| `MAX_PAYLOAD_BYTES` | `1048576` | Максимальный размер payload (1 MiB) |
| `RATE_LIMIT` | `100` | Ограничение API: столько запросов за `RATE_WINDOW` на один IP |
| `RATE_WINDOW` | `1s` | Окно rate limit |
| `JOB_TYPES` | `http,noop` | Whitelist разрешённых типов задач |
| `AUTO_MIGRATE` | `true` | Применять встроенные миграции на старте |
| `LOG_LEVEL` | `info` | Уровень логирования |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown при получении сигнала завершения |
| `WORKER_ID` | пусто | Опциональный идентификатор экземпляра; по умолчанию не задаётся |

Порты хоста в docker compose задаются отдельно — `POSTGRES_PORT`, `REDIS_PORT`, `SCHEDULER_PORT`, `PROMETHEUS_PORT` (см. `.env.example`).

## Наблюдаемость

- **`GET /health`** — `200` `{"status":"ok","checks":{"postgres":"ok","redis":"ok"}}` (Redis → `"disabled"`, если `REDIS_ADDR` пуст), при сбое — `503` `{"status":"degraded","checks":{"postgres":"error",…}}`. Детали ошибок в ответ не попадают — они только в логах. Тот же эндпоинт используется healthcheck'ом контейнера.
- **`GET /metrics`** — Prometheus-метрики:

| Метрика | Описание |
|---|---|
| `jobs_processed_total{status="completed\|failed\|dead"}` | Исходы обработанных воркером задач: `completed` — успешное выполнение; `failed` — выполнение с ошибкой, задача возвращена в `pending` на ретрай; `dead` — попытки исчерпаны (DLQ). Значения `pending` в лейбле не бывает |
| `jobs_failed_total` | Число неуспешных выполнений; инкрементируется, когда запись о неуспехе успешно сохранена в БД |
| `jobs_pending` | Длина очереди (gauge обновляется фоново примерно раз в 5 с) |

Полезные запросы: `rate(jobs_processed_total[1m])`, `jobs_pending`, доля `dead` — `rate(jobs_processed_total{status="dead"}[5m])`.

- **Prometheus**: UI на `:9090` (или на `PROMETHEUS_PORT`, если переопределён). Scrape — `5s`, таргет — `scheduler:8080`, retention — `7d`.

## Тесты

Запуск юнит/интеграционных тестов:

```bash
go test ./... -race
```

Интеграционные тесты требуют PostgreSQL через `TEST_DATABASE_URL`. Пример поднятия тестовой базы на порту 15432:

```bash
docker run -d --name scheduler-test-pg -p 15432:5432 \
  -e POSTGRES_USER=scheduler -e POSTGRES_PASSWORD=scheduler -e POSTGRES_DB=scheduler \
  postgres:17-alpine

TEST_DATABASE_URL='postgres://scheduler:scheduler@localhost:15432/scheduler?sslmode=disable' \
  go test ./... -race
```

В наборе есть конкурентный тест — **10 воркеров × 100 задач** — на корректность параллельной обработки одной очереди. CI и статус прогонов появятся позже.

## Безопасность

- **Лимит payload**: `MAX_PAYLOAD_BYTES=1048576` — задачи больше 1 MiB не принимаются.
- **Whitelist типов**: принимаются только типы из `JOB_TYPES` (по умолчанию `http,noop`).
- **Rate limit**: не более `RATE_LIMIT` запросов за `RATE_WINDOW` с одного IP (по умолчанию 100 rps).
- **Таймауты**: `JOB_TIMEOUT` на выполнение задачи, `10s` на HTTP-вызов, `SHUTDOWN_TIMEOUT` на graceful остановку.
- **Контейнер**: минимальный `alpine:3.20` (только бинарник + `ca-certificates`/`tzdata`), non-root пользователь UID 10001.
- ⚠️ **SSRF**: задачи `http` позволяют выполнять запросы на произвольные URL из payload. Ограничивайте исходящий трафик (egress) на уровне инфраструктуры (network policy/файрвол/прокси) так, чтобы scheduler не имел доступа к внутренним сервисам и облачным metadata-эндпоинтам.

## Структура проекта

```
.
├── Dockerfile              # multi-stage: статическая сборка Go + alpine:3.20 runtime
├── .dockerignore           # контекст сборки: только go.mod/go.sum, cmd/, internal/, migrations/
├── docker-compose.yml      # postgres, redis, scheduler, prometheus
├── .env.example            # переменные окружения со значениями по умолчанию
├── .gitignore
├── README.md
├── prometheus/
│   └── prometheus.yml      # scrape scheduler:8080/metrics каждые 5s
├── cmd/
│   └── scheduler/          # точка входа (main, подкоманда tui)
├── internal/               # внутренние пакеты приложения
│   └── tui/                # bubbletea-клиент: таблица задач, retry, форма создания
└── migrations/             # SQL-миграции (встраиваются в бинарник через embed)
```

## Ограничения и что дальше

- **Нет exactly-once** — только at-least-once; повторы возможны (упавший воркер, таймаут), поэтому идемпотентность — обязанность обработчика.
- **Нет аутентификации** на API — держите сервис во внутреннем контуре (за прокси/firewall).
- **Reaper масштабируется горизонтально**: reclaim протухшей задачи — атомарный `UPDATE`, поэтому несколько инстансов reaper'а допустимы и не приводят к двойному возврату в очередь.
- `redis:7-alpine` в compose работает **без persistence**: Redis не хранит durable-состояние задач (очередь — в PostgreSQL), поэтому при рестарте Redis ничего не теряется. Если понадобится persistence — добавьте `volume` и флаги `--appendonly yes`.
- **Надёжность старта**: если Postgres недоступен на момент запуска, scheduler ретраит подключение до 30 секунд (попытки по ~3 с); при занятом HTTP-порту процесс завершается с ненулевым кодом выхода; по SIGTERM — graceful shutdown: in-flight задачи возвращаются в `pending` без расхода попытки.
- Идеи для развития: аутентификация API, recurring-задачи (cron-подобное расписание), приоритеты очереди, метрики по HTTP-вызовам наружу.

*Статус тестов/CI появится позже; команды для запуска — в разделе «Тесты».*
