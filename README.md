# Black Friday Emulator

A Spring Boot app that simulates a Black Friday flash sale. A crowd of virtual shoppers rushes a store that has limited stock. A **virtual waiting room backed by Kafka** lines them up and lets them in at a controlled rate. A live dashboard shows the line growing, shoppers giving up, stock selling out and events flowing through Kafka.

Prices are in **South African rand**.

## Quick start

Requirements: JDK 21 or newer (with `javac`, not just a JRE) and Docker.

```bash
docker compose up -d        # Postgres on :5433, Kafka on :9092
./mvnw spring-boot:run      # app on :8081
```

Open **http://localhost:8081**, choose the crowd size and click **Open the doors**.

Stop the app with **Ctrl+C**. Ctrl+Z only pauses it and leaves port 8081 in use. `docker compose down` stops the infrastructure and also wipes the Kafka line. Add `-v` to delete the database too.

## Tech stack and why

| Technology | Used for | Why this choice |
|---|---|---|
| **Java 21** | Language | LTS release. It has records for small immutable types (tickets, settings, events) and virtual threads. |
| **Spring Boot 4.1** | App framework | Auto-configures the web server, database, Kafka client and JSON, so the code is almost all business logic. Most Java backend teams know it. |
| **Spring MVC** | REST API and serving the dashboard | Simple blocking request handling. With virtual threads it scales without the complexity of reactive code. |
| **Java virtual threads** | One thread per simulated shopper, plus web requests | Thousands of shoppers spend most of their time sleeping or waiting. Virtual threads make one thread per shopper cheap, so each shopper reads as a simple top-to-bottom script. |
| **Apache Kafka 4 (KRaft)** | The waiting-room line, and the event stream | A log with strict order inside a partition is exactly a first-come, first-served line. Offsets give each shopper a position, and committed offsets record how far the line has moved, which survives restarts. KRaft mode means a single container, with no ZooKeeper. |
| **Spring for Apache Kafka** | Producer, `@KafkaListener` and topic creation | `KafkaTemplate` and listeners remove the client boilerplate. `NewTopic` beans create the topics on startup. |
| **PostgreSQL 17** | Products, stock and orders | Money and stock need ACID transactions. One conditional `UPDATE ... WHERE stock > 0` makes overselling impossible, even with hundreds of concurrent buyers. |
| **Spring Data JPA / Hibernate 7** | Database access | Repositories cover the CRUD. The single atomic stock update is one JPQL query. |
| **HikariCP** | Connection pool | Spring Boot's default and very fast. The pool is set to 20 connections so a buying spike queues on the pool instead of overwhelming Postgres. |
| **Jackson 3** | JSON for the API and Kafka messages | Built into Spring Boot 4 and serialises Java records directly. |
| **Lombok** | Constructors, getters and loggers | Less boilerplate on entities and services. |
| **Plain HTML, CSS and JS (canvas chart)** | Dashboard | One static file served by Spring, with no Node toolchain or build step. Polling once a second is enough for a live view. |
| **Docker Compose** | Local Postgres and Kafka | One command starts the infrastructure, and the versions are pinned. |
| **Maven wrapper** | Build | `./mvnw` downloads the right Maven version, so the build is the same on any machine. |

## How it works

```
                 join = produce                 gatekeeper consumer
 virtual shoppers ────────► Kafka topic ───────► admits N/sec, up to ───► store ──► Postgres
                            waiting-room-queue   store capacity,                    (orders, stock)
                            (1 partition = FIFO) commits offset on admit
                                                                         │
                every event (joined, admitted, purchased, ...) ──────────┴──► Kafka topic
                                                                              black-friday-events
                                                                              ──► dashboard counters
```

1. **Join.** A shopper's ticket is written to the Kafka topic `waiting-room-queue` (key = ticket ID), and the app waits for Kafka to acknowledge it. The record's offset is the shopper's place in line. The topic has exactly **one partition**, because Kafka only guarantees ordering within a partition.
2. **Wait.** The shopper's position is `ticket offset − head offset + 1`, and the estimated wait is position ÷ admission rate. Simulated shoppers give up if they wait longer than their patience, which varies from 0.5× to 1.5× the configured value.
3. **Admit.** A single gatekeeper consumer (`KafkaWaitingQueue`) reads from the head of the line every 100 ms. It lets in as many shoppers as the rate budget and free store capacity allow, and skips anyone who gave up. It commits the offset only once a shopper is handled, so the committed offset is how far the line has moved, and a restart resumes from it.
4. **Shop.** Admitted shoppers browse for 0.3–2 s and buy with the configured probability. If their first choice is sold out, they try up to two other products. Bigger deals are picked more often. Sessions expire after `session-seconds`.
5. **Buy.** `StoreService` takes stock with one conditional `UPDATE` and saves the order in Postgres. Only admitted shoppers can buy.
6. **Observe.** Every step publishes an event to `black-friday-events`. A `@KafkaListener` reads them back and counts them, and the dashboard polls `/api/stats` every second.

## Project structure

```
src/main/java/com/example/waiting_room/
├── WaitingRoomApplication.java    entry point
├── config/
│   ├── BlackFridayProperties      typed blackfriday.* settings
│   └── KafkaTopics                topic names and NewTopic beans
├── waitingroom/                   the Kafka-backed virtual waiting room
│   ├── KafkaWaitingQueue          produce to join; gatekeeper consumer admits from the head
│   ├── WaitingRoomService         ticket registry, admission budget, store occupancy
│   ├── Ticket, TicketStatus       a shopper's place in line and its state
│   ├── WaitingRoomSettings        rate, capacity and session length (validated)
│   └── QueueUnavailableException  Kafka did not accept a ticket (HTTP 503)
├── store/                         the flash-sale shop
│   ├── Product, PurchaseOrder     JPA entities
│   ├── ProductRepository          includes the atomic "take one" stock update
│   ├── PurchaseOrderRepository
│   ├── StoreService, PurchaseResult
│   └── ProductSeeder              seeds and updates the catalogue on startup
├── events/                        Kafka event stream
│   ├── EventPublisher             fire-and-forget publish of ShopEvent
│   ├── EventStatsListener         consumes and counts events by type
│   └── ShopEvent, EventType
├── simulation/                    the crowd
│   ├── SimulationService          one virtual thread per shopper
│   └── SimulationRequest          crowd size, ramp-up, buy chance, patience (validated)
├── dashboard/                     read model for the UI
│   ├── DashboardService           builds the snapshot; resets every module
│   ├── DashboardSnapshot          GET /api/stats response
│   └── StatsHistory               one-second samples for the chart
└── web/                           HTTP layer only, no business logic
    ├── QueueController            /api/queue
    ├── StoreController            /api/products, /api/store
    ├── EmulatorController         /api/simulation, /api/settings, /api/reset, /api/stats
    ├── TicketView                 ticket response DTO
    └── ApiExceptionHandler        errors → RFC 9457 problem responses

src/main/resources/
├── application.properties
└── static/index.html              the dashboard
compose.yaml                       Postgres 17 + Kafka 4.3.1
```

Dependencies point inwards. `web` calls the services, `dashboard` reads from every module, and `waitingroom`, `store` and `simulation` know nothing about HTTP.

## Configuration

`src/main/resources/application.properties`. Every value can be overridden with an environment variable:

| Property | Env var | Default |
|---|---|---|
| `server.port` | `PORT` | `8081` |
| `spring.datasource.url` | `DB_URL` | `jdbc:postgresql://localhost:5433/waiting_room` |
| `spring.datasource.username` / `password` | `DB_USERNAME` / `DB_PASSWORD` | `waiting_room` |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `blackfriday.admission-rate-per-second` | | `20` |
| `blackfriday.store-capacity` | | `200` |
| `blackfriday.session-seconds` | | `30` |

The `blackfriday.*` values are startup defaults. They can be changed while a run is in progress, from the dashboard or with `PUT /api/settings`.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/queue` | Join the waiting room, returns a ticket (`503` if Kafka is unavailable) |
| `GET` | `/api/queue/{ticketId}` | Ticket status, Kafka offset, position and estimated wait in seconds |
| `DELETE` | `/api/queue/{ticketId}` | Leave the line, or the store if admitted |
| `GET` | `/api/products` | Catalogue with current stock |
| `POST` | `/api/store/{ticketId}/buy/{productId}` | Buy one unit: `200` purchased, `409` sold out, `403` not admitted |
| `POST` | `/api/simulation` | Start a simulated crowd (`409` if one is already running) |
| `DELETE` | `/api/simulation` | Stop the simulation |
| `PUT` | `/api/settings` | Change admission rate, capacity and session length |
| `POST` | `/api/reset` | Stop, skip everyone in the line, delete orders and restock |
| `GET` | `/api/stats` | Everything the dashboard shows |

Invalid input returns `400` with an RFC 9457 problem body that explains what is wrong.

```bash
curl -X POST localhost:8081/api/simulation -H 'Content-Type: application/json' \
  -d '{"shoppers":5000,"rampUpSeconds":30,"buyProbability":0.7,"patienceSeconds":90}'
```

## Things to try

- **Throttle hard:** set the admission rate to 5/s and watch the line grow and patience run out.
- **Open the floodgates:** set the rate to 1000/s and the capacity to 5000, and the doorbuster sells out in seconds.
- **Short sessions:** set the session to 2 s and admitted shoppers time out before they can buy.

## Limitations

- The line and its progress are stored in Kafka, and orders and stock persist in Postgres. Ticket status (waiting, admitted, gave up) and the counters are kept in memory so they are fast to look up. After a restart, the gatekeeper resumes at the committed offset, but tickets from before the restart are unknown to the new process and get skipped.
- Only one gatekeeper can run at a time, because a single consumer owns the single partition. That is what keeps the line in order.
- This is a simulator, so there is no authentication or payment.
