# Black Friday Emulator

A Spring Boot app that simulates a Black Friday flash sale. A crowd of virtual shoppers rushes a store that has limited stock, and a **virtual waiting room** queues them and admits them at a controlled rate. A live dashboard shows the queue building up, shoppers giving up, stock selling out and events flowing through Kafka.

## Tech stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 4.1 (Spring MVC, Spring Data JPA, Spring for Apache Kafka, scheduling) |
| Concurrency | Java virtual threads (one per simulated shopper, plus request handling) |
| Database | PostgreSQL 17 with Hibernate ORM 7 |
| Messaging | Apache Kafka 4 (KRaft mode, no ZooKeeper) |
| JSON | Jackson 3 |
| Boilerplate | Lombok |
| Frontend | Plain HTML, CSS and JavaScript with a canvas chart (no build step) |
| Build | Maven (wrapper included) |
| Local infra | Docker Compose |

## How it works

```
 virtual shoppers ──► waiting room (FIFO queue) ──► store ──► Postgres (orders, stock)
                         │ admits N/sec,              │
                         │ up to store capacity       │
                         └──────── every event ───────┴──► Kafka ──► dashboard counters
```

- **Shoppers** are virtual threads. Each one arrives at a random time during the ramp-up and joins the queue. A shopper gives up if it waits longer than its patience, which varies from 0.5× to 1.5× the configured value. Once admitted, it browses for 0.3–2 s and buys with the configured probability. If its first choice is sold out, it tries up to two other products. Bigger deals are picked more often.
- **Waiting room** (`waitingroom/`): an in-memory FIFO queue. Every 100 ms a scheduler admits shoppers at the configured rate, but only while the store is below capacity. Admitted sessions expire after `session-seconds`. The queue position and estimated wait come from sequence numbers.
- **Store** (`store/`): products and orders are stored in PostgreSQL. A purchase takes stock with a single conditional `UPDATE ... SET stock = stock - 1 WHERE stock > 0`, so concurrent buyers can never oversell. Only admitted shoppers can buy.
- **Events** (`events/`): every event (joined, admitted, abandoned, expired, purchased, sold out, left store) is published as JSON to the `black-friday-events` topic, with the event type as the record key. A `@KafkaListener` consumes the stream and counts events by type.
- **Dashboard** (`static/index.html`): polls `/api/stats` every second.

Prices are in **South African rand** (stored as cents, R1 = 100).

## Running

Requirements: a JDK 21 or newer (with `javac`, not just a JRE) and Docker.

```bash
docker compose up -d        # Postgres on :5433, Kafka on :9092
./mvnw spring-boot:run      # app on :8081
```

Open **http://localhost:8081**, choose the crowd size and click **Open the doors**.

Stop the app with **Ctrl+C**. Ctrl+Z only pauses it and leaves port 8081 in use. Stop the infrastructure with `docker compose down`, or `docker compose down -v` to also delete the database.

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

The three `blackfriday.*` settings can also be changed while a run is in progress, from the dashboard or with `PUT /api/settings`.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/queue` | Join the waiting room, returns a ticket |
| `GET` | `/api/queue/{ticketId}` | Ticket status, position and estimated wait in seconds |
| `DELETE` | `/api/queue/{ticketId}` | Leave the queue or the store |
| `GET` | `/api/products` | Catalogue with current stock |
| `POST` | `/api/store/{ticketId}/buy/{productId}` | Buy one unit: `200` purchased, `409` sold out, `403` not admitted |
| `POST` | `/api/simulation` | Start a simulated crowd |
| `DELETE` | `/api/simulation` | Stop the simulation |
| `PUT` | `/api/settings` | Change admission rate, capacity and session length |
| `POST` | `/api/reset` | Stop, clear the queue, delete orders and restock |
| `GET` | `/api/stats` | Everything the dashboard shows |

Example:

```bash
curl -X POST localhost:8081/api/simulation -H 'Content-Type: application/json' \
  -d '{"shoppers":5000,"rampUpSeconds":30,"buyProbability":0.7,"patienceSeconds":90}'
```

## Things to try

- **Throttle hard:** set the admission rate to 5/s and watch the queue grow and patience run out.
- **Open the floodgates:** set the rate to 1000/s and the capacity to 5000, and the doorbuster sells out in seconds.
- **Short sessions:** set the session to 2 s and admitted shoppers time out before they can buy.

## Limitations

- The queue, tickets and counters live in memory in a single instance, so a restart clears them. Orders and stock persist in Postgres.
- This is a simulator, so there is no authentication or payment.
