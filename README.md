# Event-Driven Commerce Platform

A production-style distributed commerce backend built with Java and Spring Boot.

This project is designed to demonstrate backend engineering, event-driven architecture, distributed systems, database design, containerization, observability, and cloud deployment.

## Current Architecture

- Order Service
- PostgreSQL
- Docker Compose
- Spring Boot Actuator

## Planned Services

- Order Service
- Inventory Service
- Payment Service
- Notification Service

## Planned Infrastructure

- Apache Kafka
- Redis
- PostgreSQL
- Docker
- AWS
- GitHub Actions CI/CD

## Engineering Goals

This project will include:

- Event-driven communication between services
- Transactional outbox pattern
- Saga-style workflows
- Idempotent event consumers
- Retry and dead-letter handling
- Distributed caching
- Integration testing
- Observability and metrics
- Load testing
- Failure-recovery scenarios
- Cloud deployment

## Tech Stack

- Java 21
- Spring Boot
- PostgreSQL
- Docker
- Maven

More components will be added as the system evolves.

## Local Development

### Requirements

- Java 21
- Docker Desktop
- Git

### Start PostgreSQL

```bash
docker compose up -d postgres
```

### Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Then provide your local development values in `.env`.

### Run Order Service

```bash
cd services/order-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

### Health Check

In another terminal:

```bash
curl http://localhost:8080/actuator/health
```

Expected response:

```json
{
  "status": "UP"
}
```

## Project Status

In Development
