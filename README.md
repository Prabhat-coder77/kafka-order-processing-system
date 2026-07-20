# Kafka Order Processing System

**Production-Ready Microservices with Apache Kafka & Avro Serialization**

[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.1-black.svg)](https://kafka.apache.org/)
[![Confluent](https://img.shields.io/badge/Confluent%20Platform-7.6.0-blue.svg)](https://www.confluent.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![Java](https://img.shields.io/badge/Java-17%20LTS-orange.svg)](https://openjdk.org/)
[![Avro](https://img.shields.io/badge/Apache%20Avro-1.11.3-red.svg)](https://avro.apache.org/)

## EG/2020/3990 -

## Table of Contents

- [Overview](#overview)
- [Dashboard Demo](#-dashboard-demo)
- [Quick Start](#quick-start)
- [System Architecture](#system-architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Deployment Options](#deployment-options)
- [API Documentation](#api-documentation)
- [Monitoring & Operations](#monitoring--operations)

## Overview

A **production-grade distributed order processing system** built with Apache Kafka, demonstrating enterprise-level event streaming patterns, fault tolerance, and microservices architecture.

### Key Capabilities

**High Availability** - 3-node Kafka cluster with RF=3, min ISR=2
**Schema Management** - Confluent Schema Registry with Avro serialization
**Fault Tolerance** - Automatic retry with exponential backoff + Dead Letter Queue
**Real-time Aggregation** - Thread-safe running average calculation
**Web Dashboard** - Modern UI for monitoring and order creation (Port 3000)
**Containerized** - Full Docker Compose orchestration with health checks
**Observable** - Kafka UI dashboard, structured logging, metrics endpoints
**Production-Ready** - Idempotent producers, manual commits, graceful shutdown

## Dashboard Demo

### Access the Interactive Dashboard

```
http://localhost:3000
```

**Features**:

- **Real-time statistics** - Total orders, running average, revenue, success rate
- **Order creation** - Interactive form with random data generation
- **Cluster monitoring** - Health status for all 9 containers
- **Service health** - Producer and consumer status with health checks
- **Order history** - Recent orders table with status tracking
- **Quick actions** - Batch orders, export data, manual refresh

**Quick Start**:

```bash
./infrastructure/scripts/start-dashboard.sh
# Opens browser automatically at http://localhost:3000
```

## Quick Start

### Prerequisites

- **Docker Desktop** (version 20.x or higher)
- **Docker Compose** (version 2.x or higher)
- **8GB RAM** minimum for Docker
- **macOS/Linux** (Windows with WSL2)

### One-Command Deployment

```bash
# Clone repository
git clone https://github.com/Prabhat-coder77/kafka-order-processing-system
cd Big-data-Assignment

# Start entire system (infrastructure + services)
./infrastructure/scripts/quick-start.sh
```

**That's it!** The script will:

1. Start ZooKeeper
2. Start 3 Kafka brokers
3. Start Schema Registry
4. Build & deploy Producer service
5. Build & deploy Consumer service
6. Start Kafka UI
7. Create topics (orders, orders-retry, orders-dlq)
8. Run health checks

**System Ready in ~60 seconds!**

### Verify Deployment

```bash
# Check all containers are healthy
docker ps

# Test Producer service
curl http://localhost:8090/actuator/health

# Test Consumer service
curl http://localhost:8082/actuator/health

# Access Kafka UI
open http://localhost:8080
```

### Send Your First Order

```bash
curl -X POST "http://localhost:8090/api/orders?orderId=DEMO001&product=Laptop&price=999.99"
```

**Check the result:**

```bash
curl http://localhost:8082/api/consumer/stats | python3 -m json.tool
```

## System Architecture

### High-Level Overview

```
┌─────────────────────┐         ┌──────────────────────────────────┐         ┌─────────────────────┐
│     Dashboard       │         │       Kafka Cluster (3 nodes)    │         │  Consumer Service   │
│    (Port 3000)      │         │  ┌────────┬────────┬──────────┐  │         │    (Port 8082)      │
│                     │    ┌───▶│  │ kafka1 │ kafka2 │  kafka3  │  │────────▶│                     │
│  Web Interface:     │    │    │  │ :9092  │ :9093  │  :9094   │  │         │  Processes:         │
│  • Order creation   │    │    │  └────────┴────────┴──────────┘  │         │  • Order validation │
│  • Statistics view  │    │    │           RF=3, min ISR=2        │         │  • Running average  │
│  • System monitor   │    │    │                                  │         │  • Retry handling   │
│  • Real-time updates│    │    │  Topics:                         │         │  • DLQ processing   │
└──────────┬──────────┘    │    │  • orders (3 partitions)         │         │  • Stats API        │
           │               │    │  • orders-retry (3 partitions)   │         └──────────┬──────────┘
           │               │    │  • orders-dlq (1 partition)      │                    │
           v               │    └──────────────────────────────────┘                    │
┌─────────────────────┐    │                  │                                         │
│  Producer Service   │────┘                  │                                         │
│    (Port 8090)      │                       │                                         │
│                     │                       v                                         │
│  REST API:          │         ┌──────────────────────────────────┐                    │
│  • POST /api/orders │         │         ZooKeeper                │              v     │
│  • Avro Serializer  │         │        (Port 2181)               │                    │
└──────────┬──────────┘         │                                  │                   │
           │                    │  • Cluster coordination          │                   │
           v                    │  • Leader election               │                   │
┌─────────────────────┐         │  • Broker metadata               │                   │
│  Schema Registry    │         └──────────────────────────────────┘                   │
│    (Port 8081)      │                       │                                         │
│                     │                       v                                         v
│  • Schema storage   │         ┌──────────────────────────────────┐         ┌─────────────────────┐
│  • Schema evolution │         │          Kafka UI                │◀────────│  Dashboard Stats    │
│  • Validation       │         │        (Port 8080)               │         │   GET /stats        │
└─────────────────────┘         │                                  │         │   (Port 8082)       │
                                │  • Visual monitoring             │         └─────────────────────┘
                                │  • Message browser               │
                                │  • Consumer groups               │
                                └──────────────────────────────────┘

                                     9 Containers Total
```

### Message Flow

**1. Normal Flow (Happy Path)**

```
Client → Producer API → Avro Serialization → Kafka Topic → Consumer → Process → Running Average 
```

**2. Retry Flow (Temporary Failure)**

```
Consumer → Processing Error → orders-retry topic → Exponential Backoff (2s, 4s, 8s) → Retry → Success 
```

**3. DLQ Flow (Permanent Failure)**

```
Consumer → 3 Failed Retries → orders-dlq topic → Manual Investigation → Fix & Reprocess 🔧
```

### Container Architecture


| Container            | Image                                 | Port | Purpose                  |
| -------------------- | ------------------------------------- | ---- | ------------------------ |
| **zookeeper**        | confluentinc/cp-zookeeper:7.6.0       | 2181 | Cluster coordination     |
| **kafka1**           | confluentinc/cp-kafka:7.6.0           | 9092 | Kafka broker #1          |
| **kafka2**           | confluentinc/cp-kafka:7.6.0           | 9093 | Kafka broker #2          |
| **kafka3**           | confluentinc/cp-kafka:7.6.0           | 9094 | Kafka broker #3          |
| **schema-registry**  | confluentinc/cp-schema-registry:7.6.0 | 8081 | Avro schema management   |
| **kafka-ui**         | provectuslabs/kafka-ui:latest         | 8080 | Visual monitoring        |
| **producer-service** | Custom (Spring Boot 3.3.5)            | 8090 | Order creation API       |
| **consumer-service** | Custom (Spring Boot 3.3.5)            | 8082 | Order processing + stats |

---

## Features

### 1. High Availability

- **3-node Kafka cluster** with replication factor 3
- **Automatic failover** - survives single broker failure
- **No single point of failure** - all data replicated

### 2. Schema Management

- **Avro binary serialization** - 50% smaller than JSON
- **Schema Registry** - centralized schema versioning
- **Schema evolution** - backward/forward compatibility

### 3. Fault Tolerance

- **Retry mechanism** - automatic retry with exponential backoff
- **Dead Letter Queue** - preserve failed messages for investigation
- **Manual commits** - at-least-once delivery guarantee
- **Idempotent producer** - exactly-once semantics

### 4. Real-time Processing

- **Running average calculation** - thread-safe aggregation
- **Parallel processing** - 3 partitions for throughput
- **Low latency** - sub-second processing times

### 5. Observability

- **Kafka UI** - visual dashboard for monitoring
- **Health checks** - actuator endpoints for all services
- **Structured logging** - detailed processing logs
- **Consumer metrics** - lag, throughput, success rate

### 6. Production-Ready

- **Containerized** - Docker Compose orchestration
- **Health checks** - all containers monitored
- **Graceful shutdown** - proper cleanup on stop
- **Resource optimized** - multi-stage Dockerfiles

---

## Technology Stack

### Core Technologies


| Category           | Technology         | Version | Why?                                  |
| ------------------ | ------------------ | ------- | ------------------------------------- |
| **Message Broker** | Apache Kafka       | 3.7.1   | Industry standard for event streaming |
| **Platform**       | Confluent Platform | 7.6.0   | Enterprise Kafka distribution         |
| **Serialization**  | Apache Avro        | 1.11.3  | Binary format, schema evolution       |
| **Framework**      | Spring Boot        | 3.3.5   | Rapid microservices development       |
| **Language**       | Java               | 17 LTS  | Long-term support, modern features    |
| **Build Tool**     | Maven              | 3.9.6   | Dependency management, plugins        |
| **Container**      | Docker             | 24.x    | Consistent deployment environment     |
| **Orchestration**  | Docker Compose     | 2.x     | Multi-container management            |
| **Monitoring**     | Kafka UI           | Latest  | Visual monitoring and debugging       |

### Key Libraries

- **spring-kafka** 3.2.4 - Kafka integration
- **confluent-kafka-avro-serializer** 7.6.0 - Avro serialization
- **avro-maven-plugin** 1.11.3 - Code generation from schemas
- **spring-boot-starter-actuator** 3.3.5 - Health checks and metrics
- **lombok** 1.18.30 - Boilerplate reduction

---

## Project Structure

```
Big-data-Assignment/
│
├── producer-service/              # Order creation microservice
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/com/pramithamj/kafka/
│   │   │   │   ├── ProducerServiceApplication.java
│   │   │   │   ├── config/
│   │   │   │   │   └── KafkaProducerConfig.java      # Kafka producer config
│   │   │   │   ├── controller/
│   │   │   │   │   └── OrderController.java          # REST API endpoints
│   │   │   │   └── producer/
│   │   │   │       └── OrderProducer.java            # Kafka message sender
│   │   │   └── resources/
│   │   │       ├── application.properties            # Local config
│   │   │       ├── application-docker.properties     # Docker config
│   │   │       └── avro/
│   │   │           └── order.avsc                    # Avro schema
│   │   └── test/
│   ├── Dockerfile                                     # Multi-stage build
│   ├── .dockerignore                                  # Build optimization
│   └── pom.xml                                        # Maven dependencies
│
├── consumer-service/              # Order processing microservice
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/com/pramithamj/kafka/
│   │   │   │   ├── ConsumerServiceApplication.java
│   │   │   │   ├── aggregation/
│   │   │   │   │   └── RunningAverageCalculator.java # Thread-safe aggregation
│   │   │   │   ├── config/
│   │   │   │   │   ├── KafkaConsumerConfig.java      # Consumer config
│   │   │   │   │   └── KafkaProducerConfig.java      # For retry/DLQ
│   │   │   │   ├── consumer/
│   │   │   │   │   └── OrderConsumer.java            # Message listeners
│   │   │   │   ├── controller/
│   │   │   │   │   └── ConsumerController.java       # Stats API
│   │   │   │   ├── retry/
│   │   │   │   │   └── RetryHandler.java             # Exponential backoff
│   │   │   │   └── dlq/
│   │   │   │       └── DLQHandler.java               # Dead letter queue
│   │   │   └── resources/
│   │   │       ├── application.properties
│   │   │       ├── application-docker.properties
│   │   │       └── avro/
│   │   │           └── order.avsc
│   │   └── test/
│   ├── Dockerfile
│   ├── .dockerignore
│   └── pom.xml
│
├── infrastructure/                # Infrastructure as code
│   ├── docker/
│   │   ├── docker-compose.yml                        # 9 containers orchestration
│   │   ├── .env                                      # Environment variables
│   │   └── .env.example                              # Template
│   └── scripts/
│       ├── quick-start.sh                            # One-command deployment
│       ├── start-dashboard.sh                        # Dashboard launcher
│       ├── create-topics.sh                          # Kafka topic creation
│       ├── seed-data.sh                              # Test data generation
│       └── check-cluster.sh                          # Health check script
│
├── dashboard/                     # Web Dashboard (Port 3000)
│   ├── index.html                                    # Main UI
│   ├── styles.css                                    # Styling
│   ├── app.js                                        # Frontend logic
│   ├── nginx.conf                                    # Nginx config
│   ├── Dockerfile                                    # Container build
│   └── README.md                                     # Dashboard docs
│
├── docs/                          # Comprehensive documentation
│   ├── ARCHITECTURE.md                               # System architecture
│   ├── MANUAL-STARTUP-DEMO-GUIDE.md                  # Step-by-step guide
│   └── DASHBOARD-DEMO-GUIDE.md                       # Dashboard demo scenarios
│
└── README.md                      # This file
```

---

## Deployment Options

### Option 1: Quick Start

**One command to deploy everything:**

```bash
./infrastructure/scripts/quick-start.sh
```

### Option 2: Step-by-Step Deployment

**Step 1: Start infrastructure**

```bash
cd infrastructure/docker
docker compose up -d zookeeper kafka1 kafka2 kafka3 schema-registry kafka-ui
```

**Step 2: Create topics**

```bash
cd ../scripts
./create-topics.sh
```

**Step 3: Build and deploy services**

```bash
cd ../../
docker compose -f infrastructure/docker/docker-compose.yml up -d producer-service consumer-service
```

## API Documentation

### Producer Service (Port 8090)

#### Send Single Order

```bash
POST http://localhost:8090/api/orders

Query Parameters:
- orderId: string (required) - Unique order identifier
- product: string (required) - Product name
- price: double (required) - Order amount

Example:
curl -X POST "http://localhost:8090/api/orders?orderId=ORD001&product=Laptop&price=999.99"

Response:
{
  "orderId": "2658",
  "success": true,
  "message": "Order sent to Kafka successfully"
}
```

#### Health Check

```bash
GET http://localhost:8090/actuator/health

Response:
{"status":"UP"}
```

### Consumer Service (Port 8082)

#### Get Statistics

```bash
GET http://localhost:8082/api/consumer/stats

Response:
{
    "ordersProcessed": 25,
    "totalAmount": 4567.89,
    "runningAverage": 182.72,
    "detailedStats": "Processed: 25 | Errors: 1 | Success Rate: 96.00% | Total Amount: $4567.89 | Running Average: $182.72"
}
```

#### Health Check

```bash
GET http://localhost:8082/actuator/health

Response:
{"status":"UP"}
```

### Schema Registry (Port 8081)

#### List Schemas

```bash
GET http://localhost:8081/subjects

Response:
["orders-value"]
```

#### Get Schema Details

```bash
GET http://localhost:8081/subjects/orders-value/versions/1

Response:
{
  "subject": "orders-value",
  "version": 1,
  "id": 1,
  "schema": "{...}"
}
```

---

## Monitoring & Operations

### Web Dashboard

**Access:** http://localhost:3000

**Features:**

- ** Real-time Statistics** - Total orders, running average, revenue, success rate
- **Order Management** - Create single orders, random orders, or batch of 10
- **System Monitoring** - All 9 containers health status
- **Service Health** - Producer & consumer service checks
- **Order History** - Recent orders table with status tracking
- **Quick Actions** - Refresh, clear stats, export data

**Kafka UI Dashboard**

**Access:** http://localhost:8080

**Features:**

- **Brokers** - Health status of all 3 Kafka nodes
- **Topics** - Message counts, partitions, replication
- **Messages** - Browse and inspect individual messages
- **Consumers** - Consumer groups, lag, assignments
- **Schemas** - Avro schema registry visualization

### Health Checks

```bash
# Check all containers
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Check specific service
docker inspect producer-service --format='{{json .State.Health}}' | python3 -m json.tool

# Check Kafka cluster
./infrastructure/scripts/check-cluster.sh
```

### Logs

```bash
# View producer logs
docker logs producer-service --tail 50 --follow

# View consumer logs
docker logs consumer-service --tail 50 --follow

# View Kafka broker logs
docker logs kafka1 --tail 50 --follow

# View only order processing messages
docker logs consumer-service 2>&1 | grep "Received order"
```

### Metrics

```bash
# Consumer statistics
curl http://localhost:8082/api/consumer/stats | python3 -m json.tool

# Topic details
docker exec kafka1 kafka-topics --describe --bootstrap-server kafka1:19092 --topic orders

# Consumer group lag
docker exec kafka1 kafka-consumer-groups --describe \
  --bootstrap-server kafka1:19092 --group order-consumer-group
```

---

### Avro Schema

**File:** `producer-service/src/main/resources/avro/order.avsc`

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.pramithamj.kafka.model",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
```

## 👥 Authors

**Pramitha M.J.**

- GitHub: [@Prabhat-coder77](https://github.com/Prabhat-coder77)

## Quick Command Reference

```bash
# Start system
./infrastructure/scripts/quick-start.sh

# Stop system
docker compose -f infrastructure/docker/docker-compose.yml down

# Send order
curl -X POST "http://localhost:8090/api/orders?orderId=TEST&product=Item&price=99.99"

# Check stats
curl http://localhost:8082/api/consumer/stats | python3 -m json.tool

# View Kafka UI
open http://localhost:8080

# View logs
docker logs consumer-service --tail 50 --follow

# Health check
docker ps --format "table {{.Names}}\t{{.Status}}"

# Clean restart
docker compose -f infrastructure/docker/docker-compose.yml down -v && \
./infrastructure/scripts/quick-start.sh
```
