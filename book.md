# Zero to Hero Kafka with NestJS
### Build Real-Time Event-Driven Microservices from Scratch to Production

**Author:** Your Name  
**Version:** 1.0  
**Audience:** Backend engineers, NestJS developers, microservices teams  
**Prerequisites:** TypeScript, NestJS basics, Docker basics

---

## Table of Contents

1. [Introduction](#1-introduction)  
2. [Kafka Fundamentals](#2-kafka-fundamentals)  
3. [NestJS + Kafka Foundations](#3-nestjs--kafka-foundations)  
4. [Local Environment Setup](#4-local-environment-setup)  
5. [First Producer in NestJS](#5-first-producer-in-nestjs)  
6. [First Consumer in NestJS](#6-first-consumer-in-nestjs)  
7. [MessagePattern vs EventPattern](#7-messagepattern-vs-eventpattern)  
8. [Offsets and Manual Commit](#8-offsets-and-manual-commit)  
9. [Retries, Backoff, and Dead Letter Topics](#9-retries-backoff-and-dead-letter-topics)  
10. [Idempotency and Duplicate Handling](#10-idempotency-and-duplicate-handling)  
11. [Schema Design and Versioning](#11-schema-design-and-versioning)  
12. [Observability and Operations](#12-observability-and-operations)  
13. [Security Essentials](#13-security-essentials)  
14. [Performance and Scaling](#14-performance-and-scaling)  
15. [Testing Strategy](#15-testing-strategy)  
16. [Real-Life Event-Driven Microservices Architecture](#16-real-life-event-driven-microservices-architecture)  
17. [Hands-On Workflow: End-to-End Order Lifecycle](#17-hands-on-workflow-end-to-end-order-lifecycle)  
18. [Troubleshooting Playbook](#18-troubleshooting-playbook)  
19. [Production Readiness Checklist](#19-production-readiness-checklist)  
20. [Interview Questions (Beginner → Advanced)](#20-interview-questions-beginner--advanced)  
21. [Appendix: Useful Commands](#21-appendix-useful-commands)

---

## 1. Introduction

Kafka + NestJS is a powerful combination for building **event-driven microservices** that are decoupled, scalable, and resilient.

### Why event-driven communication?
- Loose coupling between services
- Better scalability (independent consumers)
- Better resilience (retry/replay)
- Clear audit trail (event log)

### Typical use cases
- Order and payment workflows
- Notifications
- Fraud/risk analysis
- Audit events
- Data pipelines/analytics
- CDC (Change Data Capture) integrations

---

## 2. Kafka Fundamentals

### Core concepts
- **Broker**: Kafka server node
- **Topic**: category/stream of events
- **Partition**: ordered shard of topic
- **Offset**: position within partition
- **Producer**: writes events
- **Consumer**: reads events
- **Consumer Group**: set of consumers sharing topic partitions
- **Retention**: how long Kafka keeps data
- **Replication**: fault tolerance across brokers

### Delivery semantics
- **At-most-once**: may lose messages
- **At-least-once**: may process duplicates
- **Exactly-once** (Kafka-level): complex, limited scope; application idempotency still required

---

## 3. NestJS + Kafka Foundations

NestJS provides transport abstractions and clean architecture:
- `ClientsModule` for producers
- Kafka microservice transport for consumers
- Decorators:
  - `@EventPattern` for event streaming (`emit`)
  - `@MessagePattern` for request-response (`send`)

### Recommended separation
- **Producer app**: HTTP + Kafka producer
- **Consumer app**: Kafka-only worker service

---

## 4. Local Environment Setup

## 4.1 Docker Compose (Kafka + Kafka UI, KRaft mode)

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_DELETE_TOPIC_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:19092
      DYNAMIC_CONFIG_ENABLED: "true"
Start
docker compose down -v --remove-orphans
docker compose up -d
UI: http://localhost:8080

5. First Producer in NestJS
5.1 Producer module registration
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { ProducerController } from './producer.controller';
import { ProducerService } from './producer.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'producer-app',
            brokers: ['localhost:9092'],
          },
          producer: {
            allowAutoTopicCreation: true,
          },
          consumer: {
            groupId: 'producer-app-client-v1',
          },
        },
      },
    ]),
  ],
  controllers: [ProducerController],
  providers: [ProducerService],
})
export class ProducerModule {}
5.2 Producer service
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Injectable()
export class ProducerService implements OnModuleInit {
  constructor(@Inject('KAFKA_SERVICE') private readonly kafka: ClientKafka) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

  async publish(topic: string, payload: any) {
    await this.kafka.emit(topic, payload);
  }
}
5.3 Producer controller
import { Body, Controller, Post } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { ProducerService } from './producer.service';

@Controller('orders')
export class ProducerController {
  constructor(private readonly producer: ProducerService) {}

  @Post('create')
  async createOrder(@Body() body: any) {
    const event = {
      eventId: randomUUID(),
      eventType: 'order.created',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      data: body,
    };

    await this.producer.publish('order.created', event);
    return { status: 'accepted', eventId: event.eventId };
  }
}
6. First Consumer in NestJS
6.1 Consumer bootstrap (main.ts)
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { ConsumerModule } from './consumer.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    ConsumerModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'consumer-app',
          brokers: ['localhost:9092'],
        },
        consumer: {
          groupId: 'consumer-app-v1',
          sessionTimeout: 30000,
          heartbeatInterval: 3000,
        },
        run: {
          autoCommit: true,
        },
      },
    },
  );

  await app.listen();
  console.log('Kafka consumer running');
}
bootstrap();
6.2 Consumer handler
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload, Ctx, KafkaContext } from '@nestjs/microservices';

@Controller()
export class ConsumerController {
  private readonly logger = new Logger(ConsumerController.name);

  @EventPattern('order.created')
  async onOrderCreated(@Payload() payload: any, @Ctx() ctx: KafkaContext) {
    const msg = ctx.getMessage();
    this.logger.log(
      `topic=${ctx.getTopic()} partition=${ctx.getPartition()} offset=${msg.offset} payload=${JSON.stringify(payload)}`
    );
  }
}
7. MessagePattern vs EventPattern
emit + @EventPattern → event-driven, fire-and-forget

send + @MessagePattern → request-response semantics

For microservices event communication, prefer EventPattern.

8. Offsets and Manual Commit
Manual commit is useful when you want stronger control over “process successfully then commit.”

8.1 Consumer run config
run: {
  autoCommit: false,
}
8.2 Manual commit in handler
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Ctx, KafkaContext, Payload } from '@nestjs/microservices';

@Controller()
export class ConsumerController {
  private readonly logger = new Logger(ConsumerController.name);

  @EventPattern('order.created')
  async handle(@Payload() payload: any, @Ctx() ctx: KafkaContext) {
    const consumer = ctx.getConsumer();
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const partition = ctx.getPartition();

    try {
      // Business logic
      this.logger.log(`Processing eventId=${payload?.eventId}`);

      // Commit NEXT offset after success
      const nextOffset = String(Number(message.offset) + 1);
      await consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);

      this.logger.log(`Committed offset=${nextOffset}`);
    } catch (err: any) {
      this.logger.error(`Processing failed, no commit. err=${err?.message}`);
      // No commit -> message may be redelivered
    }
  }
}
9. Retries, Backoff, and Dead Letter Topics
Design
Main topic: order.created

Retry topic(s): order.created.retry.1, order.created.retry.2

Dead-letter topic: order.created.dlt

Retry metadata headers
x-retry-count

x-original-topic

x-failure-reason

x-first-failure-at

Backoff strategy
Retry 1 after 10s

Retry 2 after 60s

Then DLT

10. Idempotency and Duplicate Handling
At-least-once implies duplicates can occur.
Always design consumers to be idempotent.

Patterns
Event ID dedupe table in DB

Unique constraint on natural business key

Redis SETNX with TTL per event ID

Upsert semantics rather than insert-only

Example dedupe schema
event_id (PK / unique)

processed_at

consumer_name

11. Schema Design and Versioning
Recommended envelope
{
  "eventId": "uuid",
  "eventType": "order.created",
  "eventVersion": 1,
  "occurredAt": "2026-02-12T10:00:00.000Z",
  "traceId": "abc-123",
  "source": "order-service",
  "data": {}
}
Versioning rules
Add optional fields, avoid removing required fields

Maintain backward compatibility

Introduce eventVersion changes carefully

Consumer should handle known versions safely

12. Observability and Operations
Logs
Log these on each message:

topic, partition, offset

groupId

eventId/traceId

processing time

Metrics
messages/sec

error rate

consumer lag

rebalance count

DLT rate

Alerts
lag above threshold

repeated rebalance spikes

DLT volume anomalies

consumer downtime

13. Security Essentials
Use TLS in transit

Use SASL/SCRAM or IAM-style auth

Apply ACLs per service principal

Rotate credentials

Never embed secrets in source code

14. Performance and Scaling
Scale levers
More partitions for higher parallelism

More consumer instances in same group

Tune producer batch/linger/compression

Tune consumer fetch sizes

Practical rule
Max parallel consumers in a group ≈ partition count.

15. Testing Strategy
Unit tests
handler business logic

retry policy

idempotency checks

Integration tests
real Kafka via Docker test container

publish + consume + assert side effects

End-to-end tests
API call → event published → downstream processing

Contract tests
Validate payload shape/version compatibility

16. Real-Life Event-Driven Microservices Architecture
16.1 Example: E-commerce Order Platform
Services
API Gateway / BFF

Order Service

Payment Service

Inventory Service

Shipping Service

Notification Service

Fraud Service

Analytics Service

Customer Profile Service

Core topics
order.created

payment.authorized

payment.failed

inventory.reserved

inventory.failed

shipment.created

shipment.delivered

notification.send

order.completed

order.cancelled

order.failed

*.dlt

16.2 Communication flow (event choreography)
Client places order → Order Service stores order, publishes order.created.

Payment Service consumes order.created, tries authorization.

Success → publishes payment.authorized

Failure → publishes payment.failed

Inventory Service consumes payment.authorized, reserves stock.

Success → inventory.reserved

Failure → inventory.failed

Shipping Service consumes inventory.reserved, creates shipment → shipment.created.

Notification Service consumes events and sends email/SMS/push.

Order Service aggregates outcomes:

all success → order.completed

any failure → order.failed + compensation events.

16.3 Compensation (Saga-style)
If payment succeeds but inventory fails:

publish payment.refund.requested

update order state to FAILED

publish order.failed

Why this works
Each service owns its DB

No central synchronous locking

Failures are recoverable via compensating events

16.4 Real-world best practices
Use outbox pattern from service DB to Kafka

Correlate events with traceId and orderId

Keep event payloads minimal but complete

Document event ownership and contracts

Use DLT + replay tooling

17. Hands-On Workflow: End-to-End Order Lifecycle
17.1 Create topic(s)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic order.created \
  --bootstrap-server kafka:19092 \
  --partitions 3 --replication-factor 1
17.2 Start apps
Start consumer workers first

Start producer API service

POST /orders/create

17.3 Verify
Kafka UI topic messages increase

Consumer logs show processing

Consumer group lag near zero

18. Troubleshooting Playbook
Problem: This is not the correct coordinator for this group
Causes

Wrong advertised listeners

stale cluster metadata/volumes

mixed host/container broker endpoints

Fixes

host app uses localhost:9092

container app uses kafka:19092

clean old volumes when topology changes

use fresh groupId for diagnosis

Problem: Consumer group visible, but no message consumed
Checks

@EventPattern used with emit?

microservice actually started with await app.listen()?

topic name exact match?

message produced to same cluster?

no handler exception before logs?

Problem: Kafka UI works but Nest app fails
Usually endpoint mismatch in app broker config

or app running in container but using host address incorrectly

19. Production Readiness Checklist
 Unique and stable groupId per service

 Idempotency implemented

 Retry + DLT policy live

 Manual commit strategy documented

 Monitoring + alerts configured

 Security (TLS/auth/ACL) enabled

 Load and chaos testing completed

 Replay/runbook procedures documented

20. Interview Questions (Beginner → Advanced)
20.1 Fundamentals
What is Kafka and how is it different from RabbitMQ?

What are topic, partition, and offset?

What is a consumer group and why is it important?

How does Kafka ensure ordering?

What is retention in Kafka?

20.2 NestJS + Kafka Basics
Difference between @EventPattern and @MessagePattern?

Difference between emit and send in NestJS Kafka client?

How do you configure a Kafka consumer microservice in Nest?

Why should producer and consumer have different group IDs?

Why might consumer not receive messages even when topic has data?

20.3 Reliability
What is auto-commit and manual commit?

How do you commit offsets manually in NestJS?

What happens if consumer crashes before commit?

How do you implement retry and dead-letter topics?

How do you prevent duplicate processing?

20.4 Scaling and Design
How does partition count affect consumer scaling?

Can you have 10 consumers for a 3-partition topic in one group?

How do you design event keys for partitioning?

What is rebalancing and why can it hurt throughput?

How do you reduce rebalance churn?

20.5 Architecture
Explain choreography vs orchestration in Saga.

How would you design order-payment-inventory flow with Kafka?

How do you handle partial failure in event-driven workflows?

Why is outbox pattern useful with Kafka?

How do you guarantee data consistency across service DB and Kafka event publish?

20.6 Operations and Debugging
How do you diagnose consumer lag?

What does not correct coordinator for this group mean?

What metrics would you monitor for Kafka consumers?

How do you perform safe event replay?

How do you version event schemas without breaking consumers?

20.7 Advanced
What is exactly-once semantics in Kafka and practical caveats?

Difference between idempotent producer and transactional producer?

What are ISR and min.insync.replicas?

How do you design multi-region Kafka architecture?

How do you secure Kafka in production (authn/authz/encryption)?

Sample strong answer format in interviews
Start with concept

explain tradeoff

give real production example

mention failure handling/observability

21. Appendix: Useful Commands
List topics
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server kafka:19092
Describe topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --describe --topic order.created --bootstrap-server kafka:19092
Produce test message
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic order.created --bootstrap-server kafka:19092
Consume from beginning
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order.created --from-beginning --bootstrap-server kafka:19092
List consumer groups
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --list --bootstrap-server kafka:19092
Describe group lag
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --describe --group consumer-app-v1 --bootstrap-server kafka:19092
Final Notes
Start simple: JSON events + auto-commit in dev.

Evolve to manual commit + idempotency + DLT for production.

Always design for retries, duplicates, and partial failures.

Observability is not optional in event-driven systems.

Suggested Next Steps for This Book
Add diagrams (event flow, saga compensation, retry pipeline)

Add “mini projects” per chapter

Add GitHub repo links for each chapter branch

Add Kubernetes deployment chapter with HPA by lag

Add cloud-managed Kafka chapter (MSK/Confluent Cloud)


If you want, I can also generate:
1) a **companion `book-outline-with-chapter-exercises.md`**, and  
2) a **“100 interview Q&A” add-on chapter** in the same style.
::contentReference[oaicite:0]{index=0}