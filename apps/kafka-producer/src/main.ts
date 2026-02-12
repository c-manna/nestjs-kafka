import { NestFactory } from '@nestjs/core';
import { KafkaProducerModule } from './kafka-producer.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.create(KafkaProducerModule);
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: ['localhost:9092'], // Add all brokers here
      },
      consumer: {
        groupId: 'nestjs-group',
      },
      producer: {
        allowAutoTopicCreation: true, // Enable auto topic creation
        idempotent: true, // enable idempotent producer
        maxInFlightRequests: 1, // safest ordering with retries
        retry: {
          retries: 10,
        },
      },
    },
  });

  await app.startAllMicroservices();
  await app.listen(process.env.port ?? 3000);
}
bootstrap();
