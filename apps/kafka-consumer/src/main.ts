import { NestFactory } from '@nestjs/core';
import { KafkaConsumerModule } from './kafka-consumer.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';


async function bootstrap() {
  const app = await NestFactory.create(KafkaConsumerModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'nestjs-server',
        brokers: ['localhost:9092'],
      },
      consumer: {
        groupId: 'nestjs-consumer-group-1',
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
      },
      run: {
        autoCommit: false, // IMPORTANT
        // partitionsConsumedConcurrently: 1, // optional
      },
    },
  });

  await app.startAllMicroservices();
  await app.listen(3001); // HTTP port
}
bootstrap();
