import { Module } from '@nestjs/common';
import { KafkaProducerController } from './kafka-producer.controller';
import { KafkaProducerService } from './kafka-producer.service';
import { ClientsModule, Transport } from '@nestjs/microservices';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'nestjs-client',
            brokers: ['localhost:9092'], // host run
          },
          consumer: {
            groupId: 'nestjs-client-v1',
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
          },
          producer: {
            allowAutoTopicCreation: true,
          },
        },
      },
    ]),
  ],
  controllers: [KafkaProducerController],
  providers: [KafkaProducerService],
})
export class KafkaProducerModule {}
