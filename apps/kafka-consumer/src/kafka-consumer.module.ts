import { Module } from '@nestjs/common';
import { KafkaConsumerController } from './kafka-consumer.controller';
import { KafkaConsumerService } from './kafka-consumer.service';
import { ClientsModule, Transport } from '@nestjs/microservices';

@Module({
  imports: [
    // ClientsModule.register([
    //   {
    //     name: 'KAFKA_SERVICE',
    //     transport: Transport.KAFKA,
    //     options: {
    //       client: {
    //         brokers: ['localhost:9092'], // Add all brokers here
    //       },
    //       consumer: {
    //         groupId: 'nestjs-consumer-group-1',
    //       },
    //     },
    //   },
    // ]),
  ],
  controllers: [KafkaConsumerController],
  providers: [KafkaConsumerService],
})
export class KafkaConsumerModule {}
