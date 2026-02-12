import { Module } from '@nestjs/common';
import { KafkaConsumerController } from './kafka-consumer.controller';
import { KafkaConsumerService } from './kafka-consumer.service';
import { ClientsModule, Transport } from '@nestjs/microservices';

@Module({
  imports: [

  ],
  controllers: [KafkaConsumerController],
  providers: [KafkaConsumerService],
})
export class KafkaConsumerModule {}
