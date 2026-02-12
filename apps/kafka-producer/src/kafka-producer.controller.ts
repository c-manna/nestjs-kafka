import { Controller, Get, Inject, OnModuleInit } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';

import { Client, ClientKafka, Transport } from '@nestjs/microservices';

@Controller()
export class KafkaProducerController implements OnModuleInit{
  // constructor(private readonly kafkaProducerService: KafkaProducerService) {}
  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @Get('send-message')
  async sendMessage(): Promise<string> {
    await this.kafkaClient.emit('test-topic', {
      value: 'Hello Kafka test',
      ts: new Date().toISOString(),
    });
    return 'Message sent!';
  }

  // @Get()
  // getHello(): string {
  //   return this.kafkaProducerService.getHello();
  // }
}
