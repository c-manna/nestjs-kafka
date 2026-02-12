import { Controller, Get, Inject, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';

import { Client, ClientKafka, Ctx, EventPattern, KafkaContext, Payload, Transport } from '@nestjs/microservices';

@Controller()
export class KafkaProducerController implements OnModuleInit {
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
  private readonly logger = new Logger(KafkaProducerController.name);

  
  @EventPattern('test-topic')
  async handleTestTopic(@Payload() payload: any, @Ctx() context: KafkaContext) {
    const consumer = context.getConsumer();
    const topic = context.getTopic();
    const partition = context.getPartition();
    const message = context.getMessage();

    // Kafka message offset is string, e.g. "5"
    const currentOffset = Number(message.offset);
    const nextOffset = String(currentOffset + 1); // commit next offset

    try {
      const rawValue = message.value?.toString();
      this.logger.log(
        `Received topic=${topic} partition=${partition} offset=${message.offset} payload=${rawValue}`,
      );

      console.log('Topic:', topic);
      console.log('Partition:', partition);
      console.log('Payload:', message);

      // 1) Your business logic here
      // await this.someService.process(JSON.parse(rawValue ?? '{}'));

      // 2) Commit only after success
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: nextOffset,
        },
      ]);

      this.logger.log(
        `Committed topic=${topic} partition=${partition} nextOffset=${nextOffset}`,
      );
    } catch (error: any) {
      this.logger.error(
        `Processing failed at offset=${message.offset}. Not committing. Error=${error?.message}`,
      );
      // no commit => message can be reprocessed or DLQ
      setTimeout(() => {
        consumer.seek({
          topic,
          partition,
          offset: message.offset,
        });
      }, 5000);
    }
  }
}
