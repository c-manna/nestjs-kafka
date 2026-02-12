import { Controller, Logger } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';

@Controller()
export class KafkaConsumerController {
  // @EventPattern('test-topic')
  // handleTestTopic(
  //   @Payload() message: any,
  //   @Ctx() context: KafkaContext,
  // ) {
  //   const originalMessage = context.getMessage();
  //   const topic = context.getTopic();
  //   const partition = context.getPartition();

  //   console.log('--- Received Kafka Event ---');
  //   console.log('Topic:', topic);
  //   console.log('Partition:', partition);
  //   console.log('Payload:', message);
  //   console.log('Raw Value:', originalMessage.value?.toString());
  // }

  private readonly logger = new Logger(KafkaConsumerController.name);

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
        console.log('Payload:', rawValue);

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
