import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';

@Controller()
export class KafkaConsumerController {
  @EventPattern('test-topic')
  handleTestTopic(
    @Payload() message: any,
    @Ctx() context: KafkaContext,
  ) {
    const originalMessage = context.getMessage();
    const topic = context.getTopic();
    const partition = context.getPartition();

    console.log('--- Received Kafka Event ---');
    console.log('Topic:', topic);
    console.log('Partition:', partition);
    console.log('Payload:', message);
    console.log('Raw Value:', originalMessage.value?.toString());
  }
}
