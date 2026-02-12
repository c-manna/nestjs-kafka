import { Injectable } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Injectable()
export class KafkaConsumerService {
  getHello(): string {
    return 'Hello World!';
  }

  @MessagePattern('test-topic')
  consumeMessage(message: any) {
    console.log('Received message:', message);
  }
}
