import { Injectable } from '@nestjs/common';

@Injectable()
export class KafkaConsumerService {
  getHello(): string {
    return 'Hello World!';
  }

}
