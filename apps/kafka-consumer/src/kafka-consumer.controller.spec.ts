import { Test, TestingModule } from '@nestjs/testing';
import { KafkaConsumerController } from './kafka-consumer.controller';
import { KafkaConsumerService } from './kafka-consumer.service';

describe('KafkaConsumerController', () => {
  let kafkaConsumerController: KafkaConsumerController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [KafkaConsumerController],
      providers: [KafkaConsumerService],
    }).compile();

    kafkaConsumerController = app.get<KafkaConsumerController>(KafkaConsumerController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(kafkaConsumerController.getHello()).toBe('Hello World!');
    });
  });
});
