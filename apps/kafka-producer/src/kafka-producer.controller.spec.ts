import { Test, TestingModule } from '@nestjs/testing';
import { KafkaProducerController } from './kafka-producer.controller';
import { KafkaProducerService } from './kafka-producer.service';

describe('KafkaProducerController', () => {
  let kafkaProducerController: KafkaProducerController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [KafkaProducerController],
      providers: [KafkaProducerService],
    }).compile();

    kafkaProducerController = app.get<KafkaProducerController>(KafkaProducerController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(kafkaProducerController.getHello()).toBe('Hello World!');
    });
  });
});
