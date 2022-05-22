import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  HttpCode,
  Inject,
} from '@nestjs/common';
import { OrdersService } from './orders.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { UpdateOrderDto } from './dto/update-order.dto';
import {
  KafkaMessage,
  Producer,
} from '@nestjs/microservices/external/kafka.interface';
import { MessagePattern, Payload } from '@nestjs/microservices';
import { OrderStatus } from './entities/order.entity';

@Controller('orders')
export class OrdersController {
  constructor(
    private readonly ordersService: OrdersService,
    @Inject('KAFKA_PRODUCER') private kafkaProducer: Producer,
  ) {}

  @Post()
  async create(@Body() createOrderDto: CreateOrderDto) {
    const order = await this.ordersService.create(createOrderDto);
    this.kafkaProducer.send({
      topic: 'orders',
      messages: [{ key: 'orders', value: JSON.stringify(order) }],
    });
    return order;
  }

  @Get()
  findAll() {
    return this.ordersService.findAll();
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.ordersService.findOne(id);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() updateOrderDto: UpdateOrderDto) {
    return this.ordersService.update(id, updateOrderDto);
  }

  @HttpCode(204)
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.ordersService.remove(id);
  }

  @MessagePattern('orders')
  async consumer(@Payload() message: KafkaMessage) {
    console.log('method consumer:', message.value);

    await this.kafkaProducer.send({
      topic: 'orders-approved',
      messages: [
        {
          key: 'orders-approved',
          value: JSON.stringify({
            ...message.value,
            status: OrderStatus.Approved,
          }),
        },
      ],
    });
  }

  @MessagePattern('orders-approved')
  async consumerApproved(@Payload() message: KafkaMessage) {
    const { id } = message.value as any;
    await this.ordersService.update(id, { status: OrderStatus.Approved });
    console.log('method consumerApproved:', message.value);
  }
}
