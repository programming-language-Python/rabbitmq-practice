import asyncio
import json

from aio_pika.abc import AbstractIncomingMessage

from rabbitmq import connect_rabbitmq, declare_orders_exchange, declare_notifications_queue


async def handle_order_created(message: AbstractIncomingMessage):
    event_data = json.loads(message.body.decode())

    # Имитация отправки этого уведомления на почту через Celery
    # Имитация сохранения этого уведомления в БД

    print(f"Полученное сообщение: {event_data}")

    # потверждение, что сообщение приняли и обработали иначе выдаст ошибку
    await message.ack()


async def main():
    connection = await connect_rabbitmq()
    channel = await connection.channel()
    orders_exchange = await declare_orders_exchange(channel)
    queue = await declare_notifications_queue(channel, orders_exchange)

    await queue.consume(handle_order_created)

    # для бесконечного считывания сообщений из очередей (чтоб сразу не закрылось соединение)
    await asyncio.Future()

    await connection.close()


if __name__ == '__main__':
    asyncio.run(main())
