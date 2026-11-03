import json

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue, AbstractExchange

NOTIFICATIONS_QUEUE = "notifications.order_created"
ORDER_EXCHANGE = "order"
ORDER_ROUTING_KEY = "order.created"


async def connect_rabbitmq():
    return await aio_pika.connect_robust("awgp://guest:guest@localhost:5672/")


# принято так называть declare_exchange
# Это обменик
# В channel передаётся connect_rabbitmq
async def declare_orders_exchange(channel: AbstractChannel):
    # по умолчанию используется ExchangeType.DIRECT
    return await channel.declare_exchange(ORDER_EXCHANGE)


# очередь
async def declare_notifications_queue(
        channel: AbstractChannel,
        exchange: AbstractExchange
):
    queue: AbstractQueue = await channel.declare_queue(NOTIFICATIONS_QUEUE)
    await queue.bind(exchange, routing_key=ORDER_ROUTING_KEY)
    return queue


async def publish_json(exchange: AbstractExchange, routing_key: str, data: dict):
    message = aio_pika.Message(json.dumps(data).encode())
    await exchange.publish(message, routing_key)
