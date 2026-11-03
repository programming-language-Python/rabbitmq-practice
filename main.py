from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from pydantic import BaseModel

from rabbitmq import connect_rabbitmq, declare_orders_exchange, publish_json, ORDER_ROUTING_KEY


class Product(BaseModel):
    name: str
    price: str


class Order(BaseModel):
    user_id: int
    products: list[Product]


@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = await connect_rabbitmq()
    channel = await connection.channel()
    orders_exchange = await declare_orders_exchange(channel)

    # можем с помощью request забрать из любого места
    app.state.orders_exchange = orders_exchange

    yield
    connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/orders")
async def create_order(payload: Order, request: Request):
    # Представим, что тут у нас сохранение в БД

    event_data = {
        "event_id": str(uuid4()),
        "event": "order.created",
        "user_id": payload.user_id,
        "products": [product.model_dump() for product in payload.products]
    }
    await publish_json(
        exchange=request.app.state.orders_exchange,
        routing_key=ORDER_ROUTING_KEY,
        data=event_data
    )
    return "success"
