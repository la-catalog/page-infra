import asyncio
import json

from aio_pika import (
    Channel,
    Connection,
    DeliveryMode,
    IncomingMessage,
    Message,
    connect,
)


async def publish_json(channel: Channel, message: dict, queue: str):
    """Publish a dictionary as JSON to queue."""

    message_json = json.dumps(message)
    message_body = message_json.encode()

    await channel.default_exchange.publish(
        message=Message(body=message_body, delivery_mode=DeliveryMode.PERSISTENT),
        routing_key=queue,
    )


async def publish_many(connection: Connection):
    channel = await connection.channel()

    async with channel:
        for i in range(10_000):
            await publish_json(
                channel=channel,
                message={"amazon_sku": f"{i}"},
                queue="amazon_sku",
            )


async def process_message(message: IncomingMessage):
    print(message.body)
    await message.ack()


async def consume_queue(connection: Connection, name: str):
    channel = await connection.channel()

    async with channel:
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(name=name, durable=True)

        await queue.consume(process_message)

        await asyncio.Future()


# async def main():
#     connection = await connect("amqp://username:password@127.0.0.1:5672")

#     async with connection:
#         await asyncio.gather(
#             publish_many(connection), publish_many(connection), publish_many(connection)
#         )


async def main():
    connection = await connect("amqp://username:password@127.0.0.1:5672")

    async with connection:
        await asyncio.gather(
            consume_queue(connection, "amazon_sku"),
            consume_queue(connection, "americanas_sku"),
        )


asyncio.run(main())
