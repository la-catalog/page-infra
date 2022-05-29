from aio_pika import Channel, DeliveryMode, Message


async def publish(channel: Channel, message: str, queue: str):
    await channel.default_exchange.publish(
        message=Message(body=message.encode(), delivery_mode=DeliveryMode.PERSISTENT),
        routing_key=queue,
    )
