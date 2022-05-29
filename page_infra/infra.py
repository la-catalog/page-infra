from motor.motor_asyncio import AsyncIOMotorClient
from page_sku import SKU
from pydantic import AnyHttpUrl
from pymongo.collection import Collection
from structlog.stdlib import BoundLogger, get_logger

from page_infra.options import get_marketplace_infra


class Infra:
    def __init__(
        self,
        redis_url: str,
        mongo_url: str,
        meilisearch_url: str,
        logger: BoundLogger = get_logger(),
    ):
        self._logger = logger.bind(lib="page_infra")
        self._redis_url = redis_url
        self._mongo_url = mongo_url
        self._meilisearch_url = meilisearch_url

    async def insert_skus(self, skus: list[SKU], marketplace: str):
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.sku_database]
        collection = database[infra.sku_collection]
        documents = [sku.dict() for sku in skus]

        # Temporary: while Motor doesn't have typing
        collection: Collection

        await collection.insert_many(documents=documents)
