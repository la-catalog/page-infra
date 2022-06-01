from datetime import datetime

from la_stopwatch import Stopwatch
from motor.motor_asyncio import AsyncIOMotorClient
from page_sku import SKU
from pymongo import TEXT
from pymongo.collection import Collection
from structlog.stdlib import BoundLogger, get_logger

from page_infra.options import get_marketplace_infra
from page_infra.options import options as marketplace_options


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

    async def setup_collections(self) -> None:
        """Make sure that collections have indexes"""

        mongo = AsyncIOMotorClient(self._mongo_url)

        for marketplace in marketplace_options:
            infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
            collection = mongo[infra.sku_database][infra.sku_collection]

            # Temporary: while Motor doesn't support typing
            collection: Collection

            await collection.create_indexes([("code", TEXT)])

    def _on_inserting(
        self, skus: list[SKU], marketplace: str, duration: datetime
    ) -> None:
        self._logger.info(
            event="SKUs inserted",
            duration=duration,
            quantity=len(skus),
            marketplace=marketplace,
        )

    @Stopwatch(_on_inserting)
    async def insert_skus(self, skus: list[SKU], marketplace: str) -> None:
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.sku_database]
        collection = database[infra.sku_collection]
        documents = [sku.dict() for sku in skus]

        # Temporary: while Motor doesn't support typing
        collection: Collection

        await collection.insert_many(documents=documents)
