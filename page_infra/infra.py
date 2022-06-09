import meilisearch
from la_stopwatch import Stopwatch
from motor.motor_asyncio import AsyncIOMotorClient
from page_sku import SKU
from pymongo import TEXT
from pymongo.collection import Collection
from pymongo.operations import IndexModel
from structlog.stdlib import BoundLogger, get_logger

from page_infra.options import get_marketplace_infra
from page_infra.options import options as marketplace_options


class Infra:
    def __init__(
        self,
        redis_url: str,
        mongo_url: str,
        meilisearch_url: str,
        meilisearch_key: str,
        logger: BoundLogger = get_logger(),
    ):
        self._logger = logger.bind(lib="page_infra")
        self._redis_url = redis_url
        self._mongo_url = mongo_url
        self._meilisearch_url = meilisearch_url
        self._meilisearch_key = meilisearch_key

    async def setup_sku_database(self) -> None:
        """Make sure that collections have indexes"""

        mongo = AsyncIOMotorClient(self._mongo_url)

        for marketplace in marketplace_options:
            infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
            collection = mongo[infra.sku_database][infra.sku_collection]

            # Temporary: while Motor doesn't support typing
            collection: Collection

            index = IndexModel([("code", TEXT)])
            await collection.create_indexes([index])

    async def setup_catalog_database(self) -> None:
        """Make sure that catalog have settings"""

        client = meilisearch.Client(
            url=self._meilisearch_url,
            api_key=self._meilisearch_key,
        )

        for marketplace in marketplace_options:
            infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
            client.index(infra.catalog_index).update_settings(
                {
                    "rankingRules": [
                        "words",
                        "typo",
                        "proximity",
                        "attribute",
                        "sort",
                        "exactness",
                    ],
                    "searchableAttributes": [
                        "_id",
                        "gtin",
                        "name",
                        "brand",
                        "description",
                    ],
                    "typoTolerance": {
                        "disableOnAttributes": [
                            "_id",
                            "gtin",
                        ]
                    },
                }
            )

    async def identify_new_urls(self, urls: list[str], marketplace: str) -> list[bool]:
        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.sku_database]
        collection = database[infra.sku_collection]

        # Temporary: while Motor doesn't support typing
        collection: Collection

        self._logger.info(
            event="Finish identify URLs",
            urls_before=urls,
            urls_after=urls,  # TODO: change to new_urls after being implemented
            marketplace=marketplace,
            duration=str(stopwatch),
        )

        return urls

    async def identify_new_skus(self, skus: list[SKU], marketplace: str) -> list[bool]:
        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.sku_database]
        collection = database[infra.sku_collection]
        codes = [sku.code for sku in skus if sku.code]

        # Temporary: while Motor doesn't support typing
        collection: Collection

        async for doc in collection.find({"code": {"$in": codes}}, {"code": 1}):
            codes.remove(doc["code"])

        new_skus = [sku for sku in skus if sku.code in codes]

        self._logger.info(
            event="Finish identify SKUs",
            skus_before=len(skus),
            skus_after=len(new_skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

        return new_skus

    async def insert_skus(self, skus: list[SKU], marketplace: str) -> None:
        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.sku_database]
        collection = database[infra.sku_collection]
        documents = [sku.dict() for sku in skus]

        # Temporary: while Motor doesn't support typing
        collection: Collection

        if len(documents) > 0:
            await collection.insert_many(documents=documents)

        self._logger.info(
            event="SKUs inserted",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )
