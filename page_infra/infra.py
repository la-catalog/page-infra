from datetime import datetime, timedelta

import meilisearch
import redis.asyncio as redis
from la_stopwatch import Stopwatch
from motor.motor_asyncio import AsyncIOMotorClient
from page_models import SKU
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.operations import IndexModel
from structlog.stdlib import BoundLogger, get_logger

from page_infra.options import get_marketplace_infra
from page_infra.options import options as marketplace_options


class Infra:
    def __init__(
        self,
        redis_url: str | None = None,
        mongo_url: str | None = None,
        meilisearch_url: str | None = None,
        meilisearch_key: str | None = None,
        logger: BoundLogger = get_logger(),
    ):
        self._logger = logger.bind(lib="page_infra")
        self._redis_url = redis_url
        self._mongo_url = mongo_url
        self._meilisearch_url = meilisearch_url
        self._meilisearch_key = meilisearch_key

    async def setup_databases(self) -> None:
        """Make sure that collections exists and have indexes"""

        mongo = AsyncIOMotorClient(self._mongo_url)

        for marketplace in marketplace_options:
            infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)

            # Temporary (while Motor doesn't support typing)
            collection: Collection

            collection = mongo[infra.database][infra.search_collection]
            index = IndexModel([("query", 1)], unique=True)
            await collection.create_indexes([index])

            collection = mongo[infra.database][infra.url_collection]
            index = IndexModel([("url", 1)], unique=True)
            await collection.create_indexes([index])

            collection = mongo[infra.database][infra.sku_collection]
            index = IndexModel([("code", 1)], unique=True)
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

    async def get_queries(self, marketplace: str) -> Cursor:
        """
        Returns every query to be made on marketplaces.

        I have explicit said that it returns Cursor but
        it's actually a AsyncIOMotorCursor. I should change
        once Motor gives support for typing.
        """
        mongo = AsyncIOMotorClient(self._mongo_url)
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        collection = mongo[infra.database][infra.search_collection]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        return collection.find({})

    async def discard_recent_urls(self, urls: list[str], marketplace: str) -> list[str]:
        """
        Discard any URL that have been accessed in the last X seconds.

        This is important when many SKUs have the URL for the same SKU.
        For example,
            When scraping A we discovered the URL for B
            And when scraping C we also discovered the URL for B

        We don't want to scrap B two times, that's why we check
        on redis if have being scraped in the last X seconds.
        """
        if not urls:
            return urls

        stopwatch = Stopwatch()
        redis_ = redis.from_url(self._redis_url)

        async def new_url(url):
            lock = redis_.lock(name=url, timeout=60, blocking_timeout=0.1)
            return await lock.acquire(blocking=False)

        new_urls = [url for url in urls if await new_url(url)]

        self._logger.info(
            event="Finish discarding recent URLs",
            urls_before=urls,
            urls_after=new_urls,
            marketplace=marketplace,
            duration=str(stopwatch),
        )

        return new_urls

    async def discard_old_urls(self, urls: list[str], marketplace: str) -> list[str]:
        """
        Discard any URL that have been accessed in the last X days.

        This is important because we care about updating SKUs
        but we don't need to update so frequently.
        """
        if not urls:
            return urls

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.sku_collection]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        yesterday = datetime.utcnow() - timedelta(days=1)
        async for doc in collection.find({"url": {"$in": urls}}):
            if doc["accessed"] >= yesterday.isoformat():
                urls.remove(doc["url"])

        self._logger.info(
            event="Finish discarding old URLs",
            urls_before=urls,
            urls_after=urls,  # TODO: change to new_urls after being implemented
            marketplace=marketplace,
            duration=str(stopwatch),
        )

        return urls

    async def identify_new_skus(self, skus: list[SKU], marketplace: str) -> list[SKU]:
        """
        Identify the SKUs that are not in the database or didn't change.
        """
        if not skus:
            return skus

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.sku_collection]
        codes = [sku.code for sku in skus if sku.code]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        async for doc in collection.find({"code": {"$in": codes}}, {"code": 1}):
            codes.remove(doc["code"])

        new_skus = [sku for sku in skus if sku.code in codes]

        self._logger.info(
            event="Finish identifying new SKUs",
            skus_before=len(skus),
            skus_after=len(new_skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

        return new_skus

    async def insert_skus(self, skus: list[SKU], marketplace: str) -> None:
        if not skus:
            return skus

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.sku_collection]
        documents = [sku.dict() for sku in skus]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        if documents > 0:
            await collection.insert_many(documents=documents)

        self._logger.info(
            event="SKUs inserted",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )
