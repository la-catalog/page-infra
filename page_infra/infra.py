from datetime import datetime, timedelta

import meilisearch
import redis.asyncio as redis
from la_stopwatch import Stopwatch
from motor.motor_asyncio import AsyncIOMotorClient
from page_models import SKU
from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.operations import IndexModel
from pymongo.results import BulkWriteResult
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

            collection = mongo[infra.database][infra.historic_collection]
            index = IndexModel([("code", 1)], unique=True)
            await collection.create_indexes([index])

            collection = mongo[infra.database][infra.snapshot_collection]
            index = IndexModel([("hash", 1)], unique=True)
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

        new_urls = urls.copy()
        date = datetime.utcnow() - timedelta(days=7)
        isoformat = date.isoformat()

        async for doc in collection.find(
            {"url": {"$in": urls}, "accessed": {"$lt": isoformat}}
        ):
            new_urls.remove(doc["url"])

        self._logger.info(
            event="Finish discarding old URLs",
            urls_before=urls,
            urls_after=new_urls,
            marketplace=marketplace,
            duration=str(stopwatch),
        )

        return urls

    async def insert_skus(self, skus: list[SKU], marketplace: str) -> None:
        if not skus:
            return skus

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.sku_collection]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        requests = []

        for sku in skus:
            doc = sku.dict()
            created = doc["metadata"].pop("created")

            requests.append(
                UpdateOne(
                    filter={"code": sku.code},
                    update={
                        "$set": doc,
                        "$setOnInsert": {"metadata.created": created},
                    },
                    upsert=True,
                )
            )

        if requests:
            result: BulkWriteResult = await collection.bulk_write(
                requests=requests, ordered=False
            )

        self._logger.info(
            event="SKUs inserted",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

    async def update_historics(self, skus: list[SKU], marketplace: str) -> None:
        if not skus:
            return skus

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.historic_collection]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        requests = []

        for sku in skus:
            requests.append(
                UpdateOne(
                    filter={"code": sku.code},
                    update={
                        "$push": {
                            "historic": {
                                "$each": [
                                    {
                                        "created": sku.metadata.created,
                                        "hash": sku.metadata.hash,
                                    }
                                ],
                                "$position": 0,
                            }
                        }
                    },
                    upsert=True,
                )
            )

        if requests:
            result: BulkWriteResult = await collection.bulk_write(
                requests=requests, ordered=False
            )

        self._logger.info(
            event="Historics updated",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

    async def insert_snapshots(self, skus: list[SKU], marketplace: str) -> None:
        if not skus:
            return skus

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.snapshot_collection]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        requests = []

        for sku in skus:
            requests.append(
                InsertOne(
                    document={
                        "hash": sku.metadata.hash,
                        "created": sku.metadata.created,
                        "sku": sku.get_core(),
                    }
                )
            )

        if requests:
            result: BulkWriteResult = await collection.bulk_write(
                requests=requests, ordered=False
            )

        self._logger.info(
            event="Snapshots inserted",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

    async def update_relatives(self, skus: list[SKU], marketplace: str) -> None:
        if not skus:
            return skus

        stopwatch = Stopwatch()
        infra = get_marketplace_infra(marketplace=marketplace, logger=self._logger)
        mongo = AsyncIOMotorClient(self._mongo_url)
        database = mongo[infra.database]
        collection = database[infra.sku_collection]

        # Temporary (while Motor doesn't support typing)
        collection: Collection

        for sku in skus:
            if sku.metadata.relatives:
                relative = next(iter(sku.metadata.relatives))
                sku_ = SKU(await collection.find_one({"code": relative}))
                relatives = list(sku_.metadata.relatives | sku.metadata.relatives)
                set_ = {f"metadata.relatives.{r}": True for r in relatives}

                await collection.update_many(
                    {"code": {"$in": relatives}}, {"$set": set_}
                )

        self._logger.info(
            event="Relatives updated",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )
