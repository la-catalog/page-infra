from datetime import datetime, timedelta

import meilisearch
import redis.asyncio as redis
from la_stopwatch import Stopwatch
from logger_utility import WritePoint
from motor.motor_asyncio import AsyncIOMotorClient
from page_models import SKU
from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.operations import IndexModel

from page_infra.options import get_marketplace_infra
from page_infra.options import options as marketplace_options


class Infra:
    def __init__(
        self,
        redis_url: str,
        mongo_url: str,
        meilisearch_url: str,
        meilisearch_key: str,
        logger: WritePoint,
    ):
        self._redis_url = redis_url
        self._mongo_url = mongo_url
        self._meilisearch_url = meilisearch_url
        self._meilisearch_key = meilisearch_key
        self._logger = logger.copy().tag("package", "page_infra")

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

    # TODO: move to catalog-infra package
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
            When scraping A we may discover an URL for B
            And when scraping C we may discover the URL for B again

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
        collection = database[infra.url_collection]

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
        """
        Upsert SKU into collection.

        It will update the SKU only if the hash doesn't match.
        If it match, it will attempt to insert a new SKU
        but will fail because "code" field is a unique index.

        Note: This is great because bulk_write() with the paremeter "ordered"
        as False, will ignore errors.
        """

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

            # Only field that shouln't be changed if exist
            created = doc["metadata"].pop("created")

            requests.append(
                UpdateOne(
                    filter={
                        "code": sku.code,
                        "metadata.hash": {"$not": sku.metadata.hash},
                    },
                    update={
                        "$set": doc,
                        "$setOnInsert": {"metadata.created": created},
                    },
                    upsert=True,
                )
            )

        await collection.bulk_write(requests=requests, ordered=False)

        self._logger.info(
            event="SKUs inserted",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

    async def update_historics(self, skus: list[SKU], marketplace: str) -> None:
        """
        Upsert snapshot hash into collection.

        The "historic" field is ordered from newest to oldest.

        It will update the "historic" only if the first in array isn't the same
        as the one being inserted. In case it is, it will attempt to insert a new document
        but will fail because "code" field is a unique index.

        Note: This is great because bulk_write() with the paremeter "ordered"
        as False, will ignore errors.
        """

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
                    filter={
                        "code": sku.code,
                        "historic": {"$first": {"hash": {"$not": sku.metadata.hash}}},
                    },
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

        await collection.bulk_write(requests=requests, ordered=False)

        self._logger.info(
            event="Historics updated",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

    async def insert_snapshots(self, skus: list[SKU], marketplace: str) -> None:
        """
        Insert SKU snapshot into collection.

        The hash field is unique, so attempting to insert the
        same hash would fail.

        Note: This is great because you will never have duplicated snapshots.

        No error will be raised because bulk_write()
        use the parameter "ordered" as False and it can't
        guarantee that every operation will complete.

        Note: This is perfect because now you don't have to filter
        before inserting to prevent any error (one less request to database).
        """

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

        await collection.bulk_write(requests=requests, ordered=False)

        self._logger.info(
            event="Snapshots inserted",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )

    async def update_relatives(self, skus: list[SKU], marketplace: str) -> None:
        """
        Update SKUs that are relatives to each other.

        Whenever a SKU have one or more relatives, look at this relatives
        to discover more relatives and update your and their relatives.
        """
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
            for relative in sku.metadata.relatives:
                if s := await collection.find_one({"code": relative}):
                    relatives = list(SKU(s).metadata.relatives | sku.metadata.relatives)
                    set_ = {f"metadata.relatives.{r}": True for r in relatives}

                    # This will update this SKU relatives and
                    # the database SKUs relatives.
                    await collection.update_many(
                        {"code": {"$in": relatives}}, {"$set": set_}
                    )

        self._logger.info(
            event="Relatives updated",
            quantity=len(skus),
            marketplace=marketplace,
            duration=str(stopwatch),
        )
