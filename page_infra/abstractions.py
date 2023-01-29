from logger_utility import WritePoint


class Marketplace:
    def __init__(self, marketplace: str, logger: WritePoint) -> None:
        self._marketplace = marketplace
        self._logger = logger.copy().tag("marketplace", marketplace)

        # Rabbit
        self.search_queue = f"{marketplace}_search"
        self.sku_queue = f"{marketplace}_sku"
        self.image_queue = f"{marketplace}_image"

        # Mongo
        self.database = marketplace
        self.search_collection = "search"
        self.url_collection = "url"
        self.sku_collection = "sku"
        self.core_historic_collection = "core_historic"
        self.core_snapshot_collection = "core_snapshot"
        self.price_historic_collection = "price_historic"
        self.price_snapshot_collection = "price_snapshot"
        self.rating_historic_collection = "rating_historic"
        self.rating_snapshot_collection = "rating_snapshot"

        # Meilisearch
        self.catalog_index = marketplace
