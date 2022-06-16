from structlog.stdlib import BoundLogger


class Marketplace:
    def __init__(self, marketplace: str, logger: BoundLogger) -> None:
        self._marketplace = marketplace
        self._logger = logger

        # Rabbit
        self.search_queue = ""
        self.sku_queue = ""
        self.image_queue = ""

        # Mongo
        self.sku_database = ""
        self.sku_collection = ""
        self.search_database = ""
        self.search_collection = ""

        # Meilisearch
        self.catalog_index = ""
