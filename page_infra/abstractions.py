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
        self.database = ""
        self.url_collection = "urls"
        self.sku_collection = "skus"
        self.historic_collection = "historics"
        self.snapshot_collection = "snapshots"
        self.search_collection = "searchs"

        # Meilisearch
        self.catalog_index = ""
