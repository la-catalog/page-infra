from structlog.stdlib import BoundLogger


class Marketplace:
    def __init__(self, marketplace: str, logger: BoundLogger) -> None:
        self._marketplace = marketplace
        self._logger = logger

        # Rabbit
        self._sku_queue = ""
        self._image_queue = ""

        # Mongo
        self._sku_database = ""
        self._sku_collection = ""

        # Meilisearch
        self._sku_index = ""
