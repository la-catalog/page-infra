from structlog.stdlib import BoundLogger

from page_infra.abstractions import Marketplace


class Rihappy(Marketplace):
    def __init__(self, marketplace: str, logger: BoundLogger):
        super().__init__(marketplace, logger)

        # Rabbit
        self.search_queue = "rihappy_search"
        self.sku_queue = "rihappy_sku"
        self.image_queue = "rihappy_image"

        # Mongo
        self.database = "rihappy"
        self.url_collection = "urls"
        self.sku_collection = "skus"
        self.search_collection = "search"

        # Meilisearch
        self.catalog_index = "rihappy"
