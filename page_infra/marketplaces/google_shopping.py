from structlog.stdlib import BoundLogger

from page_infra.abstractions import Marketplace


class GoogleShopping(Marketplace):
    def __init__(self, marketplace: str, logger: BoundLogger):
        super().__init__(marketplace, logger)

        # Rabbit
        self.search_queue = "google_shopping_search"
        self.sku_queue = "google_shopping_sku"
        self.image_queue = "google_shopping_image"

        # Mongo
        self.database = "google_shopping"
        self.url_collection = "urls"
        self.sku_collection = "skus"
        self.search_collection = "search"

        # Meilisearch
        self.catalog_index = "google_shopping"
