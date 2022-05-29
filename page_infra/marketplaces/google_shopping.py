from structlog.stdlib import BoundLogger

from page_infra.abstractions import Marketplace


class GoogleShopping(Marketplace):
    def __init__(self, marketplace: str, logger: BoundLogger):
        super().__init__(marketplace, logger)

        # Rabbit
        self.sku_queue = "google_shopping_sku"
        self.image_queue = "google_shopping_image"

        # Mongo
        self.sku_database = "google_shopping"
        self.sku_collection = "google_shopping"

        # Meilisearch
        self.sku_index = "google_shopping"
