from structlog.stdlib import BoundLogger

from page_infra.abstractions import Marketplace


class MercadoLivre(Marketplace):
    def __init__(self, marketplace: str, logger: BoundLogger):
        super().__init__(marketplace, logger)

        # Rabbit
        self.search_queue = "mercado_livre_search"
        self.sku_queue = "mercado_livre_sku"
        self.image_queue = "mercado_livre_image"

        # Mongo
        self.database = "mercado_livre"

        # Meilisearch
        self.catalog_index = "mercado_livre"
