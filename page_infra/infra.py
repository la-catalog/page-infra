from page_sku.sku import SKU
from pydantic import AnyHttpUrl
from structlog.stdlib import BoundLogger, get_logger


class Infra:
    def __init__(self, logger: BoundLogger = get_logger()):
        self._logger = logger.bind(lib="page_sender")

    async def exist_skus(self, skus: list[AnyHttpUrl]) -> bool:
        pass

    async def insert_skus(self, skus: list[SKU]):
        pass
