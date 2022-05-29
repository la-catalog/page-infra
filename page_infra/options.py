from structlog.stdlib import BoundLogger

from page_infra.abstractions import Marketplace
from page_infra.exceptions import UnknowMarketplaceError

options: dict[str, type[Marketplace]] = {
    "google_shopping": GoogleShopping,
    "rihappy": Rihappy,
    "mercado_livre": MercadoLivre,
}


def get_marketplace_fetcher(marketplace: str, logger: BoundLogger) -> Marketplace:
    try:
        new_logger = logger.bind(marketplace=marketplace)
        marketplace_class = options[marketplace]
        return marketplace_class(marketplace=marketplace, logger=new_logger)
    except KeyError as e:
        raise UnknowMarketplaceError(
            f"Marketplace '{marketplace}' is not defined in page_sender package"
        ) from e
