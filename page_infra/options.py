from structlog.stdlib import BoundLogger

from page_infra.abstractions import Marketplace
from page_infra.exceptions import UnknowMarketplaceError
from page_infra.marketplaces.google_shopping import GoogleShopping
from page_infra.marketplaces.mercado_livre import MercadoLivre
from page_infra.marketplaces.rihappy import Rihappy

options: dict[str, type[Marketplace]] = {
    "google_shopping": GoogleShopping,
    "rihappy": Rihappy,
    "mercado_livre": MercadoLivre,
}


def get_marketplace_infra(marketplace: str, logger: BoundLogger) -> Marketplace:
    """Get the infrastructure information responsible for the marketplace."""

    try:
        new_logger = logger.bind(marketplace=marketplace)
        marketplace_class = options[marketplace]
        return marketplace_class(marketplace=marketplace, logger=new_logger)
    except KeyError as e:
        valid = ", ".join(options.keys())

        raise UnknowMarketplaceError(
            f"Marketplace '{marketplace}' is not defined in page_sender package. Valid options: {valid}"
        ) from e
