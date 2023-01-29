from logger_utility import WritePoint

from page_infra.abstractions import Marketplace
from page_infra.exceptions import UnknowMarketplaceError

options: dict[str, type[Marketplace]] = {
    "google_shopping": Marketplace,
    "rihappy": Marketplace,
    "mercado_livre": Marketplace,
}


def get_marketplace_infra(marketplace: str, logger: WritePoint) -> Marketplace:
    """Get the infrastructure information responsible for the marketplace."""

    try:
        marketplace_class = options[marketplace]
        return marketplace_class(marketplace=marketplace, logger=logger)
    except KeyError as e:
        valid = ", ".join(options.keys())

        raise UnknowMarketplaceError(
            f"Marketplace '{marketplace}' is not defined in page_infra package. Valid options: {valid}"
        ) from e
