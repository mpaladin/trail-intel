"""External data providers for trail athlete enrichment."""

from trailintel.providers.betrail import BetrailCatalogEntry, BetrailClient, BetrailLookupError, BetrailMatch
from trailintel.providers.itra import ItraCatalogEntry, ItraClient, ItraLookupError, ItraMatch
from trailintel.providers.utmb import UtmbCatalogEntry, UtmbClient, UtmbMatch

__all__ = [
    "BetrailCatalogEntry",
    "BetrailClient",
    "BetrailLookupError",
    "BetrailMatch",
    "ItraCatalogEntry",
    "ItraClient",
    "ItraLookupError",
    "ItraMatch",
    "UtmbCatalogEntry",
    "UtmbClient",
    "UtmbMatch",
]
