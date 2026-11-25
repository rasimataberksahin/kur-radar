"""Gold sources: Truncgil's live Turkish market board and a daily ounce-gold history."""

GRAMS_PER_TROY_OUNCE = 31.1034768


class GoldPayloadError(ValueError):
    """A response that cannot be trusted as data; nothing from it is loaded."""
