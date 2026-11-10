"""Factioner Python companion package."""

__version__ = "0.1.0"


def health() -> dict[str, str]:
    """Return a simple health payload for smoke tests / CI."""
    return {"status": "ok", "package": "factioner", "version": __version__}
