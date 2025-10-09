from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Configuration for database connection."""

    db_path: str
    """Path to the GeoPackage database file."""

    driver: str = "GPKG"
    """Database driver (default: GPKG for GeoPackage)."""
