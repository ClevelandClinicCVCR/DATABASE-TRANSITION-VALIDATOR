"""Database configuration classes for transition validation."""

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine.base import Engine


@dataclass
class DatabaseConfig:
    """Configuration for database connections."""

    source_or_target_type: str
    name: str
    engine: Engine
    schema: str

    def __post_init__(self):
        """Validate the database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to {self.name} database: {e}"
            )
