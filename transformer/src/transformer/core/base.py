from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class Transformer:
    source: str
    schema_version: str
    columns: list[str]
    to_record: Callable[[dict], dict]
    """Raise KeyError, TypeError, IndexError or ValueError to skip a row."""
