from dataclasses import dataclass
from datetime import date


class Event:
    pass


@dataclass
class OutOfStock(Event):
    sku: str
