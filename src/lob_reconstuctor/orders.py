from dataclasses import dataclass
from typing import Literal

@dataclass
class Order:
    timestamp: float
    event_type: Literal['submit', 'cancel', 'delete', 'vis_exec', 'hid_exec', 'cross', 'halt']
    order_id: int
    size: int
    price: int
    direction: Literal['bid', 'ask']


@dataclass
class LimitOrder:
    timestamp: float
    order_id: int
    size: int
    price: int
    direction: Literal['bid', 'ask']
