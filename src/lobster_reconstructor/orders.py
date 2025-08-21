from dataclasses import dataclass
from typing import Literal

@dataclass
class Order:
    """
    Represents a raw order event message from the limit order book data.

    Parameters
    ----------
    timestamp : float
        Event timestamp, in seconds after midnight.
    event_type : {'submit', 'cancel', 'delete', 'vis_exec', 'hid_exec', 'cross', 'halt'}
        Type of order book event:

        - 'submit' : A new order submission.
        - 'cancel' : A partial cancellation of an existing order.
        - 'delete' : A complete removal of an existing order.
        - 'vis_exec' : Execution against visible liquidity.
        - 'hid_exec' : Execution against hidden liquidity.
        - 'cross' : Crossing event (buy/sell imbalance).
        - 'halt' : Trading halt event.
    order_id : int
        Unique identifier for the order.
    size : int
        Number of shares
    price : int
        Price level of the order (scaled to be an integer, e.g. x10000 for LOBSTER data).
    direction : {'bid', 'ask'}
        Side of the order book the order belongs to:
        - 'bid' : Buy side.
        - 'ask' : Sell side.
    """
    timestamp: float
    event_type: Literal['submit', 'cancel', 'delete', 'vis_exec', 'hid_exec', 'cross', 'halt']
    order_id: int
    size: int
    price: int
    direction: Literal['bid', 'ask']


@dataclass
class LimitOrder:
    """
    Represents a limit order in the order book.

    Unlike :class:`Order`, which is a raw message/event, a ``LimitOrder``
    reflects the current resting state of an order in the book.

    Parameters
    ----------
    timestamp : float
        Time when the order was added in seconds after midnight.
    order_id : int
        Unique identifier for the order.
    size : int
        Remaining visible quantity of the order.
    price : int
        Price level of the order (scaled to be an integer, e.g. x10000 for LOBSTER data).
    direction : {'bid', 'ask'}
        Side of the order book:
        - 'bid' : Buy order.
        - 'ask' : Sell order.
    """
    timestamp: float
    order_id: int
    size: int
    price: int
    direction: Literal['bid', 'ask']
