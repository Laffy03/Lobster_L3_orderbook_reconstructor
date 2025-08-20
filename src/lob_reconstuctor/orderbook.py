from sortedcontainers import SortedDict
from typing import Literal, List
from collections import namedtuple
import numpy as np
import pandas as pd
import plotly.express as px
from plotly.basedatatypes import BaseTraceType
import warnings
import logging

from src.lob_reconstuctor.orders import Order, LimitOrder
from src.lob_reconstuctor.ofi import OFI
from src.lob_reconstuctor.utils import format_timestamp

logger = logging.getLogger(__name__)

class Orderbook:
    """
    Limit Order Book (LOB) data structure with support for order
    submission, cancellation, execution, OFI computation, and visualization.

    Parameters
    ----------
    nlevels : int
        Maximum number of price levels to store in the book.
    ticker : str
        ticker symbol for the asset.
    tick_size : float
        Minimum tick size (price increment).
    price_scaling : float, default=0.0001
        Scaling factor to convert integer price representation to display
        prices (e.g. 0.0001 for LOBSTER data).

    Attributes
    ----------
    bids : SortedDict
        Bid side of the order book, keyed by descending price.
    asks : SortedDict
        Ask side of the order book, keyed by ascending price.
    curr_book_timestamp : float
        Current timestamp of the order book.
    midprice : float or None
        Current midprice of the book, if defined.
    midprice_change_timestamp : float
        Timestamp of the last midprice change.
    cum_OFI : OFI
        Object tracking cumulative order flow imbalance (OFI).
    trade_log : list
        List of executed trades (as namedtuples).
    """
    def __init__(self, nlevels: int, ticker: str, tick_size: float, price_scaling: float =0.0001):
        if tick_size <= 0 or price_scaling <= 0:
            raise ValueError("tick_size and price_scaling must be positive")
        if not isinstance(nlevels, int):
            raise ValueError("nlevels must be an integer")

        self.bids = SortedDict(lambda x: -x) #Price : {Order ID: LimitOrder}
        self.asks = SortedDict()
        self.ticker = ticker
        self.tick_size = tick_size
        self.price_scaling = price_scaling
        self.nlevels = nlevels
        self.curr_book_timestamp = 0.0
        self.midprice = None
        self.midprice_change_timestamp = 0.0
        self.cum_OFI = OFI()
        self.trade_log = []

    # -------------------------
    # State management
    # -------------------------
    def clear_orderbook(self) -> None:
        """
        Reset the order book to an empty state.
        """
        self.bids.clear()
        self.asks.clear()
        self.curr_book_timestamp = 0.0
        self.midprice = None
        self.midprice_change_timestamp = 0.0
        self.reset_cum_OFI()
        self.trade_log.clear()

    def clear_trade_log(self) -> None:
        """
        Clear the trade log without affecting the order book.
        """
        self.trade_log.clear()

    # ----------------------------------
    # Order Processing Handler & Helpers
    # ----------------------------------
    def process_order(self, order: Order) -> None:
        """
        Process a new order message and update the book accordingly.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.

        Raises
        ------
        ValueError
            If the direction, event type, or timestamp is invalid.
        """
        if order.direction not in ("bid", "ask"):
            raise ValueError(f"Invalid order direction: {order.direction!r}. Expected 'bid' or 'ask'.")
        if order.timestamp < self.curr_book_timestamp:
            raise ValueError(f"Order timestamp {order.timestamp} is earlier than current book timestamp {self.curr_book_timestamp}.")

        self.curr_book_timestamp = order.timestamp
        prev_midprice = self.mid_price()
        if order.event_type == 'submit':
            self._add_order(order)
        elif order.event_type == 'cancel':
            self._cancel_order(order)
        elif order.event_type == 'delete':
            self._delete_order(order)
        elif order.event_type == 'vis_exec':
            self._execute_visible_order(order)
        elif order.event_type == 'hid_exec':
            self._handle_hidden_exec(order)
        elif order.event_type == 'cross':
            pass
        elif order.event_type == 'halt':
            pass
        else:
            raise ValueError(f"Unknown event type: {order.event_type}")

        new_midprice = self.mid_price()
        if prev_midprice is not None and new_midprice is not None:
            if new_midprice != prev_midprice:
                self.midprice = new_midprice
                self.midprice_change_timestamp = order.timestamp

    def _does_order_cross_spread(self,order: Order) -> bool:
        """
        Check whether an incoming order crosses the current spread.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.

        Returns
        -------
        bool
            True if the order crosses the spread, False otherwise.
        """
        if order.direction == 'bid':
            return order.price >= self.lowest_ask_price()
        if order.direction == 'ask':
            return order.price <= self.highest_bid_price()
        if order.direction=='ask' and not self.bids:
            return False
        if order.direction=='bid' and not self.asks:
            return False

    def _record_trade(
        self,
        timestamp: float,
        trade_type: Literal["vis_exec", "aggro_lim", "hid_exec"],
        direction: Literal["bid", "ask"],
        size: int,
        price: int,
        order_id: int
    ) -> None:
        """
        Record an executed trade in the trade log.
        Bid direction means a bid limit order was matched; Ask direction means an ask limit order was matched

        Parameters
        ----------
        timestamp : float
            Execution timestamp.
        trade_type : {"vis_exec", "aggro_lim", "hid_exec"}
            Type of execution.
        direction : {"bid", "ask"}
            Side of the resting order.
        size : int
            Trade size.
        price : int
            Execution price.
        order_id : int
            ID of the aggressive order/execution.
        """
        Trade = namedtuple("Trade", ["timestamp", "trade_type", "direction", "size", "price", "order_id"])
        trade = Trade(timestamp, trade_type, direction, size, price, order_id)
        self.trade_log.append(trade)

    def _add_order(self, order: Order) -> None:
        """
        Insert a new order into the book.
        If the order crosses the spread, it is executed against the
        opposite side. Any unfilled remainder is added to the book.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.
        """
        if self._does_order_cross_spread(order):
            remaining = self._execute_against_opposite_book(order)
            if remaining > 0:
                remaining_order = LimitOrder(
                    timestamp=order.timestamp,
                    order_id=order.order_id,
                    size=remaining,
                    price=order.price,
                    direction=order.direction
                )
                self._update_LOFI(remaining_order)
                side = getattr(self, f'{order.direction}s')
                if order.price not in side:
                    side[order.price] = {}
                side[order.price][order.order_id] = remaining_order
        else:
            self._update_LOFI(order)
            side = getattr(self, f'{order.direction}s')
            if order.price not in side:
                side[order.price] = {}
            side[order.price][order.order_id] = LimitOrder(timestamp=order.timestamp, order_id=order.order_id, size=order.size, price=order.price, direction=order.direction)

    def _execute_against_opposite_book(self, order: Order) -> int:
        """
        Match an aggressive order against the opposite side.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.

        Returns
        -------
        int
            Remaining unfilled size of the order after matching.
        """
        remaining_size = order.size
        while remaining_size > 0 and self._does_order_cross_spread(order):
            opposite_side = 'asks' if order.direction == 'bid' else 'bids'
            side = getattr(self, opposite_side)
            if not side:
                break
            best_price = next(iter(side))
            orders_at_price = side[best_price]

            if not orders_at_price:
                del side[best_price]
                continue

            order_id, first_order = next(iter(orders_at_price.items()))

            trade_size = min(remaining_size, first_order.size)

            first_order.size -= trade_size
            remaining_size -= trade_size

            if first_order.size <= 0:
                del orders_at_price[order_id]
            if not orders_at_price:
                del side[best_price]

            self._record_trade(order.timestamp, "aggro_lim", 'ask' if order.direction == 'bid' else 'bid', trade_size, best_price, order.order_id)
            if order.direction == 'bid':
                self.cum_OFI.Ma.size += trade_size
                self.cum_OFI.Ma.count += 1
            elif order.direction == 'ask':
                self.cum_OFI.Mb.size += trade_size
                self.cum_OFI.Mb.count += 1

        return remaining_size

    def _execute_visible_order(self, order: Order) -> None:
        """
        Execute a visible resting order.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.

        Warns
        -----
        UserWarning
            If the price or order ID is not found in the book.
        """
        self._update_MOFI(order)
        self._record_trade(order.timestamp, "vis_exec", order.direction, order.size, order.price, order.order_id)
        side = getattr(self, f'{order.direction}s')
        if order.price not in side:
            logger.warning("Warning _execute_vis_order: Price %s not found on %s side.\n"
                           "Order info: %s", order.price, order.direction, order)
            return

        if order.order_id not in side[order.price]:
            logger.warning("Warning _execute_vis_order: Order ID %s not found at price %s on %s side.\n"
                           "Order info: %s", order.order_id, order.price, order.direction, order)
            return

        side[order.price][order.order_id].size -= order.size

        if side[order.price][order.order_id].size <= 0:
            del side[order.price][order.order_id]

        if not side[order.price]:
            del side[order.price]

    def _cancel_order(self, order: Order) -> None:
        """
        Cancel a portion of a resting order.
        Does not update timestamp of remaining limit order (i.e. affected order keeps the same timestamp from when it was added)
        Remaining limit order keeps its position in the order queue

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.

        Warns
        -----
        UserWarning
            If the price or order ID is not found in the book.
        """
        self._update_DOFI(order)
        side = getattr(self, f'{order.direction}s')
        if order.price not in side:
            logger.warning("Warning _cancel_order: Price %s not found on %s side.\n"
                           "Order info: %s", order.price, order.direction, order)
            return

        if order.order_id not in side[order.price]:
            logger.warning("Warning _cancel_order: Order ID %s not found at price %s on %s side.\n"
                           "Order info: %s", order.order_id, order.price, order.direction, order)
            return

        side[order.price][order.order_id].size -= order.size

        if side[order.price][order.order_id].size <= 0:
            del side[order.price][order.order_id]

        if not side[order.price]:
            del side[order.price]

    def _delete_order(self, order: Order):
        """
        Remove a resting order entirely from the book.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.

        Warns
        -----
        UserWarning
            If the price or order ID is not found in the book.
        """
        self._update_DOFI(order)
        side = getattr(self, f'{order.direction}s')
        if order.price in side:
            if order.order_id in side[order.price]:
                del side[order.price][order.order_id]
                if not side[order.price]:
                    del side[order.price]
            else:
                logger.warning("Warning _delete_order: Price %s not found on %s side.\n"
                             "Order info: %s", order.price, order.direction, order)
                return
        else:
            logger.warning("Warning _delete_order: Order ID %s not found at price %s on %s side.\n"
                         "Order info: %s", order.order_id, order.price, order.direction, order)
            return

    def _handle_hidden_exec(self, order: Order):
        """
        Record a hidden execution (not visible in the book).

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.
        """
        self._record_trade(order.timestamp, "hid_exec", order.direction, order.size, order.price, order.order_id)

    # --------------------------
    # OFI helpers
    # --------------------------
    def reset_cum_OFI(self):
        """
        Reset the cumulative Order Flow Imbalance (OFI) counters.
        """
        self.cum_OFI.reset()

    def _update_LOFI(self, order: Order | LimitOrder):
        """
        Update Limit Order Flow Imbalance (LOFI) given a new limit order.

        Parameters
        ----------
        order : Order or LimitOrder


        """
        if order.direction == 'bid' and order.price >= self.highest_bid_price():
            self.cum_OFI.Lb.size += order.size
            self.cum_OFI.Lb.count += 1
        elif order.direction == 'ask' and order.price <= self.lowest_ask_price():
            self.cum_OFI.La.size += order.size
            self.cum_OFI.La.count += 1

    def _update_MOFI(self, order: Order):
        """
        Update Market Order Flow Imbalance (MOFI) given a visible execution.

        Parameters
        ----------
            order : Order
                Order object containing event details. See :class:`Order`
                in `orders.py` for full definition.
        """
        if order.price == self.highest_bid_price() and order.direction == 'bid':
            self.cum_OFI.Mb.size += order.size
            self.cum_OFI.Mb.count += 1
        elif order.price == self.lowest_ask_price() and order.direction == 'ask':
            self.cum_OFI.Ma.size += order.size
            self.cum_OFI.Ma.count += 1

    def _update_DOFI(self, order: Order):
        """
        Update Deletion Order Flow Imbalance (DOFI) given a cancel/delete.

        Parameters
        ----------
        order : Order
            Order object containing event details. See :class:`Order`
            in `orders.py` for full definition.
        """
        if order.price == self.highest_bid_price() and order.direction == 'bid':
            self.cum_OFI.Db.size += order.size
            self.cum_OFI.Db.count += 1
        elif order.price == self.lowest_ask_price() and order.direction == 'ask':
            self.cum_OFI.Da.size += order.size
            self.cum_OFI.Da.count += 1

    # --------------------------
    # Visualization
    # --------------------------
    def convert_orderbook_to_L2_dataframe(self) -> pd.DataFrame:
        """
        Convert the current order book state into a DataFrame containing L2 data.
        Captures nlevels of data (specified when orderbook is initialized).

        Returns
        -------
        DataFrame
            Pandas DataFrame with columns:
            - `direction` : {"bid", "ask"}
            - `price` : int
            - `size` : int (aggregate volume at price level)
        """
        order_dict = {}
        for direction in ["bid", "ask"]:
            prices = getattr(self, f'{direction}s')
            for level, price in enumerate(prices):
                if level >= self.nlevels:
                    break
                total_volume = sum(order.size for order in getattr(self, f'{direction}s')[price].values())  # type: ignore
                order_dict[direction + "_" + str(level)] = (direction, price, total_volume)
        df = pd.DataFrame(order_dict).T
        return df.rename(columns={0: "direction", 1: "price", 2: "size"})

    def convert_orderbook_to_L3_dataframe(self) -> pd.DataFrame:
        """
        Convert the current order book state into a DataFrame containing L3 data.
        Captures nlevels of data (specified when orderbook is initialized).

        Returns
        -------
        DataFrame
            Pandas DataFrame with columns:
            - `direction` : {"bid", "ask"}
            - `price` : int
            - `size` : int (aggregate volume at price level)
        """
        orders = []
        for direction in ["bid", "ask"]:
            prices = getattr(self, f'{direction}s')
            for level, price in enumerate(prices):
                if level >= self.nlevels:
                    break
                for order in prices[price].values():
                    orders.append((direction, price, order.size))
        df = pd.DataFrame(orders)
        return df.rename(columns={0: "direction", 1: "price", 2: "size"})

    def display_L2_order_book(self) -> None:
        """
        Display the L2 order book as a bar chart.

        Uses Plotly to show aggregate volume at each price level.

        Warns
        -----
        UserWarning
            If the order book is empty or plotting fails.
        """
        try:
            df = self.convert_orderbook_to_L2_dataframe()
            df.price = df.price * self.price_scaling
            fig = px.bar(
                df,
                orientation='h',
                x="size",
                y="price",
                color="direction",
                title=f"{self.ticker}<br><sup>{format_timestamp(self.curr_book_timestamp)}",
                color_discrete_sequence=["green", "red"]
            )
            fig.update_traces(width=self.tick_size)
            fig.show()
        except Exception:
            warnings.warn("display_L2_order_book failed; returning nothing. "
                          "Check if the orderbook is populated before calling.")
            logger.exception("Failed to display L2 orderbook")

    def display_L3_order_book(self) -> None:
        """
        Display the L3 order book as a bar chart.

        Uses Plotly to show aggregate volume at each price level.

        Warns
        -----
        UserWarning
            If the order book is empty or plotting fails.
        """
        try:
            df = self.convert_orderbook_to_L3_dataframe()
            df.price = df.price * self.price_scaling
            fig = px.bar(
                df,
                orientation='h',
                x="size",
                y="price",
                color="direction",
                title=f"{self.ticker}<br><sup>{format_timestamp(self.curr_book_timestamp)}",
                color_discrete_sequence=["green", "red"]
            )
            fig.update_traces(width=self.tick_size)
            fig.show()
        except Exception:
            warnings.warn("display_L3_order_book failed; returning nothing. "
                          "Check if the orderbook is populated before calling.")
            logger.exception("Failed to display L3 orderbook")

    def _get_L3_plot_traces(self) -> tuple[BaseTraceType]:
        """
        Extract Plotly traces for L3 visualization.

        Returns
        -------
        tuple of BaseTraceType
            Traces representing L3 order book bars.
        """
        try:
            df = self.convert_orderbook_to_L3_dataframe()
            df.price = df.price * self.price_scaling
            traces = px.bar(
                df,
                orientation='h',
                x="size",
                y="price",
                color="direction",
                color_discrete_sequence=["green", "red"]
            )
            return traces.data
        except Exception:
            logger.exception("Failed to extract L3 trace")

    def _get_L2_plot_traces(self) -> tuple[BaseTraceType]:
        """
        Extract Plotly traces for L3 visualization.

        Returns
        -------
        tuple of BaseTraceType
            Traces representing L3 order book bars.
        """
        try:
            df = self.convert_orderbook_to_L2_dataframe()
            df.price = df.price * self.price_scaling
            traces = px.bar(
                df,
                orientation='h',
                x="size",
                y="price",
                color="direction",
                color_discrete_sequence=["green", "red"]
            )
            return traces.data
        except Exception:
            logger.exception("Failed to extract L3 trace")

    # --------------------------
    # Feature engineering
    # --------------------------
    def lowest_ask_price(self) -> int:
        """
        Get the current lowest ask price.

        Returns
        -------
        int
            Lowest ask price, or np.inf if no asks exist.
        """
        return next(iter(self.asks), np.inf)

    def highest_bid_price(self) -> int:
        """
        Get the current highest bid price.

        Returns
        -------
        int
            Highest bid price, or 0 if no bids exist.
        """
        return next(iter(self.bids), 0)

    def lowest_ask_volume(self) -> int:
        """
        Get the total volume at the best ask price.

        Returns
        -------
        int
            Aggregate size of orders at the lowest ask.
        """
        return sum(order.size for order in self.asks[self.lowest_ask_price()].values())

    def highest_bid_volume(self) -> int:
        """
        Get the total volume at the best bid price.

        Returns
        -------
        int
            Aggregate size of orders at the highest bid.
        """
        return sum(order.size for order in self.bids[self.highest_bid_price()].values())

    def bid_ask_spread(self) -> int:
        """
        Compute the bid-ask spread.

        Returns
        -------
        int
            Difference between lowest ask and highest bid price.
        """
        return self.lowest_ask_price() - self.highest_bid_price()

    def mid_price(self) -> float | None:
        """
        Compute the midprice.

        Returns
        -------
        float or None
            Midprice if both sides exist, else None.
        """
        if not self.bids or not self.asks:
            return None
        return (self.highest_bid_price() + self.lowest_ask_price()) / 2

    def worst_ask_price(self) -> int:
        """
        Get the worst (highest) ask price in the book.

        Returns
        -------
        int
            Worst ask price.
        """
        return self.asks.peekitem(index=-1)[0]

    def worst_bid_price(self) -> int:
        """
        Get the worst (lowest) bid price in the book.

        Returns
        -------
        int
            Worst bid price.
        """
        return self.bids.peekitem(index=-1)[0]

    def orderbook_price_range(self) -> int:
        """
        Get the price range spanned by the order book.

        Returns
        -------
        int
            Difference between worst ask and worst bid.
        """
        return self.worst_ask_price() - self.worst_bid_price()

    def calc_size_OFI(self) -> int:
        """
        Compute size-based Order Flow Imbalance (OFI).

        Returns
        -------
        int
            Net OFI based on order sizes.
        """
        return self.cum_OFI.Lb.size - self.cum_OFI.Db.size + self.cum_OFI.Mb.size - self.cum_OFI.La.size + self.cum_OFI.Da.size - self.cum_OFI.Ma.size

    def calc_count_OFI(self) -> int:
        """
        Compute count-based Order Flow Imbalance (OFI).

        Returns
        -------
        int
            Net OFI based on order counts.
        """
        return self.cum_OFI.Lb.count - self.cum_OFI.Db.count + self.cum_OFI.Mb.count - self.cum_OFI.La.count + self.cum_OFI.Da.count - self.cum_OFI.Ma.count

    def available_vol_at_price(self, price: int) -> int:
        """
        Get total volume available at a given price.

        Parameters
        ----------
        price : int
            Price level.

        Returns
        -------
        int
            Aggregate volume at the specified price.
        """
        total_volume = 0
        if price in self.asks:
            total_volume += sum(order.size for order in self.asks[price].values())
        if price in self.bids:
            total_volume += sum(order.size for order in self.bids[price].values())
        return total_volume

    def total_ask_volume(self) -> int:
        """
        Get total volume on the ask side.

        Returns
        -------
        int
            Sum of sizes across all ask levels.
        """
        total_volume = 0
        for level in self.asks.values():
            for order in level.values():
                total_volume += order.size
        return total_volume

    def total_bid_volume(self) -> int:
        """
        Get total volume on the bid side.

        Returns
        -------
        int
            Sum of sizes across all bid levels.
        """
        total_volume = 0
        for level in self.bids.values():
            for order in level.values():
                total_volume += order.size
        return total_volume

    def volume_of_higher_priority_orders(self, order: LimitOrder) -> int:
        """
        Get the total size of orders ahead of a given order in priority.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        int
            Volume of higher-priority orders on the same side.
        """
        side = getattr(self, f'{order.direction}s')
        total_volume = 0
        for price, level in side.items():
            if (order.price > price) if order.direction == 'bid' else (order.price < price):
                return total_volume
            for o in level.values():
                total_volume += o.size
        return total_volume

    def symmetric_opposite_book_volume(self, order: LimitOrder) -> int:
        """
        Compute volume on the opposite side symmetric to the order price.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        int
            Symmetric opposite-side volume.
        """
        side = self.asks if order.direction == 'bid' else self.bids
        symmetric_price = 2*self.mid_price() - order.price
        total = 0
        if order.direction == 'bid':
            if order.price >= self.mid_price(): return 0
            for price, level in side.items():
                if price >= symmetric_price:
                    break
                for o in level.values():
                    total += o.size
        else:
            if order.price <= self.mid_price(): return 0
            for price, level in side.items():
                if price <= symmetric_price:
                    break
                for o in level.values():
                    total += o.size
        return total

    def opposite_side_book_depth(self, order: LimitOrder) -> int:
        """
        Get total depth of the opposite side of the book.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        int
            Total volume on the opposite side.
        """
        if order.direction == 'ask':
            return self.total_bid_volume()
        else:
            return self.total_ask_volume()

    def same_side_book_depth(self, order: LimitOrder) -> int:
        """
        Get total depth of the same side of the book.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        int
            Total volume on the same side.
        """
        return getattr(self, f'total_{order.direction}_volume')()

    def time_elapsed_since_first_available_order_with_same_price(self, order: LimitOrder) -> float:
        """
        Compute time elapsed since the first order at the same price.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        float
            Time in seconds.
        """
        side = getattr(self, f'{order.direction}s')
        first_order = next(iter(side[order.price].values()), None)
        if first_order:
            return order.timestamp - first_order.timestamp
        return 0

    def time_elapsed_since_most_recent_order_with_same_price(self, order: LimitOrder) -> float:
        """
        Compute time elapsed since the most recent order at the same price.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        float
            Time in seconds.
        """
        side = getattr(self, f'{order.direction}s')
        recent_order = next(reversed(side[order.price].values()), None)
        if recent_order:
            return order.timestamp - recent_order.timestamp
        return 0

    def time_elapsed_since_mid_price_change(self, order: LimitOrder) -> float:
        """
        Compute time elapsed since the last midprice change.

        Parameters
        ----------
        order : LimitOrder
            LimitOrder object containing event details. See :class:`LimitOrder`
            in `orders.py` for full definition.

        Returns
        -------
        float
            Time in seconds.
        """
        return order.timestamp - self.midprice_change_timestamp

    def meta_orders(self, time_delta=0) -> List[List[namedtuple]]:
        """
        Group trades into meta-orders based on time and type.

        Parameters
        ----------
        time_delta : float, default=0
            Maximum allowed gap between trades to group.

        Returns
        -------
        list of list of Trades (namedtuple("Trade", ["timestamp", "trade_type", "direction", "size", "price", "order_id"])
            Grouped meta-orders.
        """
        meta_orders = []
        i = 0
        while i < len(self.trade_log):
            group = [self.trade_log[i]]
            j = i + 1
            while (
                    j < len(self.trade_log) and
                    self.trade_log[j].timestamp - self.trade_log[i].timestamp <= time_delta and
                    self.trade_log[i].trade_type == self.trade_log[j].trade_type
            ):
                group.append(self.trade_log[j])
                j += 1
            meta_orders.append(group)
            i = j
        return meta_orders

    def order_sweeps(self, time_delta=0, level_threshold=2) -> List[List[namedtuple]]:
        """
        Identify order sweeps (large meta-orders across levels).

        Parameters
        ----------
        time_delta : float, default=0
            Maximum allowed gap between trades to group.
        level_threshold : int, default=2
            Minimum number of unique price levels to qualify as a sweep.

        Returns
        -------
        list of list of Trade (namedtuple("Trade", ["timestamp", "trade_type", "direction", "size", "price", "order_id"])
            List of order sweeps.
        """
        meta_orders = self.meta_orders(time_delta)
        order_sweeps = []
        for meta_order in meta_orders:
            unique_prices = set()
            for order in meta_order:
                unique_prices.add(order.price)
            if len(unique_prices) >= level_threshold:
                order_sweeps.append(meta_order)
        return order_sweeps



