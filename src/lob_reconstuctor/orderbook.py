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


    def clear_orderbook(self):
        self.bids.clear()
        self.asks.clear()
        self.curr_book_timestamp = 0.0
        self.midprice = None
        self.midprice_change_timestamp = 0.0
        self.reset_cum_OFI()
        self.trade_log.clear()

    def clear_trade_log(self):
        self.trade_log.clear()

    # ----------------------------------
    # Order Processing Handler & Helpers
    # ----------------------------------
    def process_order(self, order: Order) -> None:
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

    def _does_order_cross_spread(self,order: Order):
        if order.direction == 'bid':
            return order.price >= self.lowest_ask_price()
        if order.direction == 'ask':
            return order.price <= self.highest_bid_price()
        if order.direction=='ask' and not self.bids:
            return False
        if order.direction=='bid' and not self.asks:
            return False

    def _record_trade(self, timestamp: float, trade_type: Literal["vis_exec", "aggro_lim", "hid_exec"], direction: Literal["bid", "ask"], size: int, price: int, order_id: int):
        Trade = namedtuple("Trade", ["timestamp", "trade_type", "direction", "size", "price", "order_id"])
        trade = Trade(timestamp, trade_type, direction, size, price, order_id)
        self.trade_log.append(trade)
        #bid direction means a bid limit order was matched; ask direction means an ask limit order was matched

    def _add_order(self, order: Order):
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

    def _execute_against_opposite_book(self, order: Order):
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

    def _execute_visible_order(self, order: Order):
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

    def _cancel_order(self, order: Order):
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
        self._record_trade(order.timestamp, "hid_exec", order.direction, order.size, order.price, order.order_id)

    # --------------------------
    # OFI helpers
    # --------------------------
    def reset_cum_OFI(self):
        self.cum_OFI.reset()

    def _update_LOFI(self, order: Order | LimitOrder):
        if order.direction == 'bid' and order.price >= self.highest_bid_price():
            self.cum_OFI.Lb.size += order.size
            self.cum_OFI.Lb.count += 1
        elif order.direction == 'ask' and order.price <= self.lowest_ask_price():
            self.cum_OFI.La.size += order.size
            self.cum_OFI.La.count += 1

    def _update_MOFI(self, order: Order):
        if order.price == self.highest_bid_price() and order.direction == 'bid':
            self.cum_OFI.Mb.size += order.size
            self.cum_OFI.Mb.count += 1
        elif order.price == self.lowest_ask_price() and order.direction == 'ask':
            self.cum_OFI.Ma.size += order.size
            self.cum_OFI.Ma.count += 1

    def _update_DOFI(self, order: Order):
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
        return next(iter(self.asks), np.inf)

    def highest_bid_price(self) -> int:
        return next(iter(self.bids), 0)

    def lowest_ask_volume(self) -> int:
        return sum(order.size for order in self.asks[self.lowest_ask_price()].values())

    def highest_bid_volume(self) -> int:
        return sum(order.size for order in self.bids[self.highest_bid_price()].values())

    def bid_ask_spread(self) -> int:
        return self.lowest_ask_price() - self.highest_bid_price()

    def mid_price(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.highest_bid_price() + self.lowest_ask_price()) / 2

    def worst_ask_price(self) -> int:
        return self.asks.peekitem(index=-1)[0]

    def worst_bid_price(self) -> int:
        return self.bids.peekitem(index=-1)[0]

    def orderbook_price_range(self) -> int:
        return self.worst_ask_price() - self.worst_bid_price()

    def calc_size_OFI(self) -> int:
        return self.cum_OFI.Lb.size - self.cum_OFI.Db.size + self.cum_OFI.Mb.size - self.cum_OFI.La.size + self.cum_OFI.Da.size - self.cum_OFI.Ma.size

    def calc_count_OFI(self) -> int:
        return self.cum_OFI.Lb.count - self.cum_OFI.Db.count + self.cum_OFI.Mb.count - self.cum_OFI.La.count + self.cum_OFI.Da.count - self.cum_OFI.Ma.count

    def available_vol_at_price(self, price: int) -> int:
        total_volume = 0
        if price in self.asks:
            total_volume += sum(order.size for order in self.asks[price].values())
        if price in self.bids:
            total_volume += sum(order.size for order in self.bids[price].values())
        return total_volume

    def total_ask_volume(self) -> int:
        total_volume = 0
        for level in self.asks.values():
            for order in level.values():
                total_volume += order.size
        return total_volume

    def total_bid_volume(self) -> int:
        total_volume = 0
        for level in self.bids.values():
            for order in level.values():
                total_volume += order.size
        return total_volume

    def volume_of_higher_priority_orders(self, order: LimitOrder) -> int:
        side = getattr(self, f'{order.direction}s')
        total_volume = 0
        for price, level in side.items():
            if (order.price > price) if order.direction == 'bid' else (order.price < price):
                return total_volume
            for o in level.values():
                total_volume += o.size
        return total_volume

    def symmetric_opposite_book_volume(self, order: LimitOrder) -> int:
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
        if order.direction == 'ask':
            return self.total_bid_volume()
        else:
            return self.total_ask_volume()

    def same_side_book_depth(self, order: LimitOrder) -> int:
        return getattr(self, f'total_{order.direction}_volume')()

    def time_elapsed_since_first_available_order_with_same_price(self, order: LimitOrder) -> float:
        side = getattr(self, f'{order.direction}s')
        first_order = next(iter(side[order.price].values()), None)
        if first_order:
            return order.timestamp - first_order.timestamp
        return 0

    def time_elapsed_since_most_recent_order_with_same_price(self, order: LimitOrder) -> float:
        side = getattr(self, f'{order.direction}s')
        recent_order = next(reversed(side[order.price].values()), None)
        if recent_order:
            return order.timestamp - recent_order.timestamp
        return 0

    def time_elapsed_since_mid_price_change(self, order: LimitOrder) -> float:
        return order.timestamp - self.midprice_change_timestamp

    def meta_orders(self, time_delta=0) -> List[List[namedtuple("Trade", ["timestamp", "trade_type", "direction", "size", "price", "order_id"])]]:
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

    def order_sweeps(self, time_delta=0, level_threshold=2) -> List[List[namedtuple("Trade", ["timestamp", "trade_type", "direction", "size", "price", "order_id"])]]:
        meta_orders = self.meta_orders(time_delta)
        order_sweeps = []
        for meta_order in meta_orders:
            unique_prices = set()
            for order in meta_order:
                unique_prices.add(order.price)
            if len(unique_prices) >= level_threshold:
                order_sweeps.append(meta_order)
        return order_sweeps



