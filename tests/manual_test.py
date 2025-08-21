import unittest
import numpy as np
from src.lob_reconstructor.orderbook import Orderbook
from src.lob_reconstructor.orders import Order, LimitOrder

class TestOrderbookBasic(unittest.TestCase):
    def setUp(self):
        self.book = Orderbook(nlevels=5, ticker="TEST", tick_size=1, price_scaling=0.01)

    def submit_order(self, **kwargs):
        order = Order(**kwargs)
        self.book.process_order(order)
        return order

    def test_limit_order_addition(self):
        order = self.submit_order(
            timestamp=1.0,
            event_type='submit',
            order_id=1,
            size=100,
            price=101,
            direction='bid'
        )
        self.assertEqual(self.book.total_bid_volume(), 100)
        self.assertEqual(len(self.book.bids[101]), 1)

    def test_limit_order_crossing_spread(self):
        self.submit_order(
            timestamp=1.0,
            event_type='submit',
            order_id=1,
            size=100,
            price=101,
            direction='ask'
        )
        self.submit_order(
            timestamp=1.1,
            event_type='submit',
            order_id=2,
            size=100,
            price=102,
            direction='bid'
        )
        self.assertEqual(self.book.total_bid_volume(), 0)
        self.assertEqual(self.book.total_ask_volume(), 0)

    def test_partial_fill_and_resting_order(self):
        self.submit_order(
            timestamp=1.0,
            event_type='submit',
            order_id=1,
            size=50,
            price=101,
            direction='ask'
        )
        self.submit_order(
            timestamp=1.1,
            event_type='submit',
            order_id=2,
            size=100,
            price=102,
            direction='bid'
        )
        self.assertEqual(self.book.total_bid_volume(), 50)
        self.assertEqual(self.book.total_ask_volume(), 0)

    def test_cancel_order(self):
        self.submit_order(
            timestamp=1.0,
            event_type='submit',
            order_id=1,
            size=100,
            price=100,
            direction='ask'
        )
        self.submit_order(
            timestamp=1.1,
            event_type='cancel',
            order_id=1,
            size=50,
            price=100,
            direction='ask'
        )
        self.assertEqual(self.book.total_ask_volume(), 50)

    def test_delete_order(self):
        self.submit_order(
            timestamp=1.0,
            event_type='submit',
            order_id=1,
            size=100,
            price=100,
            direction='ask'
        )
        self.submit_order(
            timestamp=1.1,
            event_type='delete',
            order_id=1,
            size=100,
            price=100,
            direction='ask'
        )
        self.assertEqual(self.book.total_ask_volume(), 0)

    def test_execute_visible_order(self):
        self.submit_order(
            timestamp=1.0,
            event_type='submit',
            order_id=1,
            size=100,
            price=100,
            direction='ask'
        )
        self.submit_order(
            timestamp=1.1,
            event_type='vis_exec',
            order_id=1,
            size=100,
            price=100,
            direction='ask'
        )
        self.assertEqual(self.book.total_ask_volume(), 0)

    def test_midprice_tracking(self):
        # Adding a worse-side ask should not change the mid price or its timestamp
        self.submit_order(timestamp=1.0, event_type='submit', order_id=1, size=100, price=100, direction='bid')
        self.submit_order(timestamp=1.1, event_type='submit', order_id=2, size=100, price=104, direction='ask')
        mid1 = self.book.mid_price()
        self.submit_order(timestamp=1.2, event_type='submit', order_id=3, size=100, price=105, direction='ask')
        mid2 = self.book.mid_price()
        self.assertEqual(mid1, mid2)
        self.assertEqual(self.book.midprice_change_timestamp, 0.0)

    def test_ofi_count_size(self):
        # initial OFI zero
        self.assertEqual(self.book.calc_size_OFI(), 0)
        self.assertEqual(self.book.calc_count_OFI(), 0)
        # mixed events
        self.submit_order(timestamp=1.0, event_type='submit', order_id=1, size=20, price=100, direction='bid')
        self.submit_order(timestamp=1.1, event_type='cancel', order_id=1, size=20, price=100, direction='bid')
        self.submit_order(timestamp=1.2, event_type='vis_exec', order_id=1, size=0, price=100, direction='bid')
        self.assertEqual(self.book.calc_size_OFI(), 0)
        self.assertEqual(self.book.calc_count_OFI(), 0)

    def test_orderbook_snapshots(self):
        # build simple book
        for i, (sz, pr) in enumerate([(100,100), (200,101), (300,102), (400,103)], start=1):
            self.submit_order(
                timestamp=1.0,
                event_type='submit',
                order_id=i,
                size=sz,
                price=pr,
                direction=('bid' if pr < 102 else 'ask')
            )
        l2 = self.book.convert_orderbook_to_L2_dataframe()
        l3 = self.book.convert_orderbook_to_L3_dataframe()
        self.assertTrue(len(l2) > 0)
        self.assertTrue(len(l3) > 0)

    def test_reset_ofi(self):
        # reset should clear OFI counters
        self.submit_order(timestamp=1.0, event_type='submit', order_id=1, size=50, price=100, direction='bid')
        self.book.reset_cum_OFI()
        self.assertEqual(self.book.calc_size_OFI(), 0)
        self.assertEqual(self.book.calc_count_OFI(), 0)


class TestOrderbookEdgeCases(unittest.TestCase):
    def setUp(self):
        self.book = Orderbook(nlevels=3, ticker="EDGE", tick_size=1, price_scaling=0.01)

    def test_empty_book_prices_and_volumes(self):
        self.assertEqual(self.book.lowest_ask_price(), np.inf)
        self.assertEqual(self.book.highest_bid_price(), 0)
        self.assertIsNone(self.book.mid_price())
        self.assertEqual(self.book.bid_ask_spread(), np.inf)
        self.assertEqual(self.book.total_bid_volume(), 0)
        self.assertEqual(self.book.total_ask_volume(), 0)

    def test_single_side_volume_and_available_vol(self):
        # only bids
        o1 = Order(timestamp=1.0, event_type='submit', order_id=1, size=10, price=100, direction='bid')
        self.book.process_order(o1)
        self.assertEqual(self.book.total_bid_volume(), 10)
        self.assertEqual(self.book.available_vol_at_price(100), 10)
        self.assertEqual(self.book.available_vol_at_price(200), 0)
        # only asks
        self.book.clear_orderbook()
        o2 = Order(timestamp=2.0, event_type='submit', order_id=2, size=5, price=105, direction='ask')
        self.book.process_order(o2)
        self.assertEqual(self.book.total_ask_volume(), 5)
        self.assertEqual(self.book.available_vol_at_price(105), 5)

    def test_worst_and_price_range(self):
        # two-phase timestamp
        bids = [(100,10), (101,20), (102,30)]
        t = 1.0
        for p,sz in bids:
            t += 0.1
            self.book.process_order(Order(timestamp=t, event_type='submit', order_id=p, size=sz, price=p, direction='bid'))
        asks = [(103,10), (104,20), (105,30)]
        t = 2.0
        for p,sz in asks:
            t += 0.1
            self.book.process_order(Order(timestamp=t, event_type='submit', order_id=-p, size=sz, price=p, direction='ask'))
        self.assertEqual(self.book.worst_bid_price(), 100)
        self.assertEqual(self.book.worst_ask_price(), 105)
        self.assertEqual(self.book.orderbook_price_range(), 5)

    def test_priority_and_depth(self):
        bids = [(100,10), (101,15), (102,5)]
        asks = [(103,8), (104,12)]
        for i,(p,sz) in enumerate(bids, start=1):
            self.book.process_order(Order(timestamp=1+i*0.1, event_type='submit', order_id=i, size=sz, price=p, direction='bid'))
        for i,(p,sz) in enumerate(asks, start=10):
            self.book.process_order(Order(timestamp=2+i*0.1, event_type='submit', order_id=i, size=sz, price=p, direction='ask'))
        lo = LimitOrder(timestamp=3.0, order_id=99, size=1, price=101, direction='bid')
        higher_vol = self.book.volume_of_higher_priority_orders(lo)
        self.assertEqual(higher_vol, 20)
        lo2 = LimitOrder(timestamp=3.0, order_id=100, size=1, price=101, direction='ask')
        sym_vol = self.book.symmetric_opposite_book_volume(lo2)
        self.assertEqual(sym_vol, 0)
        self.assertEqual(self.book.opposite_side_book_depth(lo), self.book.total_ask_volume())
        self.assertEqual(self.book.same_side_book_depth(lo), self.book.total_bid_volume())

    def test_time_elapsed_functions(self):
        now = 1.0
        self.book.process_order(Order(timestamp=now, event_type='submit', order_id=1, size=10, price=100, direction='bid'))
        self.book.process_order(Order(timestamp=now+5, event_type='submit', order_id=2, size=10, price=100, direction='bid'))
        lo = LimitOrder(timestamp=now+10, order_id=3, size=5, price=100, direction='bid')
        t_first = self.book.time_elapsed_since_first_available_order_with_same_price(lo)
        t_recent = self.book.time_elapsed_since_most_recent_order_with_same_price(lo)
        self.assertEqual(t_first, lo.timestamp - now)
        self.assertEqual(t_recent, lo.timestamp - (now+5))
        t_mid = self.book.time_elapsed_since_mid_price_change(lo)
        self.assertEqual(t_mid, lo.timestamp - self.book.midprice_change_timestamp)

    def test_ofi_edge_cases(self):
        # no activity
        self.assertEqual(self.book.calc_size_OFI(), 0)
        self.assertEqual(self.book.calc_count_OFI(), 0)
        # mixed
        self.book.process_order(Order(timestamp=1.0, event_type='submit', order_id=1, size=20, price=100, direction='bid'))
        self.book.process_order(Order(timestamp=1.1, event_type='cancel', order_id=1, size=20, price=100, direction='bid'))
        self.book.process_order(Order(timestamp=1.2, event_type='vis_exec', order_id=1, size=0, price=100, direction='bid'))
        self.assertEqual(self.book.calc_size_OFI(), 0)
        self.assertEqual(self.book.calc_count_OFI(), 0)

if __name__ == '__main__':
    unittest.main(verbosity=2)

