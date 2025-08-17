import logging

from src.lob_reconstuctor.lobster_sim import *
import threading
import os

#temporary file to test orderbook

## File Parameters ##
os.chdir("/Users/lucfiechter/Documents")
ticker = "FTV"
file_start_date = "2016-07-05"
starttime = 24900000
endtime = 57900000
nlevels = 10
logging.basicConfig(
    level=logging.ERROR,
    format="%(levelname)s:%(name)s:%(message)s"
)
### Change Me ######

MSGBOOK = f"{ticker}_{file_start_date}_{starttime}_{endtime}_message_{nlevels}.csv"
OBOOK = f"{ticker}_{file_start_date}_{starttime}_{endtime}_orderbook_{nlevels}.csv"



test_lob = Orderbook(nlevels, ticker, 0.01)
test = LobsterSim(test_lob, MSGBOOK, OBOOK)

# test._validate_lobster_data()
# test._check_full_book(1)


# test.simulate_until(34200)
# test._check_books_match()
app = test.create_animated_L3_app(34200, 34500, 1)
def run_dash():
    app.run(debug=False, use_reloader=False, host='127.0.0.1', port=8050)

threading.Thread(target=run_dash, daemon=True).start()

test.size_OFI_graph(35000, 36800, 5)
test.count_OFI_graph(35000, 36800, 5)

test.plot_price_levels_heatmap(35000, 36800, 5)
test.midprice_graph(35000, 36800, 5)
test.depth_percentile_graph(35000, 36800, 5)
test.display_L3_snapshots(32400, 57600, 3600)




# test.simulate_until(35000)
# # meta_orders = test.orderbook.meta_orders()
# # filtered = [sublist for sublist in meta_orders if len(sublist) >= 2]
# # print(filtered)
# print(test.orderbook.order_sweeps())
# test.graph_trade_arrival_time(34200,57600, filter_trade_type="hid_exec")
# test.graph_trade_arrival_time(34200, 57600)
# test.graph_trade_size_distribution(34200, 57600)
# #
input("Press Enter to exit the Dash app...\n")
# test.display_L2_snapshots(32400, 57600, 3600)
# test_order = LimitOrder(36001, -5, 20, test.orderbook.highest_bid_price(), 'bid')
# print(test.orderbook.symmetric_opposite_book_volume(test_order))
# print(test.orderbook.time_elapsed_since_first_available_order_with_same_price(test_order))
# print(test.orderbook.time_elapsed_since_mid_price_change(test_order))
# print(test.orderbook.time_elapsed_since_most_recent_order_with_same_price(test_order))
#
#
#
# start = time.perf_counter()
# df = AAPL_lob.convert_orderbook_to_L3_dataframe()
# print(f"Conversion took {time.perf_counter() - start:.4f}s")