import os
import pandas as pd
from src.lob_reconstuctor.orderbook import Orderbook
from src.lob_reconstuctor.orders import LimitOrder
from src.lob_reconstuctor.lobster_sim import LobsterSim

# Change working directory to message book location
#os.chdir(r"C:\Users\komal\Downloads\AAPL_Jan2019")

# Configuration parameters
ticker = "AAPL"
file_start_date = "2019-01-02"
starttime = 24900000
endtime = 57900000
nlevels = 10
snapshot_time = 55000

# Message book filename
MSGBOOK = r"C:\Users\komal\Downloads\AAPL_Jan2019\AAPL_2019-01-02_24900000_57900000_message_10.csv"

# Initialize Orderbook and LobsterSim
lob = Orderbook(nlevels, ticker, 0.01)
sim = LobsterSim(lob, MSGBOOK)

# # 1. Simulate up to snapshot_time
# sim.simulate_until(snapshot_time)
# print(f"Simulated until {snapshot_time}. Current timestamp: {lob.curr_book_timestamp}")

# # 2. Simulate one second further
# sim.simulate_from_current_until(snapshot_time + 1)
# print(f"Simulated from current until {snapshot_time + 1}. Current timestamp: {lob.curr_book_timestamp}")

# # 3. Display L3 and L2 snapshots (09:00–10:00 at 30‑minute intervals)
# sim.display_L3_snapshots(32400, 36000, 1800)
# sim.display_L2_snapshots(32400, 36000, 1800)

# # 4. Test Orderbook utility functions
# order = LimitOrder(
#     timestamp=lob.curr_book_timestamp + 1,
#     order_id=-1,
#     size=10,
#     price=lob.highest_bid_price(),
#     direction='bid'
# )
# print("Highest bid price:", lob.highest_bid_price())
# print("Lowest ask price:", lob.lowest_ask_price())
# print("Mid price:", lob.midprice)
# print("Spread:", lob.bid_ask_spread())

# print("Symmetric opposite book volume:", lob.symmetric_opposite_book_volume(order))
# print("Time since first available same price:", lob.time_elapsed_since_first_available_order_with_same_price(order))
# print("Time since mid price change:", lob.time_elapsed_since_mid_price_change(order))
# print("Time since most recent same price:", lob.time_elapsed_since_most_recent_order_with_same_price(order))

# # 5. Convert to L3 DataFrame and measure performance
# start = time.perf_counter()
# df = lob.convert_orderbook_to_L3_dataframe()
# print(f"Conversion to L3 DataFrame took {time.perf_counter() - start:.4f}s")
# print(df.head())

# # 6. If OFI method exists, compute it
# try:
#     ofi = lob.compute_order_flow_imbalance(order)
#     print("Order Flow Imbalance:", ofi)
# except AttributeError:
#     print("compute_order_flow_imbalance not implemented")

# # 7. Generate plotly traces directly
# l3_traces = lob._get_L3_plot_traces()
# l2_traces = lob._get_L2_plot_traces()
# print(f"L3 plot traces count: {len(l3_traces)}")
# print(f"L2 plot traces count: {len(l2_traces)}")

# # 8. Verify animate_L3_book exists
# print("animate_L3_book method exists:", hasattr(sim, "animate_L3_book"))

# # 9. Smoke‐test animate_L3_book for a single frame (won’t hang, just builds one frame and exits)
# try:
#     # we pick start=end so it only needs to render one frame
#     sim.animate_L3_book(snapshot_time, snapshot_time, snapshot_time)
#     print("animate_L3_book single‐frame smoke test: SUCCESS")
# except Exception as e:
#     print("animate_L3_book single‐frame smoke test: ERROR ->", e)

features = {
    "mid_price": {"method": "mid_price", "args": []},
    "spread": {"method": "bid_ask_spread", "args": []},
}

# Example: first write
sim.print_features_to_csv(
    filename="AAPL_features",          # you choose the file name ('.csv' auto-added if missing)
    start_time=starttime - 2_000_000,              # use your existing bounds
    end_time=endtime - 2_000_000,
    interval=500000,                   # adjust step to your data's time units
    features=features,
    batch_date=file_start_date,        # user-provided date (e.g., "2019-01-02")
    symbol=ticker,                     # user-provided ticker (e.g., "AAPL")
    directory=".",                     # current dir; change if you want
)

