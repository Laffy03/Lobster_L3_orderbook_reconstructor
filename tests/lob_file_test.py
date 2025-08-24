import os
import pandas as pd

from src.lobster_reconstructor.orderbook import Orderbook
from src.lobster_reconstructor.lobster_sim import LobsterSim

ticker = "AAPL"
file_start_date = "2019-01-02"

print(file_start_date)

# <<< Use seconds >>>
starttime = 14400.0       # 04:00:00
endtime   = 14410.0       # 04:10:00
INTERVAL  = 0.5           # sample every 0.5s (try 0.1 if you want denser sampling)
ROUND     = 6             # overlap rounding for floats
nlevels   = 10

MSGBOOK = r"C:\Users\komal\Downloads\AAPL_Jan2019\AAPL_2019-01-02_24900000_57900000_message_10.csv"
OUT_NAME = "AAPL_features"
OUT_DIR  = "."

csv_path = os.path.join(OUT_DIR, OUT_NAME + ("" if OUT_NAME.lower().endswith(".csv") else ".csv"))

lob = Orderbook(nlevels, ticker, 0.01)
sim = LobsterSim(lob, MSGBOOK)

print("\n=== simulate_until / simulate_from_current_until ===")
sim.simulate_until(starttime)

def have_quotes():
    bid = lob.highest_bid_price()
    ask = lob.lowest_ask_price()
    return (bid > 0) and (ask < float("inf"))

t = starttime
# step up to 5 seconds forward in 0.1s increments until both sides exist
while not have_quotes() and t <= starttime + 5.0:
    t += 0.1
    sim.simulate_from_current_until(t)

bid = lob.highest_bid_price()
ask = lob.lowest_ask_price()
if (bid > 0) and (ask < float("inf")):
    mid = (bid + ask) / 2
    spr = ask - bid
    print(f"quotes ready at t={t:.3f}: bid={bid * lob.price_scaling:.4f}, "
          f"ask={ask * lob.price_scaling:.4f}, mid={mid * lob.price_scaling:.4f}, "
          f"spread={spr * lob.price_scaling:.4f}")
else:
    print(f"book not ready by t={t:.3f}; no valid best bid/ask yet")

# advance a bit more and report again
sim.simulate_from_current_until(t + 2.0)
bid2 = lob.highest_bid_price()
ask2 = lob.lowest_ask_price()
if (bid2 > 0) and (ask2 < float("inf")):
    mid2 = (bid2 + ask2) / 2
    spr2 = ask2 - bid2
    print(f"after +2s: bid={bid2 * lob.price_scaling:.4f}, "
          f"ask={ask2 * lob.price_scaling:.4f}, mid={mid2 * lob.price_scaling:.4f}, "
          f"spread={spr2 * lob.price_scaling:.4f}")
else:
    print("after +2s: still no valid best bid/ask")

# (Optional) quick visuals on tiny windows
# sim.midprice_graph(starttime, starttime + 2.0, interval=INTERVAL)
# sim.size_OFI_graph(starttime, starttime + 2.0, frame_interval=INTERVAL)
# sim.count_OFI_graph(starttime, starttime + 2.0, frame_interval=INTERVAL)
# sim.display_L2_snapshots(starttime, starttime + 1.0, interval=0.5)
# sim.display_L3_snapshots(starttime, starttime + 1.0, interval=0.5)
# sim.plot_price_levels_heatmap(starttime, starttime + 2.0, interval=0.5)
# sim.depth_percentile_graph(starttime, starttime + 2.0, interval=0.5)
# app = sim.create_animated_L3_app(starttime, starttime + 1.0, interval=0.5)

print("\n=== print_features_to_csv (fresh) ===")
features_A = {
    "mid_price": {"method": "mid_price", "args": []},
    "spread":    {"method": "bid_ask_spread", "args": []},
}
sim.print_features_to_csv(
    filename=OUT_NAME,
    start_time=starttime,
    end_time=endtime,
    interval=INTERVAL,
    features=features_A,
    batch_date=file_start_date,
    symbol=ticker,
    directory=OUT_DIR,
    timestamp_round=ROUND,
)

df = pd.read_csv(csv_path)
cur = df[(df["date"] == file_start_date) & (df["ticker"] == ticker)]
print(f"[Run 1] rows={len(cur)}  min_ts={cur['timestamp'].min()}  max_ts={cur['timestamp'].max()}")