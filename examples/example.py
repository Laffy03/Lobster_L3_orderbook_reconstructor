import logging

from lobster_reconstructor import LobsterSim, Orderbook
import threading


# Set logging messages to desired level
logging.basicConfig(
    level=logging.ERROR,
    format="%(levelname)s:%(name)s:%(message)s"
)

# File path for LOBSTER message file
MSGBOOK = "message.csv"

# Configuration Parameters
ticker = "AAA"
nlevels = 10
tick_size = 0.01


example_lob = Orderbook(nlevels, ticker, tick_size)
example = LobsterSim(example_lob, MSGBOOK)



app = example.create_animated_L3_app(34200, 34500, 1) # Create animation from 9:30 (34200 seconds after midnight) until 9:35 (34500 seconds after midnight) with 1 second between each frame
def run_dash():
    app.run(debug=False, use_reloader=False, host='127.0.0.1', port=8050)  # Run locally

threading.Thread(target=run_dash, daemon=True).start() # This allows us to load the rest of the graphs/examples while Dash app is running



example.size_OFI_graph(35000, 36800, 5)
example.count_OFI_graph(35000, 36800, 5)

example.plot_price_levels_heatmap(35000, 36800, 5)
example.midprice_graph(35000, 36800, 5)
example.depth_percentile_graph(35000, 36800, 5)
example.display_L3_snapshots(32400, 57600, 3600)
example.graph_trade_arrival_time(34200, 57600)
example.graph_trade_arrival_time(34200,57600, filter_trade_type="hid_exec") #Only show hidden execution arrival times
example.graph_trade_size_distribution(34200, 57600)

features = {
    "mid_price": {"method": "mid_price", "args": []},
    "spread": {"method": "bid_ask_spread", "args": []},
}

# Example: first write
example.print_features_to_csv(
    filename="AAPL_features",       # '.csv' auto-added if missing
    start_time=34200,
    end_time=36000,
    interval=300,                   # adjust step to your data's time units
    features=features,
    batch_date="2019-01-02",        # user-provided date (e.g., "2019-01-02")
    symbol=ticker,                  # user-provided ticker (e.g., "AAPL")
    directory=".",                  # current dir; change if you want
)

input("Press Enter to exit the Dash app...\n")
