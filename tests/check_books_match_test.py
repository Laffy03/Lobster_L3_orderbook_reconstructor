from lobster_reconstructor.orderbook import Orderbook
from lobster_reconstructor.lobster_sim import LobsterSim
import pandas as pd
import logging

logger = logging.getLogger("orderbook")
logger.setLevel(logging.DEBUG)

logging.basicConfig(level=logging.NOTSET)

complete_message_file = r"message_0.csv"
n_level_message_file = r"message_10.csv"
orderbook_file = r"orderbook_10.csv"


columns = ["Time", "Type", "OrderID", "Size", "Price", "Direction"]
dtype_map = {
    "Time": float,
    "Type": "Int64",
    "OrderID": "Int64",
    "Size": "Int64",
    "Price": "Int64",
    "Direction": "Int64"
}
n_level_message_df = pd.read_csv(
    n_level_message_file,
    header=None,
    names=columns,
    usecols=range(len(columns)),  # drop any extra columns in the file
    dtype=dtype_map,
    na_values=["", "NA"],  # treat blanks as NaN
    low_memory=False
)
event_map = {
    1: 'submit',
    2: 'cancel',
    3: 'delete',
    4: 'vis_exec',
    5: 'hid_exec',
    6: 'cross',
    7: 'halt'
}
n_level_message_df['Type'] = n_level_message_df['Type'].map(event_map)
n_level_message_df['Direction'] = n_level_message_df['Direction'].map({-1: 'ask', 1: 'bid'})


test_lob = Orderbook(10, "ticker", 0.01)
test_sim = LobsterSim(test_lob, complete_message_file, orderbook_file)

test_sim._check_full_book(10, n_level_message_df)

