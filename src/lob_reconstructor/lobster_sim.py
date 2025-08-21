import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from scipy.stats import zscore
from typing import Literal

from .orderbook import Orderbook
from .orders import Order
from .utils import format_timestamp
from dash import Dash, dcc, html, Input, Output, State, callback_context
from plotly.subplots import make_subplots


class LobsterSim:
    """
    LOBSTER simulation and visualization interface.

    Provides functionality for replaying limit order book events,
    computing order flow imbalance (OFI), and generating
    visualizations using Plotly and Dash.

    Parameters
    ----------
    orderbook : Orderbook
        Orderbook object to operate on. See :class:`Orderbook`
        in `orderbook.py` for full definition.
    msg_book_file_path : str
        LOBSTER message.csv file path.
    lob_book_file_path : str, default=None
        LOBSTER orderbook.csv file path.
        Not necessary for end user (just use default val), used solely in debugging/testing
        to ensure matching between reconstructed and expected.

    Attributes
    ----------
    orderbook : Orderbook
        Orderbook object to operate on. See :class:`Orderbook`
        in `orderbook.py` for full definition.
    dataM : pd.DataFrame
        Contains message data pulled from LOBSTER message.csv with columns:

        - `Time`: float
        - `Type`: Literal['submit', 'cancel', 'delete', 'vis_exec', 'hid_exec', 'cross', 'halt']
        - `OrderID`: int
        - `Size`: int
        - `Price`: int
        - `Direction`: Literal['bid', 'ask']
    """
    def __init__(self, orderbook: Orderbook, msg_book_file_path: str, lob_book_file_path: str = None):
        self.orderbook = orderbook
        self._last_idx = 0
        columns = ["Time", "Type", "OrderID", "Size", "Price", "Direction"]

        dtype_map = {
            "Time": float,
            "Type": "Int64",
            "OrderID": "Int64",
            "Size": "Int64",
            "Price": "Int64",
            "Direction": "Int64"
        }

        dataM = pd.read_csv(
            msg_book_file_path,
            header=None,
            names=columns,
            usecols=range(len(columns)),  # drop any extra columns in the file
            dtype=dtype_map,
            na_values=["", "NA"],         # treat blanks as NaN
            low_memory=False
        )

        # dataM = dataM[~dataM['Type'].isin([6, 7])] #Remove halts and auction trades
        event_map = {
            1: 'submit',
            2: 'cancel',
            3: 'delete',
            4: 'vis_exec',
            5: 'hid_exec',
            6: 'cross',
            7: 'halt'
        }

        dataM['Type'] = dataM['Type'].map(event_map)
        dataM['Direction'] = dataM['Direction'].map({-1: 'ask', 1: 'bid'})

        self.dataM = dataM

        if lob_book_file_path is None:
            self._dataL = None
        else:
            sample = pd.read_csv(lob_book_file_path, nrows=1, header=None)
            num_cols = sample.shape[1]

            if num_cols % 4 != 0:
                raise ValueError("Orderbook file column count is not a multiple of 4.")

            num_levels = num_cols // 4

            col_names = []
            for i in range(1, num_levels + 1):
                col_names.extend([
                    f"AskPrice{i}", f"AskSize{i}",
                    f"BidPrice{i}", f"BidSize{i}"
                ])

            dataL = pd.read_csv(
                lob_book_file_path,
                header=None,
                names=col_names,
                dtype=int,
                low_memory=False
            )
            self._dataL = dataL

    def simulate_until(self, time: float) -> None:
        """
        Resets orderbook state.
        Reconstructs orderbook state from beginning of message file until specified timestamp.

        Parameters
        ----------
        time : float
            Time in seconds after midnight to simulate until
        """
        self._last_idx = 0
        self.orderbook.clear_orderbook()
        for row in self.dataM.itertuples(index=False):
            if row.Time > time:
                break
            curr_order = Order(row.Time, row.Type, row.OrderID, row.Size, row.Price, row.Direction)
            self.orderbook.process_order(curr_order)
            self._last_idx += 1

    def simulate_from_current_until(self, time: float) -> None:
        """
        Continue reconstructing the order book from the current simulation state
        up to a specified timestamp.
        Does NOT reset orderbook state, unlike simulate_until.

        Parameters
        ----------
        time : float
            Time in seconds after midnight to simulate until.

        Raises
        ------
        ValueError
            If `time` is earlier than the current order book timestamp.
        """
        if time < self.orderbook.curr_book_timestamp:
            raise ValueError("time parameter must be greater than current book timestamp")
        for row in self.dataM.iloc[self._last_idx:].itertuples(index=False):
            if row.Time > time:
                break
            curr_order = Order(row.Time, row.Type, row.OrderID, row.Size, row.Price, row.Direction)
            self.orderbook.process_order(curr_order)
            self._last_idx += 1

    def display_L3_snapshots(self, start_time: float, end_time: float, interval: float) -> None:
        """
        Display multiple L3 order book snapshots as subplots over a specified time range.
        Simulates the order book from `start_time` to `end_time` and generates a Plotly
        figure with subplots showing the L3 state at each interval.
        Each subplot title includes the current time, midprice, and bid-ask spread.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation and plotting.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation and plotting.
        interval : float
            Time interval (in seconds) between consecutive snapshots.
        """
        self.simulate_until(start_time)
        curr_time = start_time + interval
        traces_tuples = []
        subplot_titles = []
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            subplot_titles.append(f"Time: {format_timestamp(curr_time)}<br>"
                                  f"Mid Price: {self.orderbook.midprice*self.orderbook.price_scaling:.2f}<br>"
                                  f"Spread: {self.orderbook.bid_ask_spread()*self.orderbook.price_scaling:.2f}")
            traces_tuples.append(self.orderbook._get_L3_plot_traces())
            curr_time += interval

        cols = 3
        rows = (len(traces_tuples) + cols-1)//cols
        fig = make_subplots(rows=rows, cols=cols, subplot_titles=subplot_titles)

        for i, traces in enumerate(traces_tuples):
            for trace in traces:
                fig.add_trace(trace, row=i // cols + 1, col=i % cols + 1)

        fig.update_traces(width=self.orderbook.tick_size)
        fig.update_layout(
            height=300 * len(traces_tuples),
            title_text=f"{self.orderbook.ticker} L3 Snapshots",
            showlegend=False
        )
        fig.show()

    def display_L2_snapshots(self, start_time: float, end_time: float, interval: float) -> None:
        """
        Display multiple L2 order book snapshots as subplots over a specified time range.
        Simulates the order book from `start_time` to `end_time` and generates a Plotly
        figure with subplots showing the L3 state at each interval.
        Each subplot title includes the current time, midprice, and bid-ask spread.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation and plotting.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation and plotting.
        interval : float
            Time interval (in seconds) between consecutive snapshots.
        """
        self.simulate_until(start_time)
        curr_time = start_time + interval
        traces_tuples = []
        subplot_titles = []
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            subplot_titles.append(
                f"Time: {format_timestamp(curr_time)}<br>"
                f"Mid Price: {self.orderbook.midprice * self.orderbook.price_scaling:.2f}<br>"
                f"Spread: {self.orderbook.bid_ask_spread() * self.orderbook.price_scaling:.2f}"
            )
            traces_tuples.append(self.orderbook._get_L2_plot_traces())
            curr_time += interval

        cols = 3
        rows = (len(traces_tuples) + cols - 1) // cols
        fig = make_subplots(rows=rows, cols=cols, subplot_titles=subplot_titles)

        for i, traces in enumerate(traces_tuples):
            for trace in traces:
                fig.add_trace(trace, row=i // cols + 1, col=i % cols + 1)

        fig.update_traces(width=self.orderbook.tick_size)
        fig.update_layout(
            height=300 * len(traces_tuples),
            title_text=f"{self.orderbook.ticker} L2 Snapshots",
            showlegend=False
        )
        fig.show()

    def sim_size_OFI(self, start_time: float, end_time: float) -> int:
        """
        Simulate the order book between two timestamps and compute the cumulative
        size-based Order Flow Imbalance (OFI).

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.

        Returns
        -------
        int
            The cumulative size-based OFI computed over the simulation interval.

        Notes
        -----
        The method resets the cumulative OFI at the start of the simulation, then
        processes all messages between `start_time` and `end_time`.
        """
        self.simulate_until(start_time)
        self.orderbook.reset_cum_OFI()
        self.simulate_from_current_until(end_time)
        return self.orderbook.calc_size_OFI()

    def sim_count_OFI(self, start_time: float, end_time: float) -> int:
        """
        Simulate the order book between two timestamps and compute the cumulative
        count-based Order Flow Imbalance (OFI).

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.

        Returns
        -------
        int
            The cumulative count-based OFI computed over the simulation interval.

        Notes
        -----
        The method resets the cumulative OFI at the start of the simulation, then
        processes all messages between `start_time` and `end_time`.
        """
        self.simulate_until(start_time)
        self.orderbook.reset_cum_OFI()
        self.simulate_from_current_until(end_time)
        return self.orderbook.calc_count_OFI()

    def create_animated_L3_app(self, start_time: float, end_time: float, interval: float) -> Dash:
        """
        Create an interactive Dash application showing an animated L3 order book.

        The application displays horizontal bar charts of order sizes at each price
        level, updating over time to animate the evolution of the L3 book. Users
        can play/pause the animation or manually slide through frames.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        interval : float
            Time interval (in seconds) between consecutive frames.

        Returns
        -------
        dash.Dash
            A Dash application instance that can be run or embedded in a web server.

        Notes
        -----
        - The method simulates the order book over the specified interval and stores
          snapshots in memory.
        - Each frame shows a horizontal bar chart of L3 order sizes by price and direction.
        - Users can interact via a play/pause button and a slider for manual navigation.
        """
        frames = []
        timestamps = []
        self.simulate_until(start_time)
        curr_time = start_time
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            df = self.orderbook.convert_orderbook_to_L3_dataframe()
            df.price = df.price * self.orderbook.price_scaling
            frames.append(df)
            timestamps.append(curr_time)
            curr_time += interval

        all_prices = pd.concat([df["price"] for df in frames])
        price_min = all_prices.min()
        price_max = all_prices.max()

        app = Dash(__name__)

        app.layout = html.Div([
            dcc.Graph(id='l3-graph'),
            html.Div([
                html.Button("⏯ Play/Pause", id="play-pause", n_clicks=0)
            ], style={'marginTop': '10px'}),

            dcc.Slider(
                id='frame-slider',
                min=0,
                max=len(frames) - 1,
                step=1,
                value=0,
                marks={i: str(i) for i in range(0, len(frames), max(1, len(frames) // 10))},
                tooltip={"placement": "bottom", "always_visible": True}
            ),
            dcc.Interval(id='interval', interval=2000, n_intervals=0),
            dcc.Store(id='paused', data=False),
        ])
        @app.callback(
            Output('l3-graph', 'figure'),
            Output('frame-slider', 'value'),
            Output('paused', 'data'),
            Input('interval', 'n_intervals'),
            Input('frame-slider', 'value'),
            Input('play-pause', 'n_clicks'),
            State('paused', 'data')
        )
        def update_l3_graph(n_intervals, slider_value, play_clicks, paused):
            ctx = callback_context
            triggered = ctx.triggered[0]['prop_id'].split('.')[0]

            if triggered == "play-pause":
                paused = not paused

            if triggered == "interval" and not paused:
                frame = (slider_value + 1) % len(frames)
            elif triggered == "frame-slider":
                frame = slider_value
            else:
                frame = slider_value

            df = frames[frame]
            timestamp = timestamps[frame]
            fig = px.bar(
                df,
                orientation='h',
                x="size",
                y="price",
                color="direction",
                title=f"{self.orderbook.ticker}<br><sup>{format_timestamp(timestamp)}",
                color_discrete_sequence=["green", "red"]
            )
            fig.update_layout(
                xaxis=dict(range=[0, 2000], autorange=False),
                yaxis=dict(range=[price_min, price_max], autorange=False),
                uirevision="static",
                height=600
            )
            return fig, frame, paused

        return app

    def plot_price_levels_heatmap(self, start_time: float, end_time: float, interval: float, show_midprice:bool=True) -> None:
        """
        Creates a heatmap graph of order book price levels over time.

        The heatmap visualizes the depth of the order book at different price levels
        over a specified time range. The x-axis represents time, the y-axis represents
        price, and the color intensity at each point indicates the total size (volume)
        of orders at that price level at that specific time.

        Parameters
        ----------
        start_time : float
            The timestamp in seconds after midnight to begin the simulation and plotting.
        end_time : float
            The timestamp in seconds after midnight to end the simulation and plotting.
        interval : float
            The time interval in seconds between each data point (snapshot) on the heatmap.
        show_midprice : bool, optional
            If True, a white line representing the mid-price of the order book is overlaid
            on the heatmap. Defaults to True.

        Notes
        -----
        - The `self.simulate_until()` and `self.simulate_from_current_until()` methods
          are used to advance the simulation and collect order book snapshots.
        - The price values are scaled by `self.orderbook.price_scaling` for accurate
          visualization.
        - This function uses the `plotly.graph_objects` library to generate an interactive
          heatmap.
        """
        l2_snapshots = []
        timestamps = []
        midprices = []
        curr_time = start_time
        self.simulate_until(curr_time)
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            df = self.orderbook.convert_orderbook_to_L2_dataframe()
            df.price = df.price * self.orderbook.price_scaling
            l2_snapshots.append(df)
            timestamps.append(format_timestamp(curr_time))
            midprices.append(self.orderbook.mid_price() * self.orderbook.price_scaling)
            curr_time += interval

        all_prices = sorted(set().union(*(df['price'] for df in l2_snapshots)))
        price_to_idx = {price: i for i, price in enumerate(all_prices)}

        heatmap = np.zeros((len(all_prices), len(l2_snapshots)))

        for t, snapshot in enumerate(l2_snapshots):
            for _, row in snapshot.iterrows():
                price = row['price']
                size = row['size']
                i = price_to_idx[price]
                heatmap[i, t] = size

        fig = go.Figure()

        fig.add_trace(go.Heatmap(
            z=heatmap,
            x=timestamps,
            y=all_prices,
            colorscale='Turbo',
            colorbar=dict(title='Size'),
            zsmooth='best'
        ))

        if show_midprice:
            fig.add_trace(go.Scatter(
                x=timestamps,
                y=midprices,
                mode='lines',
                line=dict(color='white', width=2),
                name='Midprice',
            ))

        fig.update_layout(
            title=f'{self.orderbook.ticker} Orderbook Price Level Heatmap',
            template='plotly_dark',
            xaxis_title='Time',
            yaxis_title='Price',
            height=800,
            width=1400,
            margin=dict(l=40, r=40, t=40, b=40)
        )

        fig.show()

    def size_OFI_graph(self, start_time: float, end_time: float, frame_interval: float, reset_ofi_interval: float =np.inf) -> None:
        """
        Plots a time series graph of the cumulative Size Order Flow Imbalance (OFI).

        This function simulates the order book over a specified time range, calculating
        the cumulative Size OFI at regular intervals and plotting the results. The Size
        OFI measures the imbalance between the total size of buy and sell orders.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        frame_interval : float
            Time interval (in seconds) between each point plotted on the graph.
        reset_ofi_interval : float, optional
            The time interval (in seconds) at which the cumulative OFI value is reset to zero.
            Defaults to `np.inf`, meaning the OFI is never reset within the plotting range.
        """
        timestamps = []
        ofi_values = []
        self.simulate_until(start_time)
        self.orderbook.reset_cum_OFI()
        curr_time = start_time
        reset_time = 0
        while curr_time <= end_time:
            if reset_time >= reset_ofi_interval:
                self.orderbook.reset_cum_OFI()
                reset_time = 0
            self.simulate_from_current_until(curr_time)
            ofi_values.append(self.orderbook.calc_size_OFI())
            timestamps.append(format_timestamp(curr_time))
            curr_time += frame_interval
            reset_time += frame_interval

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=timestamps,
            y=ofi_values,
            mode='lines+markers',
            line=dict(color='cyan'),
            name='Size OFI'
        ))

        fig.update_layout(
            title=f"{self.orderbook.ticker} OFI Time Series",
            xaxis_title='Time',
            yaxis_title='Order Flow Imbalance',
            template='plotly_dark',
            height=500,
            width=1200,
            margin=dict(l=40, r=40, t=40, b=40)
        )
        fig.show()


    def count_OFI_graph(self, start_time: float, end_time: float, frame_interval: float, reset_ofi_interval: float=np.inf) -> None:
        """
        Plots a time series graph of the cumulative Count Order Flow Imbalance (OFI).

        This function simulates the order book over a specified time range, calculating
        the cumulative Count OFI at regular intervals and plotting the results. The Count
        OFI measures the imbalance between the number of buy and sell orders.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        frame_interval : float
            Time interval (in seconds) between each point plotted on the graph.
        reset_ofi_interval : float, optional
            The time interval (in seconds) at which the cumulative OFI value is reset to zero.
            Defaults to `np.inf`, meaning the OFI is never reset within the plotting range.
        """
        timestamps = []
        ofi_values = []
        self.simulate_until(start_time)
        self.orderbook.reset_cum_OFI()
        curr_time = start_time
        reset_time = 0
        while curr_time <= end_time:
            if reset_time >= reset_ofi_interval:
                self.orderbook.reset_cum_OFI()
                reset_time = 0
            self.simulate_from_current_until(curr_time)
            ofi_values.append(self.orderbook.calc_count_OFI())
            timestamps.append(format_timestamp(curr_time))
            curr_time += frame_interval
            reset_time += frame_interval

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=timestamps,
            y=ofi_values,
            mode='lines+markers',
            line=dict(color='cyan'),
            name='Size OFI'
        ))

        fig.update_layout(
            title=f"{self.orderbook.ticker} OFI Time Series",
            xaxis_title='Time',
            yaxis_title='Order Flow Imbalance',
            template='plotly_dark',
            height=500,
            width=1200,
            margin=dict(l=40, r=40, t=40, b=40)
        )
        fig.show()

    def midprice_graph(self, start_time: float, end_time: float, interval: float) -> None:
        """
        Plots a time series graph of the mid-price of the order book.

        This function simulates the order book over a specified time range, capturing
        the mid-price at regular intervals and plotting the results as a line graph.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        interval : float
            Time interval (in seconds) between each data point plotted on the graph.
        """
        timestamps = []
        midprices = []
        curr_time = start_time
        self.simulate_until(curr_time)
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            timestamps.append(format_timestamp(curr_time))
            midprices.append(self.orderbook.mid_price() * self.orderbook.price_scaling)
            curr_time += interval
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=midprices,
            mode='lines',
            line=dict(color='white', width=2),
            name='Midprice',
        ))

        fig.update_layout(
            title=f"{self.orderbook.ticker} Mid Price",
            xaxis_title='Time',
            yaxis_title='Price',
            template='plotly_dark',
            height=500,
            width=1200,
            margin=dict(l=40, r=40, t=40, b=40)
        )
        fig.show()

    def depth_percentile_graph(self, start_time: float, end_time: float, interval: float) -> None:
        """
        Creates a heatmap graph of order book depth in basis points (BPS) from the mid-price.

        The heatmap visualizes the depth of the order book relative to the mid-price
        over time. The x-axis is time, the y-axis is the price level in BPS from the
        mid-price, and the color intensity at each point represents the size (volume)
        at that price level. A white horizontal line at 0 BPS indicates the mid-price.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        interval : float
            Time interval (in seconds) between each data point (snapshot) on the heatmap.
        """
        timestamps = []
        l2_snapshots = []
        midprices = []
        curr_time = start_time
        self.simulate_until(curr_time)
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            df = self.orderbook.convert_orderbook_to_L2_dataframe()
            df.price = df.price.astype(float) * self.orderbook.price_scaling
            l2_snapshots.append(df)
            timestamps.append(format_timestamp(curr_time))
            midprices.append(self.orderbook.mid_price() * self.orderbook.price_scaling)
            curr_time += interval

        all_bps = set()
        for i, df in enumerate(l2_snapshots):
            bps_values = ((df["price"] - midprices[i]) / midprices[i] * 10000)
            bps_values = bps_values.round().astype(int)
            all_bps.update(bps_values.tolist())
        all_bps = sorted(all_bps)
        bps_abs_max = abs(max(all_bps[0], -all_bps[-1]))

        bps_to_idx = {bps: i for i, bps in enumerate(all_bps)}
        heatmap = np.zeros((len(all_bps), len(l2_snapshots)))

        for t, snapshot in enumerate(l2_snapshots):
            for _, row in snapshot.iterrows():
                bps = int(round(((row['price'] - midprices[t]) / midprices[t] * 10000)))
                size = row['size']
                i = bps_to_idx[bps]
                heatmap[i, t] = size
        fig = go.Figure(data=go.Heatmap(
            z=heatmap,
            x=timestamps,
            y=all_bps,
            colorscale='Turbo',
            colorbar=dict(title='Volume'),
        ))

        fig.update_layout(
            title="Depth Percentile Graph",
            xaxis_title="Time",
            yaxis=dict(title="BPS from Midprice", range=[-bps_abs_max, bps_abs_max], autorange=False),
            template="plotly_dark",
            height=600,
        )
        fig.add_shape(
            type="line",
            x0=0,
            x1=1,
            y0=0,
            y1=0,
            xref="paper",
            yref="y",
            line=dict(
                color="white",
                width=2
            )
        )
        fig.show()

    def graph_trade_arrival_time(self, start_time: float, end_time: float, bin_size: float =None, filter_trade_type: Literal["aggro_lim", "vis_exec", "hid_exec"] = None) -> None:
        """
        Graphs the arrival count of bid and ask trades over time.

        The function simulates trades within a specified time range, aggregates them
        into time bins, and plots a bar chart showing the number of buy (bid) and
        sell (ask) trades in each bin.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        bin_size : float, optional
            The size of each time bin in seconds. If None, the bin size is set to
            1/100th of the total time range.
        filter_trade_type : Literal["aggro_lim", "vis_exec", "hid_exec"], optional
            A filter to display only a specific type of trade. Defaults to None,
            meaning all trade types are included.
        """
        if bin_size is None:
            bin_size = (end_time - start_time) / 100
        self.simulate_until(start_time)
        self.orderbook.clear_trade_log()
        self.simulate_from_current_until(end_time)

        df = pd.DataFrame(self.orderbook.trade_log)

        df["time_bin"] = (df["timestamp"] // bin_size) * bin_size
        if filter_trade_type is not None:
            df = df[df["trade_type"] == filter_trade_type]

        grouped = df.groupby(["time_bin", "direction"]).size().unstack(fill_value=0)

        if "bid" not in grouped.columns:
            grouped["bid"] = 0
        if "ask" not in grouped.columns:
            grouped["ask"] = 0

        grouped = grouped.sort_index()
        max_count = max(grouped["bid"].max(), grouped["ask"].max())

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=[format_timestamp(ts) for ts in grouped.index],
            y=grouped["bid"],
            name="Bids",
            marker_color="blue"
        ))

        fig.add_trace(go.Bar(
            x=[format_timestamp(ts) for ts in grouped.index],
            y=-grouped["ask"],
            name="Asks",
            marker_color="red"
        ))

        fig.update_layout(
            title="Bid vs Ask Trade Counts Over Time",
            xaxis_title="Time",
            yaxis_title="# of Trades",
            barmode='relative',
            bargap=0.1,
            legend=dict(x=1, y=1),
            yaxis=dict(
                range=[-max_count, max_count],
                zeroline=True,
                zerolinewidth=2,
                zerolinecolor='black',
                tickmode='linear',
                tick0=0,
                dtick=100
            )
        )

        fig.show()

    def graph_trade_size_distribution(self, start_time: float, end_time: float, bin_size:int=20, filter_trade_type: Literal["aggro_lim", "vis_exec", "hid_exec"] = None) -> None:
        """
        Graphs the size distribution of bid and ask trades.

        This function simulates trades within a specified time range, filters out outliers
        using Z-score, and then creates a bar chart showing the distribution of trade
        sizes for both bids and asks.

        Parameters
        ----------
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        bin_size : int, optional
            The size of each trade size bin. Defaults to 20.
        filter_trade_type : Literal["aggro_lim", "vis_exec", "hid_exec"], optional
            A filter to display only a specific type of trade. Defaults to None,
            meaning all trade types are included.
        """
        self.simulate_until(start_time)
        self.orderbook.clear_trade_log()
        self.simulate_from_current_until(end_time)

        df = pd.DataFrame(self.orderbook.trade_log)

        if df.empty:
            print("No trades in the given time range.")
            return

        df["z_score"] = zscore(df["size"])
        df = df[df["z_score"].abs() <= 3]
        df.drop(columns="z_score", inplace=True)

        if df.empty:
            print("All trades were filtered out as outliers.")
            return

        if filter_trade_type is not None:
            df = df[df["trade_type"] == filter_trade_type]

        df["size_bin"] = (df["size"] // bin_size) * bin_size

        grouped = df.groupby(["size_bin", "direction"]).size().unstack(fill_value=0)

        if "bid" not in grouped.columns:
            grouped["bid"] = 0
        if "ask" not in grouped.columns:
            grouped["ask"] = 0

        grouped = grouped.sort_index()
        max_count = max(grouped["bid"].max(), grouped["ask"].max())

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=grouped.index,
            y=grouped["bid"],
            name="Bids",
            marker_color="blue"
        ))

        fig.add_trace(go.Bar(
            x=grouped.index,
            y=-grouped["ask"],
            name="Asks",
            marker_color="red"
        ))

        fig.update_layout(
            title="Bid & Ask Trade Sizes Distribution (Outliers Removed)",
            xaxis_title="Size of Trade",
            yaxis_title="# of Trades",
            barmode='relative',
            bargap=0.1,
            legend=dict(x=1, y=1),
            yaxis=dict(
                range=[-max_count, max_count],
                zeroline=True,
                zerolinewidth=2,
                zerolinecolor='black',
                tickmode='linear',
                tick0=0,
                dtick=max(1, int(max_count / 5))
            )
        )

        fig.show()

    def print_features_to_csv(self, path: str, start_time: float, end_time: float, interval: float, features: dict) -> None:
        """
        Exports a time series of specified order book features to a CSV file.

        The function simulates the order book from `start_time` to `end_time` at a given
        `interval`. At each interval, it calculates a set of features defined by the
        `features` dictionary and saves the results to a CSV file.

        Parameters
        ----------
        path : str
            The file path to save the output CSV.
        start_time : float
            Timestamp (seconds after midnight) to start the simulation.
        end_time : float
            Timestamp (seconds after midnight) to end the simulation.
        interval : float
            Time interval (in seconds) between each data point.
        features : dict
            A dictionary where keys are feature names and values are dictionaries
            specifying the order book method to call and its arguments.
            Example: `{"mid_price": {"method": "mid_price", "args": []},"spread": {"method": "bid_ask_spread", "args": []},"vol_at_105": {"method": "available_vol_at_price", "args": [105000]}}`
        """
        self.simulate_until(start_time)
        results = []
        timestamps = []

        curr_time = start_time
        while curr_time <= end_time:
            self.simulate_from_current_until(curr_time)
            row = {}

            for feature_name, spec in features.items():
                method_name = spec.get("method")
                args = spec.get("args", [])

                if not hasattr(self.orderbook, method_name):
                    raise AttributeError(f"Orderbook has no method '{method_name}'")

                method = getattr(self.orderbook, method_name)
                try:
                    value = method(*args)
                except Exception as e:
                    value = None
                    print(f"Error computing {feature_name} at {curr_time}: {e}")

                row[feature_name] = value

            results.append(row)
            timestamps.append(curr_time)

            curr_time += interval

        # Convert to DataFrame
        df = pd.DataFrame(results)
        df.insert(0, "timestamp", timestamps)

        # Save to CSV
        df.to_csv(path, index=False)
        print(f"Features saved to {path}")



    # --------------------------
    # DEBUGGING
    # --------------------------
    def _check_books_match(self, num_levels_to_check):
        snapshot = self._dataL.iloc[self._last_idx]
        reconstructed = self.orderbook.convert_orderbook_to_L2_dataframe()
        for side in ["ask", "bid"]:
            for level in range(num_levels_to_check):
                csv_price = snapshot[f"{side.capitalize()}Price{level + 1}"]
                csv_size = snapshot[f"{side.capitalize()}Size{level + 1}"]
                recon_key = f"{side}_{level}"
                if recon_key in reconstructed.index:
                    recon_row = reconstructed.loc[recon_key]
                    recon_price = recon_row["price"]
                    recon_size = recon_row["size"]

                    # If CSV has dummy value but reconstructed has a real value, fail
                    if (side == "ask" and csv_price == 9999999999) or (side == "bid" and csv_price == -9999999999):
                        raise AssertionError(
                            f"{side.upper()} level {level} unexpectedly present in reconstruction: CSV has dummy value but reconstruction has price {recon_price}"
                        )
                    if recon_price != csv_price:
                        raise AssertionError(
                            f"{side.upper()} level {level} price mismatch: CSV={csv_price}, Reconstructed={recon_price}"
                        )
                    if recon_size != csv_size:
                        raise AssertionError(
                            f"{side.upper()} level {level} size mismatch: CSV={csv_size}, Reconstructed={recon_size}"
                        )
                else:
                    if not ((side == "ask" and csv_price == 9999999999) or (side == "bid" and csv_price == -9999999999)):
                        raise AssertionError(
                            f"{side.upper()} level {level} missing in reconstruction: expected CSV price {csv_price}"
                        )
        print("MashAllah")

    def _debug_sim(self, number_of_rows_to_sim):
        self.orderbook.clear_orderbook()
        for i, row in enumerate(self.dataM.itertuples(index=False)):
            if i > number_of_rows_to_sim:
                break
            curr_order = Order(row.Time, row.Type, row.OrderID, row.Size, row.Price, row.Direction)
            self.orderbook.process_order(curr_order)
        self._last_idx = number_of_rows_to_sim

    def _debug_sim_next(self):
        row = self.dataM.iloc[self._last_idx+1]
        curr_order = Order(row.Time, row.Type, row.OrderID, row.Size, row.Price, row.Direction)
        self.orderbook.process_order(curr_order)
        self._last_idx += 1

    def _check_full_book(self, num_levels_to_check):
        for row in self.dataM.itertuples(index=False):
            curr_order = Order(row.Time, row.Type, row.OrderID, row.Size, row.Price, row.Direction)
            self.orderbook.process_order(curr_order)
            try:
                self._check_books_match(num_levels_to_check)
            except AssertionError as e:
                print(f"You fucked up at index: {self._last_idx}")
                print(e)
                return

            self._last_idx += 1
        print("Full book is good")


    def _validate_lobster_data(self):
        if not hasattr(self, "dataM") or not hasattr(self, "dataL"):
            raise AttributeError("Both self.dataM and self.dataL must be initialized before validation.")

        msg_len = len(self.dataM)
        lob_len = len(self._dataL)

        if msg_len != lob_len:
            print(f"Row count mismatch: messages = {msg_len}, orderbook = {lob_len}")
            min_len = min(msg_len, lob_len)
            print(f"Extra rows in message file starting from index {min_len}")
            return

        print(f"Data aligned correctly, {msg_len} lines in file")










