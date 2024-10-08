import pandas as pd
import numpy as np
from hdf5reader import HDF5Reader
from datetime import datetime, timedelta
from scipy import signal

class EventAnalyser():
    def __init__(self, order_book: pd.DataFrame, public_trade: pd.DataFrame):
        self.order_book = order_book
        self.public_trade = public_trade
        self.__get_mid_price()

    def analyse(self):
        self.bin_data()
        self.get_direction()
        self.__event_end_times(self.binned_data)
        self.__event_end_prices(self.binned_data)
        self.__assign_event_size_buckets(self.binned_data)
        self.save_to_xls([0.1, 0.2, 0.5, 1.0])
        return self.binned_data
        
    @staticmethod
    def __relative_price_change(df):
        df['Relative price change'] = (df['Mid price|max'] - df['Mid price|min']) / df['Mid price|mean']
        return df['Relative price change']

    @staticmethod
    def __mid_price(df):
        df['Mid price'] = 0.5 * (df['Bid price'] + df['Ask price'])
        return df['Mid price']
    
    def __get_mid_price(self):
        return self.__mid_price(self.order_book)
    
    @staticmethod
    def __rebase_time_column(df, column_name, init_time):
        # Make time series column values relative to inital time
        df[column_name] = df[column_name] - init_time
        return df[column_name]

    def bin_data(self, bucket_size = 0.1):
        # Split data into discrete discrete bins of specified time interval
        init_time = self.order_book.iloc[0]["Transaction time"]
        self.order_book['Time bin'] = ((self.order_book['Transaction time'] - init_time)/bucket_size).astype(int) * bucket_size
        self.binned_data = self.order_book.copy(deep=True).groupby('Time bin').agg({
            'Mid price': ('max', 'min', 'mean', 'idxmax', 'idxmin'),
            })
        self.binned_data.columns = self.binned_data.columns.map('|'.join).str.strip('|')
        self.__relative_price_change(self.binned_data)
        self.__min_max_timestamps()
        return self.binned_data
    
    def __min_max_timestamps(self):
        max_time_stamps = self.order_book.iloc[self.binned_data['Mid price|idxmax']]['Transaction time'].reset_index(drop=True)
        max_time_stamps.name = 'Max timestamp'
        min_time_stamps = self.order_book.iloc[self.binned_data['Mid price|idxmin']]['Transaction time'].reset_index(drop=True)
        min_time_stamps.name = 'Min timestamp'
        self.min_max_time_stamps = pd.concat([max_time_stamps, min_time_stamps], axis=1)
        self.min_max_time_stamps.index = self.binned_data.index
        self.binned_data = pd.concat([self.binned_data, self.min_max_time_stamps], axis=1)
        return self.min_max_time_stamps
    
    @staticmethod
    def __direction(row):
        if row['Min timestamp'] <= row['Max timestamp']:
            return 1
        elif row['Min timestamp'] > row['Max timestamp']:
            return -1
        
    def get_direction(self):
        self.binned_data['Direction'] = self.binned_data.apply(self.__direction, axis=1)
        self.binned_data['Relative price change'] *= self.binned_data['Direction']

    @staticmethod
    def __event_end_times(df):
        df["Event end time"] = df[["Max timestamp", "Min timestamp"]].max(axis=1)
        return df["Event end time"]
    
    @staticmethod
    def __event_end_prices(df):
        conditions = [
            (df['Direction'] == 1),
            (df['Direction'] == -1)
        ]
        choices = [df['Mid price|max'], df['Mid price|min']]
        df['Event end price'] = np.select(conditions, choices)
        return df["Event end price"]
    
    @staticmethod
    def __event_start_times(df):
        df["Event start time"] = df[["Max timestamp", "Min timestamp"]].min(axis=1)
        return df["Event start time"]
    
    @staticmethod
    def __event_start_prices(df):
        conditions = [
            (df['Direction'] == 1),
            (df['Direction'] == -1)
        ]
        choices = [df['Mid price|min'], df['Mid price|max']]
        df['Event start price'] = np.select(conditions, choices)
        return df["Event start price"]
    

    @staticmethod
    def __assign_event_size_buckets(df):
        # Define the ranges and corresponding values
        conditions = [
            (df['Relative price change'] < -0.0040),
            (df['Relative price change'] >= -0.0040) & (df['Relative price change'] < -0.0020),
            (df['Relative price change'] >= -0.0020) & (df['Relative price change'] < -0.0010),
            (df['Relative price change'] >= -0.0010) & (df['Relative price change'] < -0.0005),
            (df['Relative price change'] >= -0.0005) & (df['Relative price change'] < 0.0005),
            (df['Relative price change'] >= 0.0005) & (df['Relative price change'] < 0.0010),
            (df['Relative price change'] >= 0.0010) & (df['Relative price change'] < 0.0020),
            (df['Relative price change'] >= 0.0020) & (df['Relative price change'] < 0.0040),
            (df['Relative price change'] >= 0.0040)
        ]
        values = [
            -4,  # For range: Relative price change < -0.0040
            -3,  # For range: -0.0040 <= Relative price change < -0.0020
            -2,  # For range: -0.0020 <= Relative price change < -0.0010
            -1,  # For range: -0.0010 <= Relative price change < -0.0005
            0,   # For range: -0.0005 <= Relative price change < 0.0005
            1,   # For range: 0.0005 <= Relative price change < 0.0010
            2,   # For range: 0.0010 <= Relative price change < 0.0020
            3,   # For range: 0.0020 <= Relative price change < 0.0040
            4    # For range: Relative price change >= 0.0040
        ]
        df['Event size bucket'] = np.select(conditions, values, default=0)
        return df
    
    @staticmethod
    def get_most_recent_price(timestamp, order_book):
        mask = order_book['Transaction time'] <= timestamp
        if mask.any():
            nearest_time = order_book.loc[mask, 'Transaction time'].max()
            recent_price = order_book.loc[order_book['Transaction time'] == nearest_time, 'Mid price'].values[0]
            return recent_price
        else:
            raise ValueError("There is no 'Transaction time' in the DataFrame that is less than or equal to the input timestamp.")

    def get_post_event_relative_price_change(self, df, time_delay):
        post_event_timestamps = df["Event end time"] + time_delay
        df["Post event price"] = post_event_timestamps.apply(self.get_most_recent_price, args=(self.order_book, ))
        # (P2 - P0) / (P1 - P0)
        df["Post event relative price change"] = (df["Post event price"] - df["Event end price"]) / (df["Event end price"] * df["Relative price change"])
        return df["Post event relative price change"]
    
    def get_relative_price_change_distribution(self, bucket_number, time_delay):
        sized = self.binned_data[self.binned_data["Event size bucket"] == bucket_number]
        self.get_post_event_relative_price_change(sized, time_delay)
        return sized["Post event relative price change"]
    
    def select_xls_data(self, df, time_delay):
        df["P0 Timestamp"] = self.__event_start_times(self.binned_data)
        df["P0"] = self.__event_start_prices(df)
        df["P1"] = self.__event_end_prices(df)
        post_event_timestamps = df["Event end time"] + time_delay
        df["P2"] = np.apply(self.get_most_recent_price, args=(self.order_book, ))
        df["P2-P0/P1-P0"] = (df["P2"] - df["P0"])/(df["P1"] - df["P0"])
        df = df[df["Event size bucket"] != 0]
        return df[["P0 Timestamp", "P0", "P1", "P2", "P2-P0/P1-P0"]]
    
    def save_to_xls(self, time_delays: list):
        with pd.ExcelWriter("output/xls/events_data.xlsx", mode='w') as writer:
            for time_delay in time_delays:
                df_selection = self.select_xls_data(self.binned_data, time_delay)
                df_selection.to_excel(writer, sheet_name=f"P2 at {time_delay*1000} ms", index = False)


class DoubleEmaAnalyser(EventAnalyser):
    def __init__(self, order_book, public_trade, halflife_short, halflife_long):
        super().__init__(order_book, public_trade)
        self.analyse_init(halflife_short, halflife_long)

    def get_ema(self, halflife: float, is_short_ema: bool):
        col_name = "EMA Short" if is_short_ema else "EMA Long"
        halflife = timedelta(seconds=halflife)
        times = self.order_book["Transaction time"].map(datetime.fromtimestamp)
        self.order_book[col_name] = self.order_book["Mid price"].ewm(halflife=halflife, 
                                                                     times = times).mean()
        return self.order_book[col_name]
    
    def get_double_ema(self, hl_short, hl_long):
        self.get_ema(hl_short, True)
        self.get_ema(hl_long, False)
    
    @staticmethod
    def get_ema_intersection_points(short_ema: pd.Series, long_ema: pd.Series):
        intersections = np.diff(np.heaviside(short_ema - long_ema, 0))
        up_intersections = np.heaviside(intersections, 0)
        down_intersections = np.heaviside(-intersections, 0)
        idx_ups = np.argwhere(up_intersections).flatten()
        idx_downs = np.argwhere(down_intersections).flatten()
        return idx_ups, idx_downs

    def get_events(self):
        idx_arr = np.concatenate((self.idx_ups, self.idx_downs))
        idx_arr.sort(kind = 'mergesort')
        start_idx = idx_arr[:-1]
        end_idx = idx_arr[1:]
        start_times = self.order_book["Transaction time"][start_idx].reset_index(drop = True)
        end_times = self.order_book["Transaction time"][end_idx].reset_index(drop = True)
        start_prices = self.order_book["Mid price"][start_idx].reset_index(drop = True)
        end_prices = self.order_book["Mid price"][end_idx].reset_index(drop = True)
        self.events = pd.DataFrame({
            "Start idx": start_idx,
            "End idx": end_idx,
            "Start time": start_times,
            "End time": end_times,
            "Start price": start_prices,
            "End price": end_prices,
            "Duration": end_times - start_times,
            "Relative price change": (end_prices - start_prices)/start_prices
        })
        return self.events
    
    def filter_events(self, events, max_time = 1.0, min_price_std = 1.5):
        filter_1 = events[events["Duration"] < max_time]
        mean = np.mean(filter_1["Relative price change"])
        std = np.std(filter_1["Relative price change"])
        upper_boundary = mean + std
        lower_boundary = mean - std
        filtered_data = filter_1[(filter_1["Relative price change"] > upper_boundary) | (filter_1["Relative price change"] < lower_boundary)]
        return filtered_data

    def analyse_init(self, hl_1, hl_2):
        self.get_double_ema(hl_1, hl_2)
        self.idx_ups, self.idx_downs = self.get_ema_intersection_points(self.order_book["EMA Short"], self.order_book["EMA Long"])


class EmaVarianceAnalyser(EventAnalyser):
    
    def get_ema_variance(self, halflife, alpha):
        halflife = timedelta(seconds=halflife)
        times = self.order_book["Transaction time"].map(datetime.fromtimestamp)
        ema_variance = self.order_book["Mid price"].ewm(halflife=halflife,
                                                        alpha=alpha, 
                                                        times = times).var()
        return ema_variance
    
    @staticmethod
    def variance_peaks(variance):
        peaks, _ = signal.find_peaks(variance)
        min_height = variance.mean() + 2 * variance.std()
        peaks, _ = signal.find_peaks(variance, height = min_height)
        peak_widths = signal.peak_widths(variance, peaks, rel_height=0.99)
        return peaks, min_height, peak_widths
    

if __name__ == "__main__":
    hdfr = HDF5Reader()
    obf = HDF5Reader.read_data('data/sample/order_book.h5')
    ptf = HDF5Reader.read_data('data/sample/public_trade.h5')
    ea = DoubleEmaAnalyser(obf, ptf, 0.001, 0.008)
    