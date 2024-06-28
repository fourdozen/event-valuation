import pandas as pd
import numpy as np
from hdf5reader import HDF5Reader

class EventAnalyser():
    def __init__(self, order_book: pd.DataFrame, public_trade: pd.DataFrame):
        self.order_book = order_book
        self.public_trade = public_trade

    def analyse(self):
        self.__get_mid_price()
        self.bin_data()
        self.get_direction()
        self.__event_end_times(self.binned_data)
        self.__event_end_prices(self.binned_data)
        self.__assign_event_size_buckets(self.binned_data)
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
        subset = self.binned_data[self.binned_data["Event size bucket"] == bucket_number]
        self.get_post_event_relative_price_change(subset, time_delay)
        return subset["Post event relative price change"]

if __name__ == "__main__":
    hdfr = HDF5Reader()
    obf = HDF5Reader.read_data('data/sample/order_book.h5')
    ptf = HDF5Reader.read_data('data/sample/public_trade.h5')
    ea = EventAnalyser(obf, ptf)
    ea.analyse()