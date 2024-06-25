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
        self.order_book["Rebased time"] = self.order_book["Transaction time"] - self.order_book.iloc[0]["Transaction time"]
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

    def get_most_recent_price(self, timestamp):
        mask = self.order_book['Transaction time'] <= timestamp
        if mask.any():
            nearest_time = self.order_book.loc[mask, 'Transaction time'].max()
            recent_price = self.order_book.loc[self.order_book['Transaction time'] == nearest_time, 'Mid price'].values[0]
            return recent_price
        else:
            raise ValueError("There is no 'Transaction time' in the DataFrame that is less than or equal to the input timestamp.")

    def get_post_event_price_change(self, event_time, event_end_price, time_delay):
        post_event_price = self.get_most_recent_price(event_time + time_delay)
        price_change = np.subtract(post_event_price, event_end_price)
        return price_change
    
    def get_post_event_relative_price_change(self, event_time, event_end_price, time_delay):
        price_change = self.get_post_event_price_change(event_time, event_end_price, time_delay)
        relative_price_change = np.divide(price_change, event_end_price)
        return relative_price_change

    def get_relative_price_change_distribution(self, event_times, event_final_prices, time_delays = (0.1, 0.2, 0.5, 1.0, 2.0)):
        pass

if __name__ == "__main__":
    hdfr = HDF5Reader()
    obf = HDF5Reader.read_data('data/sample/order_book.h5')
    ptf = HDF5Reader.read_data('data/sample/public_trade.h5')
    ea = EventAnalyser(obf, ptf)
    ea.analyse()
