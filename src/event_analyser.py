import pandas as pd
from hdf5reader import HDF5Reader
import matplotlib.pyplot as plt

class EventAnalyser():
    def __init__(self, order_book: pd.DataFrame, public_trade: pd.DataFrame):
        self.order_book = order_book
        self.public_trade = public_trade

    @staticmethod
    def __relative_price_change(df):
        mp_col = df['Mid price']
        df['Relative price change'] = (mp_col['max'] - mp_col['min']) / mp_col['mean']
        return df['Relative price change']

    @staticmethod
    def __mid_price(df):
        df['Mid price'] = 0.5 * (df['Bid price'] + df['Ask price'])
        return df['Mid price']
    
    def __get_mid_price(self):
        self.__mid_price(self.order_book)

    def price_direction(self):
        mp_col = self.binned_data['Mid price']

    def bin_data(self, bucket_size = 0.1):
        # Split data into discrete discrete bins of fixed time interval
        init_time = self.order_book.iloc[0]["Transaction time"]
        self.order_book['Time bin'] = ((self.order_book['Transaction time'] - init_time)/bucket_size).astype(int) * bucket_size
        self.binned_data = self.order_book.copy(deep=True).groupby('Time bin').agg({
            'Mid price': ('max', 'min', 'mean', 'idxmax', 'idxmin'),
            })
        self.__relative_price_change(self.binned_data)
        self.__min_max_timestamps()
        return self.binned_data
    
    def plot_price_change_distribution(self):
        price_change = self.binned_data['Relative price change']
        fig, ax = plt.subplots()
        ax.hist(price_change, bins=50)
        ax.set_yscale('log')
        ax.set_title(r'Distribution of $\frac{\left|{\Delta P}\right|}{\bar{P}}$, where $P$ is the Mid price')
        ax.set_ylabel('Count')
        ax.set_xlabel(r'$\frac{\left|{\Delta P}\right|}{\bar{P}}$')
        plt.tight_layout()
        plt.savefig('price_change_distribution.png', dpi=300)

    def __min_max_timestamps(self):
        max_time_stamps = self.order_book.iloc[self.binned_data['Mid price', 'idxmax']]['Transaction time'].reset_index(drop=True)
        max_time_stamps.name = 'Max timestamp'
        min_time_stamps = self.order_book.iloc[self.binned_data['Mid price', 'idxmin']]['Transaction time'].reset_index(drop=True)
        min_time_stamps.name = 'Min timestamp'
        self.min_max_time_stamps = pd.concat([max_time_stamps, min_time_stamps], axis=1)
        self.binned_data.join(self.min_max_time_stamps)
        return self.min_max_time_stamps
    
    def analyse(self):
        self.__get_mid_price()
        self.bin_data(0.1)
        self.plot_price_change_distribution()
        self.order_book["Transaction time"] = self.order_book["Transaction time"] - self.order_book.iloc[0]["Transaction time"]
        print(self.binned_data.head(10))


if __name__ == "__main__":
    hdfr = HDF5Reader()
    obf = HDF5Reader.read_data('data/sample/order_book.h5')
    ptf = HDF5Reader.read_data('data/sample/public_trade.h5')
    ea = EventAnalyser(obf, ptf)
    ea.analyse()