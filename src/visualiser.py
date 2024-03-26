#!/usr/bin/env python3
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime

class Visualiser():
    def __init__(self) -> None:
        pass

    def visualise(self):
        self.__read_data()
        self.__plot_price()

    def __read_data(self, path = 'data/sample/order_book.h5'):
        store = pd.HDFStore(path, mode='r')
        self.df = store.get('df')
        store.close()

    def __get_mid_price(self):
        self.df['Mid price'] = 0.5 * (self.df['Bid price'] + self.df['Ask price'])
        return self.df['Mid price']
    
    @staticmethod
    def __get_datetime(df):
        timestamps = np.array(df["Transaction time"])
        utc = np.array([datetime.fromtimestamp(ts) for ts in timestamps])
        return utc

    def __plot_price(self):
        self.df['Transaction UTC'] = self.__get_datetime(self.df)
        self.__get_mid_price()
        xfmt = mpl.dates.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax = plt.gca()
        ax.xaxis.set_major_formatter(xfmt)
        plt.plot(self.df['Transaction UTC'], self.df['Mid price'])
        plt.xticks(rotation=25, ha='right')
        plt.subplots_adjust(left=0.2, bottom=0.3)
        plt.title('Mid Price')
        plt.xlabel('UTC Time')
        plt.ylabel('(USD?)')
        plt.savefig('price.png')

if __name__ == '__main__':
    Visualiser().visualise()
