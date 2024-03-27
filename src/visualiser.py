#!/usr/bin/env python3
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
from hpf5reader import read_data

class Visualiser():

    def __init__(self, order_book, public_trade) -> None:
        self.order_book = order_book
        self.public_trade = public_trade
        self.order_book['Transaction UTC'] = self.__get_datetime(self.order_book)
        self.public_trade['Transaction UTC'] = self.__get_datetime(self.public_trade)

    def visualise(self):
        self.plot_basic_data()

    @staticmethod
    def __get_mid_price(df):
        df['Mid price'] = 0.5 * (df['Bid price'] + df['Ask price'])
        return df['Mid price']
    
    @staticmethod
    def __get_datetime(df):
        timestamps = np.array(df["Transaction time"])
        utc = np.array([datetime.fromtimestamp(ts) for ts in timestamps])
        return utc
    
    @staticmethod
    def __get_spread(df):
        df['Spread'] = df['Ask price'] - df['Bid price']
        return df['Spread']

    def plot_mid_price(self, ax):
        self.__get_mid_price(self.order_book)
        xfmt = mpl.dates.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.plot(self.order_book['Transaction UTC'], self.order_book['Mid price'])
        plt.xticks(rotation=25, ha='right')
        plt.subplots_adjust(left=0.2, bottom=0.3)
        ax.set_title('Order Book Mid Price')
        ax.set_ylabel('Price (USD)')

    def plot_volume(self, ax):
        ax.plot(self.public_trade['Transaction UTC'], np.abs(self.public_trade['Trade qty']))
        ax.set_title('Public Trade Volume')
        ax.set_ylabel('Traded Qty (Volume)')

    def plot_spread(self, ax):
        self.__get_spread(self.order_book)
        ax.plot(self.order_book['Transaction UTC'], self.order_book['Spread'])
        ax.set_title('Order Book Spread')
        ax.set_ylabel('(USD)')

    def plot_basic_data(self):
        fig, (ax0, ax1, ax2) = plt.subplots(3, 1, sharex='col', figsize=(10.5, 13.5))
        self.plot_mid_price(ax0)
        self.plot_volume(ax1)
        self.plot_spread(ax2)
        plt.xlabel('Time UTC')
        plt.tight_layout()
        plt.savefig('basic_data.png', dpi=300)


if __name__ == '__main__':
    obf, ptf = read_data() 
    Visualiser(obf, ptf).visualise()
