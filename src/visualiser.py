#!/usr/bin/env python3
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
from hdf5reader import HDF5Reader
from event_analyser import *

class Visualiser():

    def __init__(self, order_book, public_trade) -> None:
        self.order_book = order_book
        self.public_trade = public_trade
        self.utc_to_timestamp()

    def visualise(self):
        pass

    def utc_to_timestamp(self):
        self.order_book['Transaction UTC'] = self.__get_datetime(self.order_book)
        self.public_trade['Transaction UTC'] = self.__get_datetime(self.public_trade)

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

    def plot_price_change_distribution(self):
        # Get data (Need to change to be separate)
        ea = EventAnalyser(self.order_book, self.public_trade)
        ea.analyse()
        binned_data = ea.binned_data
        # Plot price change
        price_change = binned_data['Relative price change']
        fig, ax = plt.subplots()
        ax.hist(price_change, bins=100)
        ax.set_yscale('log')
        ax.set_title(r'Distribution of $\frac{\Delta P}{\bar{P}}$ (relative price change), where $P$ is the Mid price')
        ax.set_ylabel('Count')
        ax.set_xlabel(r'$\frac{\Delta P}{\bar{P}}$')
        plt.tight_layout()
        plt.savefig('price_change_distribution.png', dpi=300)

    def plot_post_event_price_change_dist(self, bin: int, delay):
        event_size_dict = {
            -4: "(-∞, -0.0040)",
            -3: "[-0.0040, -0.0020)",
            -2: "[-0.0020, -0.0010)",
            -1: "[-0.0010, -0.0005)",
            0: "[-0.0005, 0.0005)",
            1: "[0.0005, 0.0010)",
            2: "[0.0010, 0.0020)",
            3: "[0.0020, 0.0040)",
            4: "[0.0040, ∞)"
        }
        ea = EventAnalyser(self.order_book, self.public_trade)
        ea.analyse()
        dist = ea.get_relative_price_change_distribution(bin, delay)
        mean = dist.mean()
        std_dev = dist.std()
        n = dist.count()
        fig, ax = plt.subplots()
        ax.hist(dist, bins=25)
        ax.set_title(f"Distribution of relative price changes {delay * 1000} ms after event, for events of size {event_size_dict[bin]}", wrap=True)
        ax.set_ylabel("Count")
        ax.set_xlabel(r"Relative price change $(P_2 - P_0)/(P_1 - P_0)$")
        plt.axvline(mean, color='r', linestyle='dashed', linewidth=1)
        plt.axvline(mean - std_dev, color='g', linestyle='dashed', linewidth=1)
        plt.axvline(mean + std_dev, color='g', linestyle='dashed', linewidth=1)
        plt.tight_layout()
        plt.savefig(f"post_event_price_change_distribution_{delay}_{bin}.png")

    def plot_double_ema(self, ema_1_hl, ema_2_hl):
        ea = DoubleEmaAnalyser(self.order_book, self.public_trade)
        ea.get_double_ema(ema_1_hl, ema_2_hl)
        ups_idx, downs_idx = ea.get_ema_intersection_points(self.order_book["EMA Short"], self.order_book["EMA Long"])
        _, (ax_price, ax_diff) = plt.subplots(2, 1, sharex='col')
        ax_price.plot(self.order_book["Transaction time"], self.order_book["Mid price"], label = "Mid price", color='grey')
        ax_price.plot(self.order_book["Transaction time"], self.order_book["EMA Short"], label = f"EMA {ema_1_hl}s")
        ax_price.plot(self.order_book["Transaction time"], self.order_book["EMA Long"], label = f"EMA {ema_2_hl}s")
        ax_price.plot(self.order_book["Transaction time"][ups_idx], self.order_book["EMA Short"][ups_idx], "go")
        ax_price.plot(self.order_book["Transaction time"][downs_idx], self.order_book["EMA Short"][downs_idx], "ro")
        ax_diff.plot(self.order_book["Transaction time"], self.order_book["EMA Short"] - self.order_book["EMA Long"])
        ax_diff.axhline(color = 'grey', ls = '--')
        ax_diff.set_xlabel("Timestamp")
        ax_diff.set_ylabel("Difference")
        ax_price.set_ylabel("USD")
        ax_price.legend(loc="lower right")
        ax_diff.autoscale(axis='y')
        ax_price.autoscale(axis='y')
        plt.show()

    def plot_ema_variance(self, halflife, alpha: float):
        ea = EmaVarianceAnalyser(self.order_book, self.public_trade)
        variance = ea.get_ema_variance(halflife, alpha)
        peak_idx, var_threshold, peak_widths = ea.variance_peaks(variance)
        fig, (ax_price, ax_var) = plt.subplots(2, 1, sharex='col')
        ax_var.plot(self.order_book["Transaction time"], variance)
        ax_var.set_xlabel("Timestamp")
        ax_var.set_ylabel("EMA Variance")
        ax_price.set_ylabel("Mid price")
        ax_var.hlines(peak_widths[1],
                      self.order_book["Transaction time"][peak_widths[2].astype(int)],
                      self.order_book["Transaction time"][peak_widths[3].astype(int)],
                      color="orange",
                      ls = 'dashed')
        ax_var.plot(self.order_book["Transaction time"][peak_idx], variance[peak_idx], 'rx', label="P1")
        ax_var.plot(self.order_book["Transaction time"][peak_widths[2].astype(int)], variance[peak_widths[2].astype(int)], 'gx', label = "P0")
        ax_var.plot(self.order_book["Transaction time"][peak_widths[3].astype(int)], variance[peak_widths[3].astype(int)], 'bx', label = "P2")
        ax_price.plot(self.order_book["Transaction time"], self.order_book["Mid price"], label = "Mid price", color='grey')
        ax_price.plot(self.order_book["Transaction time"][peak_idx], self.order_book["Mid price"][peak_idx], 'rx', label = "P1")
        ax_price.plot(self.order_book["Transaction time"][peak_widths[2].astype(int)], self.order_book["Mid price"][peak_widths[2].astype(int)],'gx', label = 'P0')
        ax_price.plot(self.order_book["Transaction time"][peak_widths[3].astype(int)], self.order_book["Mid price"][peak_widths[3].astype(int)],'bx', label = 'P2')
        ax_price.legend()
        ax_var.legend()
        fig.suptitle(f'Halflife: {halflife}, Alpha: {alpha}')
        plt.show()

    def plot_duration_size_corr(self, hl_s, hl_l):
        ea = DoubleEmaAnalyser(self.order_book, self.public_trade, hl_s, hl_l)
        events = ea.get_events()
        time_susbset = events[events["Duration"] < 1.0]
        data = time_susbset[time_susbset["Relative price change"] != 0.0]
        mean_rpc = np.mean(time_susbset["Relative price change"])
        std_rpc = np.std(time_susbset["Relative price change"])
        plt.scatter(x = "Duration", y = "Relative price change", marker = 'x', data = data)
        plt.axhline(y = mean_rpc, color="red", ls='--')
        plt.axhline(y = mean_rpc + 1.5 * std_rpc, color = "orange", ls='--')
        plt.axhline(y = mean_rpc - 1.5 * std_rpc, color = "orange", ls='--')
        plt.xlabel("Duration /s")
        plt.ylabel("Relative price change")
        plt.title("Scatter plot of price change and duration")
        plt.show()

if __name__ == '__main__':
    hdfr = HDF5Reader()
    obf = HDF5Reader.read_data('data/sample/order_book.h5')
    ptf = HDF5Reader.read_data('data/sample/public_trade.h5')
    Visualiser(obf, ptf).plot_duration_size_corr(0.001, 0.008)
