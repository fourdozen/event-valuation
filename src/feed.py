#!/usr/bin/env python3
import os
import sys
import csv
import struct
import numpy as np
import pandas as pd

class Feed():
    def __init__(self, data):
        self.data = data
        self.data_arr = self.unpack_to_arr()

    def save_to_csv(self, path):
        np.savetxt(path, self.data_arr, delimiter=',')
        
    def unpack_to_arr(self, form):
        self.data_arr = np.array([self.unpack_row(fields) for fields in struct.iter_unpack(form, self.data)])
        return self.data_arr


class OrderBookFeed(Feed):

    def unpack_row(self, fields) -> np.array:
        # Unpack binary row to array of data
        received_time = int(fields[0])/10**9
        md_entry_time = int(fields[1])/10**9
        transact_time = int(fields[2])/10**9
        seq_id = int(fields[3])
        bid_qty = int(fields[4])/10**8
        bid_prc = int(fields[5])/10**8
        ask_qty = int(fields[6])/10**8
        ask_prc = int(fields[7])/10**8
        return np.array([received_time, md_entry_time, transact_time, seq_id,
            bid_qty, bid_prc, ask_qty, ask_prc])
    
    def unpack_to_arr(self) -> np.array:    
        super().unpack_to_arr(form='QQQQqqqq')
        return self.data_arr

    def save_to_csv(self):
        super().save_to_csv(path = '../data/sample/order_book.csv')

    def bin_data(time_interval = 50):
        pass


class PublicTradeFeed(Feed):

    def unpack_row(self, fields) -> np.array:
        # Unpack binary row to array of data
        received_time = int(fields[0])/10**9
        md_entry_time = int(fields[1])/10**9
        transact_time = int(fields[2])/10**9
        seq_id = int(fields[3])
        trd_qty = int(fields[4])/10**8
        trd_prc = int(fields[5])/10**8
        return np.array([received_time, md_entry_time, transact_time, seq_id,
                trd_qty, trd_prc])

    def unpack_to_arr(self) -> np.array:
        super().unpack_to_arr(form='QQQQqq')
        return self.data_arr

    def save_to_csv(self):
        super().save_to_csv(path = '../data/sample/public_trade.csv')


def order_book_feed(data):
    for fields in struct.iter_unpack('QQQQqqqq', data):
        received_time = int(fields[0])/10**9
        md_entry_time = int(fields[1])/10**9
        transact_time = int(fields[2])/10**9
        seq_id = int(fields[3])
        bid_qty = int(fields[4])/10**8
        bid_prc = int(fields[5])/10**8
        ask_qty = int(fields[6])/10**8
        ask_prc = int(fields[7])/10**8

        print('{0:.6f},{1:.6f},{2:.6f},{3},{4},{5},{6},{7}'.format(
            received_time, md_entry_time, transact_time, seq_id,
            bid_qty, bid_prc, ask_qty, ask_prc))



def public_trade_feed(data):
    for fields in struct.iter_unpack('QQQQqq', data):
        received_time = int(fields[0])/10**9
        md_entry_time = int(fields[1])/10**9
        transact_time = int(fields[2])/10**9
        seq_id = int(fields[3])
        trd_qty = int(fields[4])/10**8
        trd_prc = int(fields[5])/10**8

        print('{0:.6f},{1:.6f},{2:.6f},{3},{4},{5}'.format(
            received_time, md_entry_time, transact_time, seq_id,
            trd_qty, trd_prc))



def unpack_trade_row(fields):
    received_time = int(fields[0])/10**9
    md_entry_time = int(fields[1])/10**9
    transact_time = int(fields[2])/10**9
    seq_id = int(fields[3])
    trd_qty = int(fields[4])/10**8
    trd_prc = int(fields[5])/10**8
    return np.array([received_time, md_entry_time, transact_time, seq_id,
            trd_qty, trd_prc])


def unpack_trade_feed(fields):
    received_time = int(fields[0])/10**9
    md_entry_time = int(fields[1])/10**9
    transact_time = int(fields[2])/10**9
    seq_id = int(fields[3])
    trd_qty = int(fields[4])/10**8
    trd_prc = int(fields[5])/10**8
    return np.array([received_time, md_entry_time, transact_time, seq_id,
            trd_qty, trd_prc])


print(sys.argv)
if len(sys.argv) == 2 and sys.argv[1] in ['-b', '-t']:
    data = sys.stdin.buffer.read()
    if sys.argv[1] == '-b':
        obf = OrderBookFeed(data)
        obf.save_to_csv()
    else:
        ptf = PublicTradeFeed(data)
        ptf.save_to_csv()

else:
    for file_name in sys.argv[1:]:
        basename = os.path.basename(file_name)

        rv_md = []
        md_tr = []

        if basename == 'order_book.feed':
            with open(file_name, mode='rb') as file:
                obf = OrderBookFeed(file.read())
                obf.save_to_csv()

        elif basename == 'public_trade.feed':
            with open(file_name, mode='rb') as file:
                ptf = public_trade_feed(file.read())
                ptf.save_to_csv()