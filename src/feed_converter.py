#!/usr/bin/env python3
import os
import sys
import struct
import numpy as np
import pandas as pd

class FeedConverter():

    def __init__(self, data, form: str, column_names: list):
        self.data = data
        self.form = form
        self.column_names = column_names

    def convert(self, name, path):
        df = self._unpack_to_dataframe()
        self._save_to_hdfstore(name, path)

    def _save_to_csv(self, path):
        self.df.to_csv(path, index=False)

    def _unpack_row(self, fields):
        pass
        
    def _unpack_to_arr(self):
        self.data_arr = np.array(
            [self._unpack_row(fields) for fields in struct.iter_unpack(self.form, self.data)]
        )
        return self.data_arr
    
    def _unpack_to_dataframe(self) -> pd.DataFrame:
        arr = self._unpack_to_arr()
        self.df = pd.DataFrame(
            arr,
            columns = self.column_names,
        )
        return self.df
        
    # def bin_data(self, bucket_size = 0.01):
    #     # Split data into discrete discrete bins of fixed time interval
    #     init_time = self.df.iloc[0]["Transaction time"]
    #     df_copy = self.df.copy(deep = True)
    #     df_copy['bin'] = ((df_copy['Transaction time'] - init_time)/bucket_size).astype(int) * bucket_size
    #     self.binned_data = df_copy.groupby('bin').agg({
    #             'Bid qty': 'mean', 
    #             'Bid price': 'mean',
    #             'Ask qty': 'mean',
    #             "Ask price": 'mean'
    #         })
    #     return self.binned_data
    
    def _save_to_hdfstore(self, name, path):
        self.store = pd.HDFStore(path)
        self.store[name] = self.df


class OrderBookFeedConverter(FeedConverter):

    def __init__(self, data):
        column_names = ["Received time", "MD entry time", "Transaction time", 
                        "Seq Id", "Bid qty", "Bid price", "Ask qty", "Ask price"]
        form = "QQQQqqqq"
        super().__init__(data, form, column_names)

    def convert(self, name='order_book', path='data/sample/order_book.h5'):
        return super().convert(name, path)

    def _unpack_row(self, fields) -> np.array:
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
    
    def _unpack_to_arr(self) -> np.array:
        super()._unpack_to_arr()
        return self.data_arr

    def _save_to_csv(self, path = 'data/sample/order_book.csv'):
        super()._save_to_csv(path)

    # @staticmethod
    # def mid_price(bid_price, ask_price):
    #     return (bid_price + ask_price)/2
    
    # def get_mid_price(self):
    #     self.df["Mid Price"] = self.mid_price(self.df["Bid price"], self.df["Ask price"])

    def _save_to_hdfstore(self, name="order_book", path="data/sample/order_book.h5"):
        return super()._save_to_hdfstore(name, path)


class PublicTradeFeedConverter(FeedConverter):

    def __init__(self, data):
        form = "QQQQqq"
        column_names = ["Received time", "MD entry time", "Transaction time", 
                        "Seq Id", "Trade qty", "Trade price"]
        super().__init__(data, form, column_names)

    def convert(self, name='public_trade', path='data/sample/public_trade.h5'):
        return super().convert(name, path)

    def _unpack_row(self, fields) -> np.array:
        # Unpack binary row to array of data
        received_time = int(fields[0])/10**9
        md_entry_time = int(fields[1])/10**9
        transact_time = int(fields[2])/10**9
        seq_id = int(fields[3])
        trd_qty = int(fields[4])/10**8
        trd_prc = int(fields[5])/10**8
        return np.array([received_time, md_entry_time, transact_time, seq_id,
                trd_qty, trd_prc])

    def _unpack_to_arr(self) -> np.array:
        super()._unpack_to_arr()
        return self.data_arr

    def _save_to_csv(self, path = 'data/sample/public_trade.csv'):
        super()._save_to_csv(path)

    # def bin_data(self, bucket_size = 0.01):
    #     # Split data into discrete discrete bins of fixed time interval
    #     init_time = self.df.iloc[0]["Transaction time"]
    #     df_copy = self.df.copy(deep = True)
    #     df_copy['bin'] = ((df_copy['Transaction time'] - init_time)/bucket_size).astype(int) * bucket_size
    #     self.binned_data = df_copy.groupby('bin').agg({
    #             'Trade qty': ['sum', 'mean'], 
    #             'Trade price': ['max', 'min'],
    #         })
    #     self.binned_data['Volume'] = self.binned_data['Trade qty']['sum']
    #     self.binned_data['Depth'] = self.binned_data['Trade price']['max'] - self.binned_data['Trade price']['min']
    #     self.binned_data['Norm depth'] = np.divide(self.binned_data['Volume'],self.binned_data['Trade qty']['mean']) 
    #     return self.binned_data
    
    def _save_to_hdfstore(self, name='public_trade', path="data/sample/public_trade.h5"):
        return super()._save_to_hdfstore(name, path)


# def order_book_FeedConverter(data):
#     for fields in struct.iter_unpack('QQQQqqqq', data):
#         received_time = int(fields[0])/10**9
#         md_entry_time = int(fields[1])/10**9
#         transact_time = int(fields[2])/10**9
#         seq_id = int(fields[3])
#         bid_qty = int(fields[4])/10**8
#         bid_prc = int(fields[5])/10**8
#         ask_qty = int(fields[6])/10**8
#         ask_prc = int(fields[7])/10**8

#         print('{0:.6f},{1:.6f},{2:.6f},{3},{4},{5},{6},{7}'.format(
#             received_time, md_entry_time, transact_time, seq_id,
#             bid_qty, bid_prc, ask_qty, ask_prc))

# def public_trade_FeedConverter(data):
#     for fields in struct.iter_unpack('QQQQqq', data):
#         received_time = int(fields[0])/10**9
#         md_entry_time = int(fields[1])/10**9
#         transact_time = int(fields[2])/10**9
#         seq_id = int(fields[3])
#         trd_qty = int(fields[4])/10**8
#         trd_prc = int(fields[5])/10**8

#         print('{0:.6f},{1:.6f},{2:.6f},{3},{4},{5}'.format(
#             received_time, md_entry_time, transact_time, seq_id,
#             trd_qty, trd_prc))


if len(sys.argv) == 2 and sys.argv[1] in ['-b', '-t']:
    data = sys.stdin.buffer.read()
    if sys.argv[1] == '-b':
        obf = OrderBookFeedConverter(data)
        obf._save_to_csv()
    else:
        ptf = PublicTradeFeedConverter(data)
        ptf._save_to_csv()

else:
    for file_name in sys.argv[1:]:
        basename = os.path.basename(file_name)

        rv_md = []
        md_tr = []

        if basename == 'order_book.feed':
            with open(file_name, mode='rb') as file:
                obf = OrderBookFeedConverter(file.read())
                obf.convert()

        elif basename == 'public_trade.feed':
            with open(file_name, mode='rb') as file:
                ptf = PublicTradeFeedConverter(file.read())
                ptf.convert()
