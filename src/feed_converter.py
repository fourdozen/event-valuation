#!/usr/bin/env python3
import os
import sys
import struct
import numpy as np
import pandas as pd
from hdf5reader import HDF5Reader

class FeedConverter():

    def __init__(self, data, form: str, column_names: list):
        self.data = data
        self.form = form
        self.column_names = column_names

    def convert(self):
        self._unpack_to_dataframe()
        self._save_to_hdfstore()

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
    
    def _save_to_hdfstore(self, path):
        HDF5Reader.write_data(path, self.df)

class OrderBookFeedConverter(FeedConverter):

    def __init__(self, data):
        column_names = ["Received time", "MD entry time", "Transaction time", 
                        "Seq Id", "Bid qty", "Bid price", "Ask qty", "Ask price"]
        form = "QQQQqqqq"
        super().__init__(data, form, column_names)

    def convert(self):
        return super().convert()

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

    def _save_to_hdfstore(self, path='data/sample/order_book.h5'):
        return super()._save_to_hdfstore(path)


class PublicTradeFeedConverter(FeedConverter):

    def __init__(self, data):
        form = "QQQQqq"
        column_names = ["Received time", "MD entry time", "Transaction time", 
                        "Seq Id", "Trade qty", "Trade price"]
        super().__init__(data, form, column_names)

    def convert(self):
        return super().convert()

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
    
    def _save_to_hdfstore(self, path='data/sample/public_trade.h5'):
        return super()._save_to_hdfstore(path)

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
