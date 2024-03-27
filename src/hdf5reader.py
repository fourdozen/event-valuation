import pandas as pd

class HDF5Reader():
    def __init__(self):
        pass

    @staticmethod
    def read_data(path):
        store = pd.HDFStore(path, mode='r')
        df = store.get('df')
        store.close()
        return df

    @staticmethod
    def write_data(path, df):
        store = pd.HDFStore(path, 'w')
        store.put('df', df, data_columns=True)
        store.close()