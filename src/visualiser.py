import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import h5py

class Visualiser():
    def __init__(self) -> None:
        pass

    def __read_data(self, path = 'data/sample/order_book.h5'):
        store = pd.HDFStore(path, mode='r')
        self.df = store.get('df')
        store.close()
        print(self.df.columns)

    def visualise(self):
        self.__read_data()
        self.__plot_price()

    def __plot_price(self):
        plt.plot(self.df['Transaction time'], self.df['Bid price'])
        plt.plot(self.df['Transaction time'], self.df['Ask price'])
        plt.savefig('price.png')

if __name__ == '__main__':
    Visualiser().visualise()
