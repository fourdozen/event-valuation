import pandas as pd

def read_data(path = 'data/sample/'):
    ob_store = pd.HDFStore(path + 'order_book.h5', mode='r')
    pt_store = pd.HDFStore(path + 'public_trade.h5', mode='r')
    order_book = ob_store.get('df')
    public_trade = pt_store.get('df')
    ob_store.close()
    pt_store.close()
    return order_book, public_trade