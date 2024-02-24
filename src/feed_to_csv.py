#!/usr/bin/env python3
import os
import sys
import csv
import struct

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


if len(sys.argv) == 2 and sys.argv[1] in ['-b', '-t']:
    data = sys.stdin.buffer.read()
    if sys.argv[1] == '-b':
        order_book_feed(data)
    else:
        public_trade_feed(data)
else:
    for file_name in sys.argv[1:]:
        basename = os.path.basename(file_name)

        rv_md = []
        md_tr = []

        if basename == 'order_book.feed':
            with open(file_name, mode='rb') as file:
                order_book_feed(file.read())

        elif basename == 'public_trade.feed':
            with open(file_name, mode='rb') as file:
                public_trade_feed(file.read())
