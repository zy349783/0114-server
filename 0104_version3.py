#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 19:04:54 2021

@author: zhenyu
"""

import os
import sys
import glob
import datetime
import numpy as np
import pandas as pd
from multiprocessing import Pool

funcPath = r'/home/work516'
sys.path.append(funcPath)
from generate5LvMBD_1203 import generateMBD

import os
import pandas as pd

os.environ['OMP_NUM_THREADS'] = '1'
import glob
import pymongo
import numpy as np
import pandas as pd
import pickle
import time
import gzip
import lzma
import pytz
import warnings
import glob
import datetime
from collections import defaultdict, OrderedDict
import pyarrow as pa
import pyarrow.parquet as pq
import io

warnings.filterwarnings(action='ignore')


def DB(host, db_name, user, passwd):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    uri = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    return DBObj(uri, db_name=db_name)


class DBObj(object):
    def __init__(self, uri, symbol_column='skey', db_name='white_db', version=3):
        self.db_name = db_name
        self.uri = uri
        self.client = pymongo.MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.chunk_size = 20000
        self.symbol_column = symbol_column
        self.date_column = 'date'
        self.version = version

    def parse_uri(self, uri):
        # mongodb://user:password@example.com
        return uri.strip().replace('mongodb://', '').strip('/').replace(':', ' ').replace('@', ' ').split(' ')

    def drop_table(self, table_name):
        self.db.drop_collection(table_name)

    def rename_table(self, old_table, new_table):
        self.db[old_table].rename(new_table)

    def write(self, table_name, df):
        if len(df) == 0: return

        multi_date = False

        if self.date_column in df.columns:
            date = str(df.head(1)[self.date_column].iloc[0])
            multi_date = len(df[self.date_column].unique()) > 1
        else:
            raise Exception('DataFrame should contain date column')

        collection = self.db[table_name]
        collection.create_index([('date', pymongo.ASCENDING), ('symbol', pymongo.ASCENDING)], background=True)
        collection.create_index([('symbol', pymongo.ASCENDING), ('date', pymongo.ASCENDING)], background=True)

        if multi_date:
            for (date, symbol), sub_df in df.groupby([self.date_column, self.symbol_column]):
                date = str(date)
                symbol = int(symbol)
                collection.delete_many({'date': date, 'symbol': symbol})
                self.write_single(collection, date, symbol, sub_df)
        else:
            for symbol, sub_df in df.groupby([self.symbol_column]):
                collection.delete_many({'date': date, 'symbol': symbol})
                self.write_single(collection, date, symbol, sub_df)

    def write_single(self, collection, date, symbol, df):
        for start in range(0, len(df), self.chunk_size):
            end = min(start + self.chunk_size, len(df))
            df_seg = df[start:end]
            version = self.version
            ser_data = self.ser(df_seg, version)
            seg = {'ver': version, 'data': ser_data, 'date': date, 'symbol': symbol, 'start': start}
            collection.insert_one(seg)

    def build_query(self, start_date=None, end_date=None, symbol=None):
        query = {}

        def parse_date(x):
            if type(x) == str:
                if len(x) != 8:
                    raise Exception("`date` must be YYYYMMDD format")
                return x
            elif type(x) == datetime.datetime or type(x) == datetime.date:
                return x.strftime("%Y%m%d")
            elif type(x) == int:
                return parse_date(str(x))
            else:
                raise Exception("invalid `date` type: " + str(type(x)))

        if start_date is not None or end_date is not None:
            query['date'] = {}
            if start_date is not None:
                query['date']['$gte'] = parse_date(start_date)
            if end_date is not None:
                query['date']['$lte'] = parse_date(end_date)

        def parse_symbol(x):
            if type(x) == int:
                return x
            else:
                return int(x)

        if symbol:
            if type(symbol) == list or type(symbol) == tuple:
                query['symbol'] = {'$in': [parse_symbol(x) for x in symbol]}
            else:
                query['symbol'] = parse_symbol(symbol)

        return query

    def delete(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name]

        query = self.build_query(start_date, end_date, symbol)
        if not query:
            print('cannot delete the whole table')
            return None

        collection.delete_many(query)

    def read(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name]

        query = self.build_query(start_date, end_date, symbol)
        if not query:
            print('cannot read the whole table')
            return None

        segs = []
        for x in collection.find(query):
            x['data'] = self.deser(x['data'], x['ver'])
            segs.append(x)
        segs.sort(key=lambda x: (x['symbol'], x['date'], x['start']))
        return pd.concat([x['data'] for x in segs], ignore_index=True) if segs else None

    def read_raw(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name]

        query = self.build_query(start_date, end_date, symbol)
        if not query:
            print('cannot read the whole table')
            return None

        return collection.find(query)

    def list_tables(self):
        return self.db.collection_names()

    def list_dates(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name]
        dates = set()
        if start_date is None:
            start_date = '00000000'
        if end_date is None:
            end_date = '99999999'
        for x in collection.find(self.build_query(start_date, end_date, symbol), {"date": 1, '_id': 0}):
            dates.add(x['date'])
        return sorted(list(dates))

    def ser(self, s, version):
        pickle_protocol = 4
        if version == 1:
            return gzip.compress(pickle.dumps(s, protocol=pickle_protocol), compresslevel=2)
        elif version == 2:
            return lzma.compress(pickle.dumps(s, protocol=pickle_protocol), preset=1)
        elif version == 3:
            # 32-bit number needs more space than 64-bit for parquet
            for col_name in s.columns:
                col = s[col_name]
                if col.dtype == np.int32:
                    s[col_name] = col.astype(np.int64)
                elif col.dtype == np.uint32:
                    s[col_name] = col.astype(np.uint64)
            tbl = pa.Table.from_pandas(s)
            f = io.BytesIO()
            pq.write_table(tbl, f, use_dictionary=False, compression='ZSTD', compression_level=0)
            f.seek(0)
            data = f.read()
            return data
        else:
            raise Exception('unknown version')

    def deser(self, s, version):
        def unpickle(s):
            return pickle.loads(s)

        if version == 1:
            return unpickle(gzip.decompress(s))
        elif version == 2:
            return unpickle(lzma.decompress(s))
        elif version == 3:
            f = io.BytesIO()
            f.write(s)
            f.seek(0)
            return pq.read_table(f, use_threads=False).to_pandas()
        else:
            raise Exception('unknown version')



def patch_pandas_pickle():
    if pd.__version__ < '0.24':
        import sys
        from types import ModuleType
        from pandas.core.internals import BlockManager
        pkg_name = 'pandas.core.internals.managers'
        if pkg_name not in sys.modules:
            m = ModuleType(pkg_name)
            m.BlockManager = BlockManager
            sys.modules[pkg_name] = m


import pymongo
import pandas as pd
import pickle
import datetime
import time
import gzip
import lzma
import pytz
import numpy as np


def DB1(host, db_name, user, passwd):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    url = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    client = pymongo.MongoClient(url, maxPoolSize=None)
    db = client[db_name]
    return db


def build_query(start_date=None, end_date=None, index_id=None):
    query = {}

    def parse_date(x):
        if type(x) == int:
            return x
        elif type(x) == str:
            if len(x) != 8:
                raise Exception("`date` must be YYYYMMDD format")
            return int(x)
        elif type(x) == datetime.datetime or type(x) == datetime.date:
            return x.strftime("%Y%m%d").astype(int)
        else:
            raise Exception("invalid `date` type: " + str(type(x)))

    if start_date is not None or end_date is not None:
        query['date'] = {}
        if start_date is not None:
            query['date']['$gte'] = parse_date(start_date)
        if end_date is not None:
            query['date']['$lte'] = parse_date(end_date)

    def parse_symbol(x):
        if type(x) == int:
            return x
        else:
            return int(x)

    if index_id:
        if type(index_id) == list or type(index_id) == tuple:
            query['index_id'] = {'$in': [parse_symbol(x) for x in index_id]}
        else:
            query['index_id'] = parse_symbol(index_id)

    return query


def build_filter_query(start_date=None, end_date=None, skey=None):
    query = {}

    def parse_date(x):
        if type(x) == int:
            return x
        elif type(x) == str:
            if len(x) != 8:
                raise Exception("`date` must be YYYYMMDD format")
            return int(x)
        elif type(x) == datetime.datetime or type(x) == datetime.date:
            return x.strftime("%Y%m%d").astype(int)
        else:
            raise Exception("invalid `date` type: " + str(type(x)))

    if start_date is not None or end_date is not None:
        query['date'] = {}
        if start_date is not None:
            query['date']['$gte'] = parse_date(start_date)
        if end_date is not None:
            query['date']['$lte'] = parse_date(end_date)

    def parse_symbol(x):
        if type(x) == int:
            return x
        else:
            return int(x)

    if skey:
        if type(skey) == list or type(skey) == tuple:
            query['skey'] = {'$in': [parse_symbol(x) for x in skey]}
        else:
            query['skey'] = parse_symbol(skey)

    return query


def read_filter_daily(db, name, start_date=None, end_date=None, skey=None, interval=None, col=None, return_sdi=True):
    collection = db[name]
    # Build projection
    prj = {'_id': 0}
    if col is not None:
        if return_sdi:
            col = ['skey', 'date', 'interval'] + col
        for col_name in col:
            prj[col_name] = 1

    # Build query
    query = {}
    if skey is not None:
        query['skey'] = {'$in': skey}
    if interval is not None:
        query['interval'] = {'$in': interval}
    if start_date is not None:
        if end_date is not None:
            query['date'] = {'$gte': start_date, '$lte': end_date}
        else:
            query['date'] = {'$gte': start_date}
    elif end_date is not None:
        query['date'] = {'$lte': end_date}

    # Load data
    cur = collection.find(query, prj)
    df = pd.DataFrame.from_records(cur)
    if df.empty:
        df = pd.DataFrame()
    else:
        df = df.sort_values(by=['date', 'skey'])
    return df


database_name = 'com_md_eq_cn'
user = 'zhenyuy'
password = 'bnONBrzSMGoE'

pd.set_option('max_columns', 200)
db1 = DB1("192.168.10.178", database_name, user, password)


def mbdGene(stockData):
    thisDateStr = str(stockData['date'].values[0])
    thisStock = stockData['skey'].values[0]
    stockData['time'] = stockData['time'] / 1000
    stockData['order_price'] = (stockData['order_price'] * 10000).round(0)
    stockData['trade_price'] = (stockData['trade_price'] * 10000).round(0)
    try:
        stockData['isAuction'] = np.where(stockData['time'] < 92900000, True, False)
        stockData = stockData[stockData['time'] < 145655000].reset_index(drop=True)
        hasAuction = True if stockData[stockData['isAuction'] == True].shape[0] > 0 else False
        simMarket = generateMBD(skey=thisStock, date=int(thisDateStr), hasAuction=hasAuction)
        stockDataNP = stockData.to_records()
        for rowEntry in stockDataNP:
            simMarket.Num.append(rowEntry.ApplSeqNum)
            if rowEntry.isAuction:
                if rowEntry.status == 'order':
                    simMarket.insertAuctionOrder(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                 rowEntry.order_side, rowEntry.order_type, rowEntry.order_price,
                                                 rowEntry.order_qty)

                elif rowEntry.status == 'cancel':
                    simMarket.removeOrderByAuctionCancel(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                         rowEntry.trade_qty, rowEntry.BidApplSeqNum,
                                                         rowEntry.OfferApplSeqNum)

                elif rowEntry.status == 'trade':
                    simMarket.removeOrderByAuctionTrade(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                        rowEntry.trade_price, rowEntry.trade_qty,
                                                        rowEntry.BidApplSeqNum, rowEntry.OfferApplSeqNum)
            else:
                if rowEntry.status == 'order':
                    simMarket.insertOrder(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                          rowEntry.order_side,
                                          rowEntry.order_type, rowEntry.order_price, rowEntry.order_qty)

                elif rowEntry.status == 'cancel':
                    simMarket.removeOrderByCancel(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                  rowEntry.trade_qty, rowEntry.BidApplSeqNum, rowEntry.OfferApplSeqNum)

                elif rowEntry.status == 'trade':
                    simMarket.removeOrderByTrade(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                 rowEntry.trade_price, rowEntry.trade_qty, rowEntry.BidApplSeqNum,
                                                 rowEntry.OfferApplSeqNum)

        data = simMarket.getSimMktInfo()
        data = data.rename(columns={"bboImprove": 'bbo_improve', "caa": "clockAtArrival"})

        mdLog = db.read('md_snapshot_l2', start_date=str(data['date'].iloc[0]), end_date=str(data['date'].iloc[0]),
                        symbol=data['skey'].iloc[0])
        assert ((mdLog['prev_close'].iloc[0] > 0) & (mdLog['prev_close'].nunique() == 1))
        assert (mdLog[mdLog['open'] > 0]['open'].nunique() == 1)
        data['prev_close'] = mdLog['prev_close'].iloc[0]
        data['open'] = mdLog[mdLog['cum_volume'] > 0]['open'].iloc[0]
        assert ((data['bbo_improve'].nunique() <= 3) & (1 in data['bbo_improve'].unique()) & (
                    0 in data['bbo_improve'].unique()))

        try:
            sizeData = read_filter_daily(db1, 'md_stock_sizefilter', skey=[int(data['skey'].iloc[0])])
            sizeFilter = sizeData[sizeData['date'] == data['date'].iloc[0]]['size_filter'].values[0]
        except:
            start = db.read('md_snapshot_l2', symbol=data['skey'].iloc[0])['date'].min()
            start = datetime.datetime.strptime(str(start), '%Y%m%d')
            da_te = sizeData['date'].iloc[0]
            da_te = datetime.datetime.strptime(str(da_te), '%Y%m%d')     
            try:
                if start.weekday() + 1 == 1:
                    assert((da_te - start).days == 7)
                elif start.weekday() + 1 == 2:
                    assert((da_te - start).days == 6)
                elif start.weekday() + 1 == 3:
                    assert((da_te - start).days == 5)
                elif start.weekday() + 1 == 4:
                    assert((da_te - start).days == 4)
                elif start.weekday() + 1 == 5:
                    assert((da_te - start).days == 10)
            except:
                print('unexpected sizeFilter data!!!!!!!!!!!!!!!!!!!!!')
                print(data['skey'].iloc[0])
                print(data['date'].iloc[0])
                    
            sizeFilter = 0
        assert (sizeFilter >= 0)

        passFilterLs = []
        passMDFilterLs = []
        passTmLs = []

        openPLs = data['open'].values
        cumVolLs = data['cum_volume'].values
        cumAmtLs = data['cum_amount'].values
        bid1pLs = data['bid1p'].values
        ask1pLs = data['ask1p'].values
        clockLs = data['clockAtArrival'].values
        tmLs = data['time'].values
        bboLs = data['bbo_improve'].values

        maxCumVol, prevCumVol, prevCumAmt, prevBid1p, prevAsk1p, prevClock, prevTm = -1, -1, -1, -1, -1, -1, -1
        for curOpen, curCumVol, curCumAmt, curBid1p, curAsk1p, curClock, curTm, curbbo in zip(openPLs, cumVolLs,
                                                                                              cumAmtLs, bid1pLs,
                                                                                              ask1pLs, clockLs, tmLs,
                                                                                              bboLs):
            maxCumVol = max(maxCumVol, curCumVol)
            if curbbo == 0:
                passFilterLs.append(-1)
            else:
                if curOpen == 0:
                    passMDFilter = False
                    passTm = False
                elif prevTm == -1:
                    passMDFilter = True
                    passTm = False
                elif curCumVol < maxCumVol:
                    passMDFilter = False
                    passTm = False
                else:
                    passMDFilter = (curCumAmt - prevCumAmt > sizeFilter) | \
                                   ((curCumVol >= prevCumVol) & ((curBid1p != prevBid1p) | (curAsk1p != prevAsk1p)))
                    passTm = False
                    if curClock - prevClock > 10 * 1e6 and curCumVol >= prevCumVol and passMDFilter == False and curTm > prevTm:
                        passMDFilter = True
                        passTm = True

                if prevTm == -1 and passMDFilter:
                    passFilterLs.append(2)
                elif passMDFilter or passTm:
                    passFilter = (curBid1p != prevBid1p) | (curAsk1p != prevAsk1p) | (
                                curCumAmt - prevCumAmt > sizeFilter)
                    passFilterLs.append(2) if passFilter else passFilterLs.append(1)
                else:
                    passFilterLs.append(0)

                if passMDFilter or passTm:
                    prevCumVol, prevCumAmt, prevBid1p, prevAsk1p, prevClock, prevTm = \
                        curCumVol, curCumAmt, curBid1p, curAsk1p, curClock, curTm

        data['pass_filter'] = passFilterLs
        data['nearLimit'] = np.where((data['bid5q'] == 0) | (data['ask5q'] == 0), 1, 0)
        data['pass_filter'] = np.where((data['pass_filter'] == 0), 0,
                                       np.where((data['pass_filter'] == 2) & (data['nearLimit'] == 1), 1,
                                                data['pass_filter']))
        data.drop(['nearLimit'], axis=1, inplace=True)
        data['pass_filter'] = data['pass_filter'].astype('int32')
        data['datetime'] = data["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
        data = data.reset_index(drop=True)
        data['ordering'] = data.index + 1
        for cols in ['date', 'ordering', 'ApplSeqNum']:
            data[cols] = data[cols].astype('int32')

        data = data[['skey', 'date', 'time', 'clockAtArrival', 'ordering', 'ApplSeqNum', 'bbo_improve',
                     'pass_filter', 'cum_trades_cnt', 'cum_volume', 'cum_amount',
                     'prev_close', 'open', 'close', 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p',
                     'bid3p', 'bid2p', 'bid1p',
                     'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p',
                     'bid10q', 'bid9q', 'bid8q', 'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q',
                     'ask1q', 'ask2q', 'ask3q', 'ask4q', 'ask5q', 'ask6q', 'ask7q', 'ask8q', 'ask9q', 'ask10q',
                     'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n', 'bid3n', 'bid2n', 'bid1n',
                     'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n',
                     'total_bid_quantity', 'total_ask_quantity', 'total_bid_vwap', 'total_ask_vwap', 'total_bid_orders',
                     'total_ask_orders', 'total_bid_levels', 'total_ask_levels']]
        try:
            db.write('md_snapshot_mbd', data)
            del data
        except:
            db.write('md_snapshot_mbd', data, chunk_size = 5000)
            del data

    except Exception as e:
        print(thisStock)
        print(e)


import multiprocessing as mp

thisDate = datetime.date(2020, 8, 1)
while thisDate <= datetime.date(2020, 10, 31):
    thisDate_str = str(thisDate).replace('-', '')
    db = DB("192.168.10.178", 'com_md_eq_cn', 'zhenyuy', 'bnONBrzSMGoE')


    mdOrderLog = db.read('md_order', start_date=thisDate_str, end_date=thisDate_str, symbol=[2000725])
    if mdOrderLog is None:
        thisDate = thisDate + datetime.timedelta(days=1)
        continue
    print(thisDate)
    mdTradeLog = db.read('md_trade', start_date=thisDate_str, end_date=thisDate_str, symbol=[2000725])

    mdOrderLog['status'] = 'order'
    assert (mdOrderLog['order_type'].nunique() <= 3)

    assert (mdTradeLog['trade_type'].nunique() == 2)
    mdTradeLog['status'] = np.where(mdTradeLog['trade_type'] == 1, 'trade', 'cancel')

    msgData = pd.concat([mdOrderLog, mdTradeLog], sort=False)
    del mdOrderLog
    del mdTradeLog

    msgData = msgData.sort_values(by=['skey', 'ApplSeqNum']).reset_index(drop=True)

    start = time.time()
    mbdGene(msgData)
    print(time.time() - start)

    print('finished ' + thisDate_str)
    thisDate = thisDate + datetime.timedelta(days=1)




