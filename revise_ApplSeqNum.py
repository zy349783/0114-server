#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 00:22:17 2021

@author: zhenyu
"""
import pymongo
import pandas as pd
import pickle
import datetime
import time
import gzip
import lzma
import pytz


import pymongo
import io
import pandas as pd
import pickle
import datetime
import time
import gzip
import lzma
import pytz
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


def DB(host, db_name, user, passwd, version=3):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    uri = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    return DBObj(uri, db_name=db_name, version=version)


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
patch_pandas_pickle()

def dailyDB(host, db_name, user, passwd):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    url = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    client = pymongo.MongoClient(url, maxPoolSize=None)
    db = client[db_name]
    return db

def read_stock_daily(db, name, start_date=None, end_date=None, skey=None, index_name=None, interval=None, col=None, return_sdi=True):
    collection = db[name]
    # Build projection
    prj = {'_id': 0}
    if col is not None:
        if return_sdi:
            col = ['skey', 'date'] + col
        for col_name in col:
            prj[col_name] = 1

    # Build query
    query = {}
    if skey is not None:
        query['skey'] = {'$in': skey}
    if index_name is not None:
        query['index_name'] = {'$in': index_name}
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

def read_memb_daily(db, name, start_date=None, end_date=None, skey=None, index_id=None, interval=None, col=None, return_sdi=True):
    collection = db[name]
    # Build projection
    prj = {'_id': 0}
    if col is not None:
        if return_sdi:
            col = ['skey', 'date', 'index_id'] + col
        for col_name in col:
            prj[col_name] = 1

    # Build query
    query = {}
    if skey is not None:
        query['skey'] = {'$in': skey}
    if index_id is not None:
        query['index_id'] = {'$in': index_id}
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
        df = df.sort_values(by=['date', 'index_id', 'skey'])
    return df 



import pandas as pd
import random
import numpy as np
import glob
import pickle
import os
import datetime
import time
pd.set_option("max_columns", 200)

year = "2020"
startDate = '20200824'
endDate = '20200828'
database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

startTm = datetime.datetime.now()
db1 = DB("192.168.10.178", database_name, user, password)
db2 = dailyDB("192.168.10.178", database_name, user, password)
save = {}
save['date'] = []
save['secid'] = []
mdOrderLog = db1.read('md_order', start_date=startDate, end_date=endDate, symbol=[2000001])
datelist = mdOrderLog['date'].unique()
ss = pd.read_csv('/data/home/zhenyu/ShareWithServer/result/shangshi1.csv')
ss = ss[ss['证券代码↑'] != 'T00018.SH']
ss['skey'] = np.where(ss['证券代码↑'].str[-2:] == 'SZ', ss['证券代码↑'].str[:6].astype(int) + 2000000, ss['证券代码↑'].str[:6].astype(int) + 1000000)
ss['date'] = (ss['上市日期'].str[:4] + ss['上市日期'].str[5:7] + ss['上市日期'].str[8:10]).astype(int)
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
for d in datelist:
    print(d)
    sl1 = db1.read('md_order', start_date=str(d), end_date=str(d))['skey'].unique()
    data1 = db1.read('md_snapshot_l2', start_date=str(d), end_date=str(d), symbol=list(sl1))
    assert(len(sl1) == data1['skey'].nunique())
    sl2 = ss[ss['date'] == d]['skey'].unique()
    sl1 = list(set(sl1) - set(sl2))
    data1 = data1[data1['skey'].isin(sl1)]
    sl1 = data1[(data1['cum_volume'] > 0) & (data1['time'] <= 145655000000) & ((data1['ApplSeqNum'].isnull()) | \
                (data1['ApplSeqNum'] == -1))]['skey'].unique()
    print(sl1)
    op = read_stock_daily(db2, 'mdbar1d_tr', start_date=int(d), end_date=int(d))
    
    if len(sl1) != 0:
        for s in sl1:
            mbd = db1.read('md_snapshot_mbd', start_date=str(d), end_date=str(d), symbol=s)
            if mbd is None:
                if ss[ss['skey'] == s]['date'].iloc[0] == d:
                    l2 = data1[data1['skey'] == s]
                    l2['ApplSeqNum'] = -1
                    l2['ApplSeqNum'] = l2['ApplSeqNum'].astype('int32') 
                    db1.write('md_snapshot_l2', l2)
                    continue
                else:
                    save['date'].append(d)
                    save['secid'].append(s)
                    print(s)
                    continue
            try:
                assert(mbd.shape[1] == 82)
            except:
                print('mdb data column unupdated')
                print(s)
            try:
                op1 = op[op['skey'] == s]['open'].iloc[0]
                assert(mbd[mbd['cum_volume'] > 0]['open'].iloc[0] == op1)
            except:
                print('%s have no information in mdbar1d_tr' % str(s))
            l2 = data1[data1['skey'] == s]
            cols = ['skey', 'date', 'cum_volume', 'prev_close', 'open', 'close', 'cum_trades_cnt', 'bid10p', 'bid9p',
                    'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p', 'bid3p', 'bid2p', 'bid1p', 'ask1p', 'ask2p',
                    'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 'bid10q', 'bid9q', 
                    'bid8q', 'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 
                    'ask4q', 'ask5q', 'ask6q','ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n',
                    'bid7n', 'bid6n', 'bid5n', 'bid4n', 'bid3n', 'bid2n', 'bid1n', 'ask1n', 'ask2n', 'ask3n', 
                    'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 'total_bid_quantity', 'total_ask_quantity']
            mbd1 = mbd.drop_duplicates(cols, keep='first')
            mbd = mbd1[cols+['ApplSeqNum', 'time']]
            if 'ApplSeqNum' in l2.columns:
                l2 = l2[list(l2.columns[l2.columns != 'ApplSeqNum'])]
            rl2 = pd.merge(l2, mbd[cols + ['ApplSeqNum']], on=cols, how='left')
            try:
                assert(rl2[(rl2['ApplSeqNum'].isnull()) & (rl2['cum_volume'] > 0) & (rl2['time'] <= 145655000000)].shape[0] == 0)
            except:
                print(rl2[(rl2['ApplSeqNum'].isnull()) & (rl2['cum_volume'] > 0) & (rl2['time'] <= 145655000000)][['skey', 'date', 'time', 'cum_volume', 'close', 'bid1p', 'bid2p','bid1q', 'bid2q', 'ask1p', 'ask2p', 'ask1q', 'ask2q']])
                print(mbd1.tail(1)[['skey', 'date', 'time', 'cum_volume', 'close', 'bid1p', 'bid2p','bid1q', 'bid2q', 'ask1p', 'ask2p', 'ask1q', 'ask2q']])
            rl2.loc[rl2['ApplSeqNum'].isnull(), 'ApplSeqNum'] = -1
            rl2['ApplSeqNum'] = rl2['ApplSeqNum'].astype('int32') 
            assert(rl2.shape[0] == l2.shape[0])
            db1.write('md_snapshot_l2', rl2)
        print(datetime.datetime.now() - startTm)
    else:
        continue