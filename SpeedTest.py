#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 20:28:19 2020

@author: zhenyu
"""

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

    def build_query(self, start_date=None, end_date=None, symbol=None):
        query = {}
        def parse_date(x):
            if type(x) == str:
                if len(x) != 8:
                    raise Exception("date must be YYYYMMDD format")
                return x
            elif type(x) == datetime.datetime or type(x) == datetime.date:
                return x.strftime("%Y%m%d")
            elif type(x) == int:
                return parse_date(str(x))
            else:
                raise Exception("invalid date type: " + str(type(x)))
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

    def read_tick(self, table_name, start_date=None, end_date=None, symbol=None):
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

    def read_tick1(self, table_name, start_date=None, end_date=None, symbol=None):
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
        return pd.DataFrame(np.concatenate([x['data'].values for x in segs], axis=0), columns=x['data'].columns).astype(x['data'].dtypes.to_dict())

    def read_tick2(self, table_name, start_date=None, end_date=None, symbol=None):
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
        return pd.DataFrame(np.concatenate([x['data'].values for x in segs], axis=0), columns=x['data'].columns)
    
    def read_daily(self, table_name, start_date=None, end_date=None, skey=None, index_id=None, interval=None, index_name=None, col=None, return_sdi=True): 
        collection = self.db[table_name]
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
        if interval is not None: 
            query['interval'] = {'$in': interval} 
        if index_id is not None: 
            query['index_id'] = {'$in': index_id}    
        if index_name is not None:
            n = '' 
            for name in index_name: 
                try: 
                    name = re.compile('[\u4e00-\u9fff]+').findall(name)[0] 
                    if len(n) == 0: 
                        n = n = "|".join(name) 
                    else: 
                        n = n + '|' + "|".join(name) 
                except: 
                    if len(n) == 0: 
                        n = name 
                    else: 
                        n = n + '|' + name 
            query['index_name'] = {'$regex': n}
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
            if 'index_id' in df.columns:
                df = df.sort_values(by=['date', 'index_id', 'skey']).reset_index(drop=True)
            else:
                df = df.sort_values(by=['date','skey']).reset_index(drop=True)
        return df 
 
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



database_name = 'com_md_eq_cn'
user = 'zhenyuy'
password = 'bnONBrzSMGoE'

import sys


pd.set_option('max_columns', 200)
db1 = DB("192.168.10.178", database_name, user, password)

import timeit

tag = []
speed = []
ram = []


def func():
    test = db1.read_tick('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)

def func1():
    test = db1.read_tick1('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)

def func2():
    test = db1.read_tick2('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)

tag.append('remote SH server, read database (pd.concat)')
speed.append(np.mean(timeit.repeat("func()", setup="from __main__ import func",repeat=20,number=1)))
test = db1.read_tick('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)
ram.append(sys.getsizeof(test)/1024)

tag.append('remote SH server, read database (np.concatenate + change dtypes)')
speed.append(np.mean(timeit.repeat("func1()", setup="from __main__ import func1",repeat=20,number=1)))
test = db1.read_tick1('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)
ram.append(sys.getsizeof(test)/1024)

tag.append('remote SH server, read database (np.concatenate)')
speed.append(np.mean(timeit.repeat("func2()", setup="from __main__ import func2",repeat=20,number=1)))
test = db1.read_tick2('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)
ram.append(sys.getsizeof(test)/1024)




def func():
    dataPathLs = np.array(glob.glob('/data/home/zhenyu/DBdata/per_day/***'))
    re = []
    for d in dataPathLs:
        test = pd.read_pickle(d)
        re += [test]
    re = pd.concat(re).reset_index(drop=True)

def func1():
    dataPathLs = np.array(glob.glob('/data/home/zhenyu/DBdata/per_month/***'))
    re = []
    for d in dataPathLs:
        test = pd.read_pickle(d)
        re += [test]
    re = pd.concat(re).reset_index(drop=True)

import os
import glob

tag.append('remote SH server,, read from server (per day)')
speed.append(np.mean(timeit.repeat("func()", setup="from __main__ import func",repeat=20,number=1)))
dataPathLs = np.array(glob.glob('/data/home/zhenyu/DBdata/per_day/***'))
size = 0
for d in dataPathLs:
    size += os.path.getsize(d)
ram.append(size/1024)

tag.append('remote SH server,, read from server (per month)')
speed.append(np.mean(timeit.repeat("func1()", setup="from __main__ import func1",repeat=20,number=1)))
test = db1.read_tick1('md_snapshot_l2', start_date=20190102, end_date=20191231, symbol=2000006)
dataPathLs = np.array(glob.glob('/data/home/zhenyu/DBdata/per_month/***'))
size = 0
for d in dataPathLs:
    size += os.path.getsize(d)
ram.append(size/1024)