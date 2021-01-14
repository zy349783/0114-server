#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 01:54:46 2020

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
from random import randint
from multiprocessing import Pool
import glob
import os
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['OMP_THREAD_LIMIT'] = '1'


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
        start_time = time.time()
        for x in collection.find(query): 
            x['data'] = self.deser(x['data'], x['ver']) 
            segs.append(x) 
        segs.sort(key=lambda x: (x['symbol'], x['date'], x['start'])) 
        time1 = time.time() - start_time
        start_time = time.time()
        data = pd.concat([x['data'] for x in segs], ignore_index=True) if segs else None
        time2 = time.time() - start_time
        print(str(time1) + ',' + str(time2))
        return time1, time2
    
        
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

import random
random.seed(1)

# for i in range(20):
#   db1.read_tick1('md_trade', 20201211, 20201211, symbol=sl)


def f(x):
    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    pd.set_option('max_columns', 200)
    random.seed(1)
    pathLs = list(np.array(glob.glob('/data/home/zhenyu/mongoDB/md_trade/20201211/***')))
    pathLs = random.sample(pathLs, 500)
    sl = [int(os.path.basename(i).split('.')[0]) for i in pathLs]
    db1 = DB("192.168.10.178", database_name, user, password)
    return db1.read_tick1('md_trade', 20201211, 20201211, symbol=sl)


def f1(x):
    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    pd.set_option('max_columns', 200)
    start_date = 20181122
    end_date = 20201211
    db1 = DB("192.168.10.178", database_name, user, password)
    return db1.read_tick1('md_trade', start_date, end_date, symbol=2000001)
  
def f2(x):
    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    pd.set_option('max_columns', 200)
    start_date = 20201211
    end_date = 20201211
    db1 = DB("192.168.10.178", database_name, user, password)
    return db1.read_tick1('md_trade', start_date, end_date, symbol=2000001)
    
if __name__ == '__main__':
    n = [randint(0,96) for i in range(96)]
    with Pool(96) as p:
        re = p.map(f, n)
        re = pd.DataFrame(re)
        re.columns=['time1', 'time2']
        re['case'] = 'case1'
        re['mode'] = 'HPC load db'
        re['core'] = 96
        re.to_csv('/data/home/zhenyu/result/HPC_db/core_96/case1.csv')   
        
    with Pool(96) as p:
        re = p.map(f1, n)
        re = pd.DataFrame(re)
        re.columns=['time1', 'time2']
        re['case'] = 'case2'
        re['mode'] = 'HPC load db'
        re['core'] = 96
        re.to_csv('/data/home/zhenyu/result/HPC_db/core_96/case2.csv')          
        
    with Pool(96) as p:
        re = p.map(f2, n)
        re = pd.DataFrame(re)
        re.columns=['time1', 'time2']
        re['case'] = 'case3'
        re['mode'] = 'HPC load db'
        re['core'] = 96
        re.to_csv('/data/home/zhenyu/result/HPC_db/core_96/case3.csv')     
# t1 = []
# t2 = []
# case = []
# mode = []
# core = []
# for i in range(20):
#     re = f(i)
#     t1.append(re[0])
#     t2.append(re[1])
#     case.append('case1')
#     mode.append('HPC load db')
#     core.append(1)
# for i in range(20):
#     re = f1(i)
#     t1.append(re[0])
#     t2.append(re[1])
#     case.append('case2')
#     mode.append('HPC load db')
#     core.append(1)
# for i in range(20):
#     re = f2(i)
#     t1.append(re[0])
#     t2.append(re[1])
#     case.append('case3')
#     mode.append('HPC load db')
#     core.append(1)

# df = pd.DataFrame()
# df['time1'] = t1
# df['time2'] = t2
# df['case'] = case
# df['mode'] = mode
# df['core'] = core
# df.to_csv('/data/home/zhenyu/result/HPC_db/core_1/' + '0' + '.csv')