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
                    s[col_name] = s[col_name].astype(np.int64)
                elif col.dtype == np.uint32:
                    s[col_name] = s[col_name].astype(np.uint64)
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


class go():
    def __init__(self):
        self.name = 'kkk'

    def RW(self, s):
        database_name = 'com_md_eq_cn'
        user = 'zhenyuy'
        password = 'bnONBrzSMGoE'
        db = DB("192.168.10.178", database_name, user, password)
        mdTradeLog = db.read('md_trade', start_date=str(s), end_date=str(s))
        mdOrderLog = db.read('md_order', start_date=str(s), end_date=str(s))
        if 'datetime' in mdTradeLog.columns:
            list1 = list(mdTradeLog.columns)
            list1.remove('datetime')
            mdTradeLog = mdTradeLog[list1]
        db.write('md_trade', mdTradeLog)
        del mdTradeLog
        if 'datetime' in mdOrderLog.columns:
            list1 = list(mdOrderLog.columns)
            list1.remove('datetime')
            mdOrderLog = mdOrderLog[list1]
        db.write('md_order', mdOrderLog)   
        del mdOrderLog
        print(str(s))
        
    def RW1(self, s):
        database_name = 'com_md_eq_cn'
        user = 'zhenyuy'
        password = 'bnONBrzSMGoE'
        db = DB("192.168.10.178", database_name, user, password)
        mdLV2 = db.read('md_snapshot_l2', start_date=str(s), end_date=str(s))
        if 'datetime' in mdLV2.columns:
            list1 = list(mdLV2.columns)
            list1.remove('datetime')
            mdLV2 = mdLV2[list1]
        db.write('md_snapshot_l2', mdLV2)
        del mdLV2
        print(str(s))
         
    def RW2(self, s):
        database_name = 'com_md_eq_cn'
        user = 'zhenyuy'
        password = 'bnONBrzSMGoE'
        db = DB("192.168.10.178", database_name, user, password)
        mdIndex = db.read('md_index', start_date=str(s), end_date=str(s))
        if 'datetime' in mdIndex.columns:
            list1 = list(mdIndex.columns)
            list1.remove('datetime')
            mdIndex = mdIndex[list1]
        db.write('md_index', mdIndex)
        del mdIndex
        print(str(s))
        
if __name__ == '__main__':
    import multiprocessing as mp
    import time


    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    db = DB("192.168.10.178", database_name, user, password)

    # dateList = np.sort(db.read('md_snapshot_l2', 20170103, 20201211, symbol=1600000)['date'].unique())
    dateList = [20180530]
    m = len(dateList)
    li_st = np.arange(0, m, 1)
    for i in range(len(li_st)):
        start = time.time()
        m1 = li_st[i]
        if i != len(li_st) - 1:
            m2 = li_st[i + 1]
        else:
            m2 = m
        start = time.time()
        g = go()
        pool = mp.Pool(processes=1)
        pool.map(g.RW, dateList[m1: m2])
        pool.close()
        pool.join()
        print(time.time() - start)
        
    for i in range(len(li_st)):
        start = time.time()
        m1 = li_st[i]
        if i != len(li_st) - 1:
            m2 = li_st[i + 1]
        else:
            m2 = m
        start = time.time()
        g = go()
        pool = mp.Pool(processes=1)
        pool.map(g.RW1, dateList[m1: m2])
        pool.close()
        pool.join()
        print(time.time() - start)

        
    for i in range(len(li_st)):
        start = time.time()
        m1 = li_st[i]
        if i != len(li_st) - 1:
            m2 = li_st[i + 1]
        else:
            m2 = m
        start = time.time()
        g = go()
        pool = mp.Pool(processes=1)
        pool.map(g.RW2, dateList[m1: m2])
        pool.close()
        pool.join()
        print(time.time() - start)        
        # li_st = np.arange(0, m, 70)
        # for i in range(len(li_st)):
        #     start = time.time()
        #     orders_data = {}
        #     trades_data = {}
        #     m1 = li_st[i]
        #     if i != len(li_st) - 1:
        #         m2 = li_st[i + 1]
        #     else:
        #         m2 = m
        #     for s in sl1[m1: m2]:
        #         mdOrderLog1 = mdOrderLog[mdOrderLog['skey'] == s]
        #         mdTradeLog1 = mdTradeLog[mdTradeLog['skey'] == s]
        #         if 'pandas' in str(type(mdOrderLog1)):
        #             orders_data[s] = mdOrderLog1
        #             del mdOrderLog1
        #         if 'pandas' in str(type(mdTradeLog1)):
        #             trades_data[s] = mdTradeLog1
        #             del mdTradeLog1
        #     g = go(orders_data, trades_data)
        #     del orders_data
        #     del trades_data
        #     pool = mp.Pool(processes=70)
        #     pool.map(g.RW1, sl1[m1: m2])
        #     pool.close()
        #     pool.join()
        #     print(time.time() - start)

        # m = len(sl2)
        
        # li_st = np.arange(0, m, 70)
        # for i in range(len(li_st)):
        #     start = time.time()
        #     orders_data = {}
        #     trades_data = {}
        #     m1 = li_st[i]
        #     if i != len(li_st) - 1:
        #         m2 = li_st[i + 1]
        #     else:
        #         m2 = m
        #     for s in sl2[m1: m2]:
        #         mdTradeLog1 = mdTradeLog[mdTradeLog['skey'] == s]
        #         if 'pandas' in str(type(mdTradeLog1)):
        #             trades_data[s] = mdTradeLog1
        #             del mdTradeLog1
        #     g = go(orders_data, trades_data)
        #     del trades_data
        #     pool = mp.Pool(processes=70)
        #     pool.map(g.RW2, sl2[m1: m2])
        #     pool.close()
        #     pool.join()
        #     print(time.time() - start)      
   
        # print(thisDate)

# dl = []
# for i in range(0, 10):
#     start = time.time()
#     database_name = 'com_md_eq_cn'
#     user = 'zhenyuy'
#     password = 'bnONBrzSMGoE'
#     db = DB("192.168.10.178", database_name, user, password)
#     data = db.read('md_trade', start_date=20201207, end_date=20201207)
#     dl.append(time.time() - start)
# print(dl)
# print(np.mean(dl))

# dl = []
# for i in range(0, 10):
#     start = time.time()
#     database_name = 'com_md_eq_cn'
#     user = 'zhenyuy'
#     password = 'bnONBrzSMGoE'
#     db = DB("192.168.10.178", database_name, user, password)
#     data = db.read('md_order', start_date=20201207, end_date=20201207)
#     dl.append(time.time() - start)
# print(dl)
# print(np.mean(dl))
