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

    def write(self, table_name, df, chunk_size=20000):
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
                self.write_single(collection, date, symbol, sub_df, chunk_size)
        else:
            for symbol, sub_df in df.groupby([self.symbol_column]):
                collection.delete_many({'date': date, 'symbol': symbol})
                self.write_single(collection, date, symbol, sub_df, chunk_size)

    def write_single(self, collection, date, symbol, df, chunk_size):
        for start in range(0, len(df), chunk_size):
            end = min(start + chunk_size, len(df))
            df_seg = df[start:end]
            version = self.version
            seg = {'ver': version, 'data': self.ser(df_seg, version), 'date': date, 'symbol': symbol, 'start': start}
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
        df = df.sort_values(by=['date','skey'])
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



database_name = 'com_md_eq_cn'
user = 'zhenyuy'
password = 'bnONBrzSMGoE'

pd.set_option('max_columns', 200)
db1 = DB1("192.168.10.178", database_name, user, password)


class go():
    def __init__(self, thisDate_str, orders_data, trades_data, prev_data, open_data):
        self.orders_data = orders_data
        self.trades_data = trades_data
        self.thisDate_str = thisDate_str
        self.prev_data = prev_data
        self.open_data = open_data

    def run(self, s):
        mdTradeLog = self.trades_data[s]
        mdOrderLog = self.orders_data[s]
        prev = self.prev_data[s]
        op = self.open_data[s]

        ###
        mdOrderLog['ID'] = int(mdOrderLog['skey'].dropna().unique())
        mdOrderLog['order_type'] = mdOrderLog['order_type'].astype(str)
        mdOrderLog['status'] = 'order'
        ## rename
        mdOrderLog.columns = ['skey', 'date', 'TransactTime', 'clockAtArrival', 'ApplSeqNum',
                              'Side', 'OrderType', 'Price', 'OrderQty', 'SecurityID', 'status']
        mdTradeLog['ID'] = int(mdTradeLog['skey'].dropna().unique())
        mdTradeLog['trade_type'] = mdTradeLog['trade_type'].astype(str)
        if 'trade_money' not in mdTradeLog.columns:
            mdTradeLog.columns = ['skey', 'date', 'TransactTime', 'clockAtArrival', 'ApplSeqNum',
                                  'ExecType', 'trade_flag', 'TradePrice', 'TradeQty', 'BidApplSeqNum',
                                  'OfferApplSeqNum', 'SecurityID']
        else:
            mdTradeLog.columns = ['skey', 'date', 'TransactTime', 'clockAtArrival', 'ApplSeqNum',
                                  'ExecType', 'trade_flag', 'TradePrice', 'TradeQty', 'BidApplSeqNum',
                                  'OfferApplSeqNum', 'SecurityID', 'trade_money']
            ###
        tradedLog = mdTradeLog[mdTradeLog['ExecType'] == '1'].reset_index(drop=True)
        tradedLog['status'] = 'trade'
        #
        bidOrderInfo = mdOrderLog[['ApplSeqNum', 'SecurityID', 'Price', 'OrderType', 'Side']].reset_index(drop=True)
        bidOrderInfo = bidOrderInfo.rename(
            columns={'TransactTime': 'TransactTime', 'ApplSeqNum': 'BidApplSeqNum', 'Price': 'BidOrderPrice',
                     'OrderType': 'BidOrderType', 'Side': 'BidSide'})
        tradedLog = pd.merge(tradedLog, bidOrderInfo, how='left', on=['SecurityID', 'BidApplSeqNum'],
                             validate='many_to_one')
        del bidOrderInfo

        askOrderInfo = mdOrderLog[['ApplSeqNum', 'SecurityID', 'Price', 'OrderType', 'Side']].reset_index(drop=True)
        askOrderInfo = askOrderInfo.rename(
            columns={'TransactTime': 'TransactTime', 'ApplSeqNum': 'OfferApplSeqNum', 'Price': 'OfferOrderPrice',
                     'OrderType': 'OfferOrderType', 'Side': 'OfferSide'})
        tradedLog = pd.merge(tradedLog, askOrderInfo, how='left', on=['SecurityID', 'OfferApplSeqNum'],
                             validate='many_to_one')
        del askOrderInfo

        cancelLog = mdTradeLog[mdTradeLog['ExecType'] == '4'].reset_index(drop=True)
        cancelLog['status'] = 'cancel'
        cancelLog['CancelApplSeqNum'] = cancelLog['BidApplSeqNum']
        mask = cancelLog['CancelApplSeqNum'] == 0
        cancelLog.loc[mask, 'CancelApplSeqNum'] = cancelLog.loc[mask, 'OfferApplSeqNum'].values
        del mask
        assert (cancelLog[cancelLog['CancelApplSeqNum'] == 0].shape[0] == 0)
        cancelLog = cancelLog.drop(columns=['TradePrice'])

        cancelPrice = mdOrderLog[['ApplSeqNum', 'SecurityID', 'Price', 'OrderType', 'Side']].reset_index(drop=True)
        cancelPrice = cancelPrice.rename(columns={'ApplSeqNum': 'CancelApplSeqNum', 'Price': 'TradePrice',
                                                  'OrderType': 'CancelOrderType', 'Side': 'CancelSide'})
        cancelLog = pd.merge(cancelLog, cancelPrice, how='left', on=['SecurityID', 'CancelApplSeqNum'],
                             validate='one_to_one')
        del cancelPrice

        msgData = pd.concat([mdOrderLog[['clockAtArrival', 'TransactTime', 'ApplSeqNum', 'SecurityID',
                                         'status', 'Side', 'OrderType', 'Price', 'OrderQty']],
                             tradedLog[['clockAtArrival', 'TransactTime', 'ApplSeqNum', 'SecurityID',
                                        'status', 'ExecType', 'TradePrice', 'TradeQty', 'BidApplSeqNum',
                                        'OfferApplSeqNum', 'BidOrderType', 'BidSide', 'OfferOrderType', 'OfferSide',
                                        'BidOrderPrice', 'OfferOrderPrice']]], sort=False)
        msgData = pd.concat([msgData, cancelLog[['clockAtArrival', 'TransactTime', 'ApplSeqNum',
                                                 'SecurityID', 'status', 'ExecType', 'TradePrice', 'TradeQty',
                                                 'CancelApplSeqNum',
                                                 'CancelOrderType', 'CancelSide']]], sort=False)
        del tradedLog
        del cancelLog
        msgData = msgData.sort_values(by=['ApplSeqNum']).reset_index(drop=True)
        for stockID, stockMsg in msgData.groupby(['SecurityID']):
            stockMsg = stockMsg.reset_index(drop=True)
            stockMsg['TransactTime'] = stockMsg['TransactTime'] / 1000
            stockMsg['isAuction'] = np.where(stockMsg['TransactTime'] < 92900000, True, False)
            stockMsg = stockMsg[stockMsg['TransactTime'] < 145655000].reset_index(drop=True)
            stockMsgNP = stockMsg.to_records()
            simMarket = SimMktSnapshotAllNew(exchange='SZ', stockID=stockID, levels=10)
        #             self.simMarket = simMarket
        try:
            for rowEntry in stockMsgNP:
                simMarket.ApplSeqNumLs.append(rowEntry.ApplSeqNum)
                if rowEntry.isAuction:
                    if rowEntry.status == 'order':
                        simMarket.insertAuctionOrder(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                     rowEntry.ApplSeqNum, rowEntry.Side, rowEntry.Price,
                                                     rowEntry.OrderQty)
                    elif rowEntry.status == 'cancel':
                        simMarket.removeOrderByAuctionCancel(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                             rowEntry.ApplSeqNum, rowEntry.TradePrice,
                                                             rowEntry.TradeQty,
                                                             rowEntry.CancelApplSeqNum, rowEntry.CancelOrderType,
                                                             rowEntry.CancelSide)
                    elif rowEntry.status == 'trade':
                        simMarket.removeOrderByAuctionTrade(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                            rowEntry.ApplSeqNum, rowEntry.TradePrice, rowEntry.TradeQty,
                                                            rowEntry.BidOrderPrice, rowEntry.OfferOrderPrice)
                else:
                    if rowEntry.status == 'order':
                        simMarket.insertOrder(rowEntry.clockAtArrival, rowEntry.TransactTime, rowEntry.ApplSeqNum,
                                              rowEntry.Side, rowEntry.OrderType, rowEntry.Price, rowEntry.OrderQty,
                                              rowEntry.ApplSeqNum)
                    elif rowEntry.status == 'cancel':
                        simMarket.removeOrderByCancel(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                      rowEntry.ApplSeqNum, rowEntry.TradePrice, rowEntry.TradeQty,
                                                      rowEntry.CancelApplSeqNum, rowEntry.CancelOrderType,
                                                      rowEntry.CancelSide)
                    elif rowEntry.status == 'trade':
                        simMarket.removeOrderByTrade(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                     rowEntry.ApplSeqNum, rowEntry.TradePrice, rowEntry.TradeQty,
                                                     rowEntry.BidApplSeqNum,
                                                     rowEntry.OfferApplSeqNum)

            database_name = 'com_md_eq_cn'
            user = "zhenyuy"
            password = "bnONBrzSMGoE"
            db = DB("192.168.10.178", database_name, user, password)
            data = simMarket.getAllInfo()
            data = data.rename(columns={'StockID': "skey"})
            data = data.rename(columns={'sequenceNo': "ApplSeqNum"})
            data['date'] = int(self.thisDate_str)
            data['prev_close'] = prev
            data['open'] = 0
            data.loc[data['cum_volume'] > 0, 'open'] = op
            data = data.reset_index(drop=True)
            data['ordering'] = data.index + 1
            for cols in ['bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p', 'bid3p',
                         'bid2p', 'bid1p', 'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p',
                         'ask9p', 'ask10p']:
                data[cols] = data[cols].astype(float)
            for cols in ['ApplSeqNum', 'date', 'ordering']:
                data[cols] = data[cols].astype('int32')


            assert((data['bbo_improve'].nunique() <= 3) & (1 in data['bbo_improve'].unique()) & (0 in data['bbo_improve'].unique()))
            
            sizeData = read_filter_daily(db1, 'md_stock_sizefilter', skey=[int(data['skey'].iloc[0])])
            sizeFilter = sizeData[sizeData['date'] == data['date'].iloc[0]]['size_filter'].values[0]
            assert(sizeFilter >= 0)
                
            passFilterLs = []
            passMDFilterLs = []
            passTmLs = []
            
            openPLs = data['open'].values
            cumVolLs = data['cum_volume'].values
            cumAmtLs = data['cum_amount'].values
            bid1pLs = data['bid1p'].values
            ask1pLs = data['ask1p'].values
            clockLs=  data['clockAtArrival'].values
            tmLs = data['time'].values
            bboLs = data['bbo_improve'].values
            
            
            maxCumVol, prevCumVol, prevCumAmt, prevBid1p, prevAsk1p, prevClock, prevTm = -1, -1, -1, -1, -1, -1, -1
            for curOpen, curCumVol, curCumAmt, curBid1p, curAsk1p, curClock, curTm, curbbo in zip(openPLs, cumVolLs, cumAmtLs, bid1pLs, ask1pLs, clockLs, tmLs, bboLs):
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
                        passMDFilter = (curCumAmt - prevCumAmt > sizeFilter) |\
                                        ((curCumVol >= prevCumVol) & ((curBid1p != prevBid1p) | (curAsk1p != prevAsk1p)))
                        passTm = False
                        if curClock - prevClock > 10*1e6 and curCumVol >= prevCumVol and passMDFilter == False and curTm > prevTm:
                            passMDFilter = True
                            passTm = True
    
                    if prevTm == -1 and passMDFilter:
                        passFilterLs.append(2)
                    elif passMDFilter or passTm:
                        passFilter = (curBid1p != prevBid1p) | (curAsk1p != prevAsk1p) | (curCumAmt - prevCumAmt > sizeFilter)
                        passFilterLs.append(2) if passFilter else passFilterLs.append(1)
                    else:
                        passFilterLs.append(0)
                        
                    if passMDFilter or passTm:
                        prevCumVol, prevCumAmt, prevBid1p, prevAsk1p, prevClock, prevTm =\
                        curCumVol, curCumAmt, curBid1p, curAsk1p, curClock, curTm
                                

            data['pass_filter'] = passFilterLs
            data['nearLimit'] = np.where((data['bid5q'] == 0) | (data['ask5q'] == 0), 1, 0)
            data['pass_filter'] = np.where((data['pass_filter'] == 0), 0,
                                      np.where((data['pass_filter'] == 2)&(data['nearLimit'] == 1), 1, data['pass_filter']))
            data.drop(['nearLimit'], axis=1, inplace=True)
            data['pass_filter'] = data['pass_filter'].astype('int32')  
                     
                   
            data = data[['skey', 'date', 'time', 'clockAtArrival', 'ordering', 'ApplSeqNum', 'bbo_improve', 'pass_filter', 'cum_trades_cnt', 'cum_volume', 'cum_amount', 
                         'prev_close', 'open', 'close','bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p', 'bid3p', 'bid2p', 'bid1p', 
                         'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 
                         'bid10q', 'bid9q', 'bid8q', 'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 
                         'ask1q', 'ask2q', 'ask3q', 'ask4q', 'ask5q', 'ask6q', 'ask7q', 'ask8q', 'ask9q', 'ask10q', 
                         'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n', 'bid3n', 'bid2n', 'bid1n', 
                         'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 
                         'total_bid_quantity', 'total_ask_quantity', 'total_bid_vwap', 'total_ask_vwap', 'total_bid_orders', 'total_ask_orders', 'total_bid_levels','total_ask_levels']]

            try:
                # data.to_pickle('/mnt/ShareWithServer/2002192.pkl')
                db.write('md_snapshot_mbd', data)
                del data
            except:
                db.write('md_snapshot_mbd', data, chunk_size = 5000)
                del data

        except Exception as e:
            print(s)
            print(e)


class SimMktSnapshotAllNew():

    def __init__(self, exchange, stockID, levels):

        self.errors = []
        self.exchange = exchange
        self.stockID = stockID
        self.levels = levels
        self.topK = 50

        self.bid = {}
        self.ask = {}
        self.allBidp = []
        self.allAskp = []
        self.bidp = []
        self.bidq = []
        self.askp = []
        self.askq = []
        self.bidn = []
        self.askn = []
        self.uOrder = {}
        self.takingOrder = {}
        self.tempOrder = {}
        self.hasTempOrder = False
        self.isAuction = True

        self.cur_cum_volume = 0
        self.cur_cum_amount = 0
        self.cur_close = 0
        self.bid1p = 0
        self.ask1p = 0
        self.trades = 0
        self.cum_volume = []
        self.cum_amount = []
        self.cum_trades_cnt = []
        self.close = []
        self.localTime = []
        self.exchangeTime = []
        self.sequenceNum = []
        self.bboImprove = []
        self.ApplSeqNumLs = []

        self.total_bid_qty = []
        self.total_bid_vwap = []
        self.total_bid_levels = []
        self.total_bid_orders_num = []
        self.total_ask_qty = []
        self.total_ask_vwap = []
        self.total_ask_levels = []
        self.total_ask_orders_num = []

        self.bidnq = defaultdict(OrderedDict)
        self.asknq = defaultdict(OrderedDict)

        self.bid_qty = 0
        self.ask_qty = 0
        self.bid_amount = 0
        self.ask_amount = 0
        self.bid_price_levels = 0
        self.ask_price_levels = 0
        self.bid_order_nums = 0
        self.ask_order_nums = 0


    def insertAuctionOrder(self, clockAtArrival, exchangeTime, seqNum, side, price, qty):

        if side == 1:
            if price in self.bid:
                self.bid[price] += qty
            else:
                self.bid[price] = qty
                ##**##
                self.bid_price_levels += 1
                ##**##
            ######
            self.bidnq[price][seqNum] = np.int32(qty)
            ######
            ##**##
            self.bid_qty += qty
            self.bid_amount += qty * price
            self.bid_order_nums += 1
            ##**##
        elif side == 2:
            if price in self.ask:
                self.ask[price] += qty
            else:
                self.ask[price] = qty
                ##**##
                self.ask_price_levels += 1
                ##**##
            ######
            self.asknq[price][seqNum] = np.int32(qty)
            ######
            ##**##
            self.ask_qty += qty
            self.ask_amount += qty * price
            self.ask_order_nums += 1
            ##**##
        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)

    def removeOrderByAuctionTrade(self, clockAtArrival, exchangeTime, seqNum,
                                  price, qty, bidOrderPrice, offerOrderPrice):
        if bidOrderPrice in self.bid:
            bidRemain = self.bid[bidOrderPrice] - qty
            if bidRemain == 0:
                self.bid.pop(bidOrderPrice)
                ##**##
                self.bid_price_levels -= 1
                ##**##
            elif bidRemain > 0:
                self.bid[bidOrderPrice] = bidRemain
            ######
            cum_vol = 0
            for seqNo in self.bidnq[bidOrderPrice]:
                cum_vol += self.bidnq[bidOrderPrice][seqNo]
                if cum_vol > qty:
                    ##**##
                    useful_qty = (self.bidnq[bidOrderPrice][seqNo] - (cum_vol - qty))
                    ##**##
                    self.bidnq[bidOrderPrice][seqNo] = np.int32(cum_vol - qty)
                    ##**##
                    self.bid_qty -= useful_qty
                    self.bid_amount -= useful_qty * bidOrderPrice
                    ##**##
                    break
                elif cum_vol == qty:
                    ##**##
                    useful_qty = self.bidnq[bidOrderPrice][seqNo]
                    ##**##
                    self.bidnq[bidOrderPrice].pop(seqNo)
                    ##**##
                    self.bid_qty -= useful_qty
                    self.bid_amount -= useful_qty * bidOrderPrice
                    self.bid_order_nums -= 1
                    ##**##
                    break
                else:
                    ##**##
                    useful_qty = self.bidnq[bidOrderPrice][seqNo]
                    ##**##
                    self.bidnq[bidOrderPrice].pop(seqNo)
                    ##**##
                    self.bid_qty -= useful_qty
                    self.bid_amount -= useful_qty * bidOrderPrice
                    self.bid_order_nums -= 1
                    ##**##
            ######
        else:
            print('bid price not in bid')

        if offerOrderPrice in self.ask:
            askRemain = self.ask[offerOrderPrice] - qty
            if askRemain == 0:
                self.ask.pop(offerOrderPrice)
                ##**##
                self.ask_price_levels -= 1
                ##**##
            elif askRemain > 0:
                self.ask[offerOrderPrice] = askRemain
            ######
            cum_vol = 0
            for seqNo in self.asknq[offerOrderPrice]:
                cum_vol += self.asknq[offerOrderPrice][seqNo]
                if cum_vol > qty:
                    ##**##
                    useful_qty = (self.asknq[offerOrderPrice][seqNo] - (cum_vol - qty))
                    ##**##
                    self.asknq[offerOrderPrice][seqNo] = np.int32(cum_vol - qty)
                    ##**##
                    self.ask_qty -= useful_qty
                    self.ask_amount -= useful_qty * offerOrderPrice
                    ##**##
                    break
                elif cum_vol == qty:
                    ##**##
                    useful_qty = self.asknq[offerOrderPrice][seqNo]
                    ##**##
                    self.asknq[offerOrderPrice].pop(seqNo)
                    ##**##
                    self.ask_qty -= useful_qty
                    self.ask_amount -= useful_qty * offerOrderPrice
                    self.ask_order_nums -= 1
                    ##**##
                    break
                else:
                    ##**##
                    useful_qty = self.asknq[offerOrderPrice][seqNo]
                    ##**##
                    self.asknq[offerOrderPrice].pop(seqNo)
                    ##**##
                    self.ask_qty -= useful_qty
                    self.ask_amount -= useful_qty * offerOrderPrice
                    self.ask_order_nums -= 1
                    ##**##
            ######
        else:
            print('ask price not in ask')

        self.cur_cum_volume += qty
        self.cur_cum_amount += price * qty
        self.cur_close = price
        self.trades += 1

        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)

    def removeOrderByAuctionCancel(self, clockAtArrival, exchangeTime, seqNum,
                                   cancelPrice, cancelQty, cancelApplSeqNum, cancelOrderType, cancelSide):
        ######
        if cancelApplSeqNum in self.asknq[cancelPrice]:
            self.asknq[cancelPrice][cancelApplSeqNum] = np.int32(self.asknq[cancelPrice][cancelApplSeqNum] - cancelQty)
            if self.asknq[cancelPrice][cancelApplSeqNum] == 0:
                self.asknq[cancelPrice].pop(cancelApplSeqNum)
        else:
            self.bidnq[cancelPrice][cancelApplSeqNum] = np.int32(self.bidnq[cancelPrice][cancelApplSeqNum] - cancelQty)
            if self.bidnq[cancelPrice][cancelApplSeqNum] == 0:
                self.bidnq[cancelPrice].pop(cancelApplSeqNum)
                ######
        if cancelApplSeqNum in self.uOrder:
            cancelPrice, cancelSide = self.uOrder[cancelApplSeqNum]
            assert (cancelPrice > 0)
            self.uOrder.pop(cancelApplSeqNum)

        if cancelSide == 1:
            remain = self.bid[cancelPrice] - cancelQty
            if remain == 0:
                self.bid.pop(cancelPrice)
                ##**##
                self.bid_price_levels -= 1
                ##**##
            elif remain > 0:
                self.bid[cancelPrice] = remain
            ##**##
            self.bid_qty -= cancelQty
            self.bid_amount -= cancelQty * cancelPrice
            self.bid_order_nums -= 1
            ##**##

        elif cancelSide == 2:
            remain = self.ask[cancelPrice] - cancelQty
            if remain == 0:
                self.ask.pop(cancelPrice)
                ##**##
                self.ask_price_levels -= 1
                ##**##
            elif remain > 0:
                self.ask[cancelPrice] = remain
            ##**##
            self.ask_qty -= cancelQty
            self.ask_amount -= cancelQty * cancelPrice
            self.ask_order_nums -= 1
            ##**##
        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)

    def insertOrder(self, clockAtArrival, exchangeTime, seqNum, side, orderType, price, qty, applySeqNum):
        if self.isAuction:
            auctionClockAtArrival = self.localTime[-1]
            auctionExchangeTime = self.exchangeTime[-1]
            auctionSeqNum = self.sequenceNum[-1]
            auctionBBOImprove = 1
            self.localTime = []
            self.exchangeTime = []
            self.sequenceNum = []
            self.bboImprove = []
            self.updateMktInfo(auctionClockAtArrival, auctionExchangeTime, auctionSeqNum, auctionBBOImprove, record=True)
            self.isAuction = False

        hasConvert = False
        if self.hasTempOrder:
            tempSeqNum = list(self.tempOrder.keys())[0]
            tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
            if tempOrderType == '1':
                hasConvert = True
            self.tempToLimit(clockAtArrival, exchangeTime, tempSeqNum)
            self.hasTempOrder = False

        if orderType == '2':
            if side == 1 and price < self.ask1p:
                if price in self.bid:
                    self.bid[price] += qty
                    isImprove = 0
                else:
                    self.bid[price] = qty
                    self.bid_price_levels += 1
                    if price > self.bid1p:
                        isImprove = 1
                    else:
                        isImprove = 0
                self.bidnq[price][applySeqNum] = np.int32(qty)
                ##**##
                self.bid_qty += qty
                self.bid_amount += qty * price
                self.bid_order_nums += 1
                ##**##

                if hasConvert:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=True)
                else:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, isImprove, record=True)
            elif side == 2 and price > self.bid1p:
                if price in self.ask:
                    self.ask[price] += qty
                    isImprove = 0
                else:
                    self.ask[price] = qty
                    self.ask_price_levels += 1
                    if price < self.ask1p:
                        isImprove = 1
                    else:
                        isImprove = 0

                self.asknq[price][applySeqNum] = np.int32(qty)
                ##**##
                self.ask_qty += qty
                self.ask_amount += qty * price
                self.ask_order_nums += 1
                ##**##

                if hasConvert:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=True)
                else:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, isImprove, record=True)
            else:
                # *****
                self.tempOrder[applySeqNum] = (orderType, side, price, qty, 0)
                #                 self.tempOrder[applySeqNum] = ('1', side, price, qty, 0)
                # *****
                self.hasTempOrder = True
                self.guessingTrade(clockAtArrival, exchangeTime, seqNum)

        elif orderType == '1':
            if side == 1:
                self.tempOrder[applySeqNum] = (orderType, side, self.ask1p, qty, 0)
                self.takingOrder[applySeqNum] = (self.ask1p, side)
            else:
                self.tempOrder[applySeqNum] = (orderType, side, self.bid1p, qty, 0)
                self.takingOrder[applySeqNum] = (self.bid1p, side)
            self.hasTempOrder = True

        elif orderType == '3':
            if side == 1:
                if len(self.bid) != 0:
                    self.bid[self.bid1p] += qty
                    self.uOrder[applySeqNum] = (self.bid1p, side)
                    self.bidnq[self.bid1p][applySeqNum] = np.int32(qty)
                    ##**##
                    self.bid_qty += qty
                    self.bid_amount += qty * self.bid1p
                    self.bid_order_nums += 1
                    ##**##
                else:
                    self.tempOrder[applySeqNum] = (orderType, side, self.bid1p, qty, 0)
                    self.hasTempOrder = True
            else:
                if len(self.ask) != 0:
                    self.ask[self.ask1p] += qty
                    self.uOrder[applySeqNum] = (self.ask1p, side)
                    self.asknq[self.ask1p][applySeqNum] = np.int32(qty)
                    ##**##
                    self.ask_qty += qty
                    self.ask_amount += qty * self.ask1p
                    self.ask_order_nums += 1
                    ##**##
                else:
                    self.tempOrder[applySeqNum] = (orderType, side, self.ask1p, qty, 0)
                    self.hasTempOrder = True
            if hasConvert:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=True)
            else:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 0, record=True)

    def removeOrderByTrade(self, clockAtArrival, exchangeTime, seqNum, price, qty, bidApplSeqNum, offerApplSeqNum):

        assert (len(self.tempOrder) == 1)

        if bidApplSeqNum in self.tempOrder:
            tempSeqNum = bidApplSeqNum
            passiveSeqNum = offerApplSeqNum
        elif offerApplSeqNum in self.tempOrder:
            tempSeqNum = offerApplSeqNum
            passiveSeqNum = bidApplSeqNum
        else:
            print('Trade not happend in taking order', bidApplSeqNum, offerApplSeqNum)

        tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
        tempRemain = tempQty - qty
        if tempRemain == 0:
            self.tempOrder.pop(tempSeqNum)
            self.hasTempOrder = False
        else:
            self.tempOrder[tempSeqNum] = (tempOrderType, tempSide, tempPrice, tempRemain, 1)

        if tempSide == 1:
            assert (self.ask1p == price)
            askRemain = self.ask[price] - qty
            if tempOrderType == '1':
                ##**##
                self.ask_qty -= qty
                self.ask_amount -= qty * price
                ##**##
            else:
                pass
                ##$$##
            #                 if self.on_1p:
            #                     self.cur_cum_aggressive_limit_trade_on_1p_buy_qty += qty
            #                     self.cur_cum_aggressive_limit_trade_on_1p_buy_amount += qty * price
            #                 else:
            #                     self.cur_cum_aggressive_limit_trade_over_1p_buy_qty += qty
            #                     self.cur_cum_aggressive_limit_trade_over_1p_buy_amount += qty * price
            ##$$##
            if askRemain == 0:
                self.ask.pop(price)
                if tempOrderType == '1':
                    ##**##
                    self.ask_price_levels -= 1
                    ##**##
            elif askRemain > 0:
                self.ask[price] = askRemain
            else:
                assert (askRemain > 0)
            if tempOrderType == '1':
                self.asknq[price][passiveSeqNum] = np.int32(self.asknq[price][passiveSeqNum] - qty)
                if self.asknq[price][passiveSeqNum] == 0:
                    self.asknq[price].pop(passiveSeqNum)
                    ##**##
                    self.ask_order_nums -= 1
                    ##**##

        elif tempSide == 2:
            assert (self.bid1p == price)
            bidRemain = self.bid[price] - qty
            if tempOrderType == '1':
                ##**##
                self.bid_qty -= qty
                self.bid_amount -= qty * price
                ##**##
            else:
                pass
                ##$$##
            #                 if self.on_1p:
            #                     self.cur_cum_aggressive_limit_trade_on_1p_sell_qty += qty
            #                     self.cur_cum_aggressive_limit_trade_on_1p_sell_amount += qty * price
            #                 else:
            #                     self.cur_cum_aggressive_limit_trade_over_1p_sell_qty += qty
            #                     self.cur_cum_aggressive_limit_trade_over_1p_sell_amount += qty * price
            ##$$##

            if bidRemain == 0:
                self.bid.pop(price)
                if tempOrderType == '1':
                    ##**##
                    self.bid_price_levels -= 1
                    ##**##
            elif bidRemain > 0:
                self.bid[price] = bidRemain
            else:
                assert (bidRemain > 0)
            if tempOrderType == '1':
                self.bidnq[price][passiveSeqNum] = np.int32(self.bidnq[price][passiveSeqNum] - qty)
                if self.bidnq[price][passiveSeqNum] == 0:
                    self.bidnq[price].pop(passiveSeqNum)
                    ##**##
                    self.bid_order_nums -= 1
                    ##**##
        self.cur_cum_volume += qty
        self.cur_cum_amount += price * qty
        self.cur_close = price
        self.trades += 1

        if self.hasTempOrder == False and tempOrderType == '1':
            self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=True)
        else:
            self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=False)

    def removeOrderByCancel(self, clockAtArrival, exchangeTime, seqNum,
                            cancelPrice, cancelQty, cancelApplSeqNum, cancelOrderType, cancelSide):

        if self.isAuction:
            auctionClockAtArrival = self.localTime[-1]
            auctionExchangeTime = self.exchangeTime[-1]
            auctionSeqNum = self.sequenceNum[-1]
            self.localTime = []
            self.exchangeTime = []
            self.sequenceNum = []
            self.updateMktInfo(auctionClockAtArrival, auctionExchangeTime, auctionSeqNum, 1, record=True)
            self.isAuction = False

            
        if cancelApplSeqNum in self.tempOrder:
            tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[cancelApplSeqNum]
            if (tempOrderType == '1') & (tempStatus == 1):
                cancelPrice = self.cur_close
                self.tempToLimit(clockAtArrival, exchangeTime, seqNum)
            else:
                self.tempOrder.pop(cancelApplSeqNum)
                self.hasTempOrder = False
            
            if (tempOrderType == '2') or ((tempOrderType == '1') & (tempStatus == 1)):
                if cancelApplSeqNum in self.asknq[cancelPrice]:
                    self.asknq[cancelPrice][cancelApplSeqNum] = np.int32(self.asknq[cancelPrice][cancelApplSeqNum] - cancelQty)
                    ##**##
                    self.ask_qty -= cancelQty
                    self.ask_amount -= cancelQty * cancelPrice
                    self.ask_order_nums -= 1
                    ##**##
                    if self.asknq[cancelPrice][cancelApplSeqNum] == 0:
                        self.asknq[cancelPrice].pop(cancelApplSeqNum)
                        ##**##
                        self.ask_price_levels -= 1
                        ##**##
                    if tempOrderType != '2':
                        remain = self.ask[cancelPrice] - cancelQty
                        assert(remain == 0)
                        self.ask.pop(cancelPrice)
    
                else:
                    self.bidnq[cancelPrice][cancelApplSeqNum] = np.int32(self.bidnq[cancelPrice][cancelApplSeqNum] - cancelQty)
                    ##**##
                    self.bid_qty -= cancelQty
                    self.bid_amount -= cancelQty * cancelPrice
                    self.bid_order_nums -= 1
                    ##**##
                    if self.bidnq[cancelPrice][cancelApplSeqNum] == 0:
                        self.bidnq[cancelPrice].pop(cancelApplSeqNum)
                        ##**##
                        self.bid_price_levels -= 1
                            ##**##
                    if tempOrderType != '2':
                        remain = self.bid[cancelPrice] - cancelQty
                        assert(remain == 0)
                        self.bid.pop(cancelPrice)

            if tempStatus == 1:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=True)
            else:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=False)

        else:
            hasConvert = False
            if self.hasTempOrder:
                tempSeqNum = list(self.tempOrder.keys())[0]
                tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
                if tempOrderType == '1':
                    hasConvert = True
                self.tempToLimit(clockAtArrival, exchangeTime, seqNum)
                self.hasTempOrder = False

            if cancelOrderType == '3':
                cancelPrice, cancelSide = self.uOrder[cancelApplSeqNum]
                assert (cancelPrice > 0)
                self.uOrder.pop(cancelApplSeqNum)

            if cancelOrderType == '1':
                cancelPrice, cancelSide = self.takingOrder[cancelApplSeqNum]
                assert (cancelPrice > 0)

            if cancelSide == 1:
                remain = self.bid[cancelPrice] - cancelQty
                if remain == 0:
                    self.bid.pop(cancelPrice)
                    ##**##
                    self.bid_price_levels -= 1
                    if cancelPrice == self.bid1p:
                        isImprove = 1
                    else:
                        isImprove = 0
                elif remain > 0:
                    self.bid[cancelPrice] = remain
                    isImprove = 0
                ##**##
                self.bid_qty -= cancelQty
                self.bid_amount -= cancelQty * cancelPrice
                self.bid_order_nums -= 1
                ##**##

            elif cancelSide == 2:
                remain = self.ask[cancelPrice] - cancelQty
                if remain == 0:
                    self.ask.pop(cancelPrice)
                    ##**##
                    self.ask_price_levels -= 1
                    if cancelPrice == self.ask1p:
                        isImprove = 1
                    else:
                        isImprove = 0
                elif remain > 0:
                    self.ask[cancelPrice] = remain
                    isImprove = 0
                ##**##
                self.ask_qty -= cancelQty
                self.ask_amount -= cancelQty * cancelPrice
                self.ask_order_nums -= 1
                ##**##

            if cancelApplSeqNum in self.asknq[cancelPrice]:
                self.asknq[cancelPrice][cancelApplSeqNum] = np.int32(self.asknq[cancelPrice][cancelApplSeqNum] - cancelQty)
                if self.asknq[cancelPrice][cancelApplSeqNum] == 0:
                    self.asknq[cancelPrice].pop(cancelApplSeqNum)
            else:
                self.bidnq[cancelPrice][cancelApplSeqNum] = np.int32(self.bidnq[cancelPrice][cancelApplSeqNum] - cancelQty)
                if self.bidnq[cancelPrice][cancelApplSeqNum] == 0:
                    self.bidnq[cancelPrice].pop(cancelApplSeqNum)

            if hasConvert:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=True)
            else:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, isImprove, record=True)

    def guessingTrade(self, clockAtArrival, exchangeTime, seqNum):
        assert (len(self.tempOrder) == 1)
        key = list(self.tempOrder.keys())[0]
        orderType, orderSide, orderPrice, orderQty, tempStatus = self.tempOrder[key]
        fakeBid = self.bid.copy()
        fakeAsk = self.ask.copy()
        fakeVol = 0
        fakeAmount = 0
        fakeClose = 0
        if orderType == '1':
            print('orderType is 1')
            if orderSide == 1:
                curAskP = sorted(fakeAsk.keys())
                remain = orderQty
                for askP in curAskP:
                    if remain > 0:
                        askSize = fakeAsk[askP]
                        if askSize > remain:
                            fakeAsk[askP] = askSize - remain
                            ######
                            cum_vol = 0
                            for seqNo in self.asknq[askP]:
                                cum_vol += self.asknq[askP][seqNo]
                                if cum_vol > remain:
                                    self.asknq[askP][seqNo] = np.int32(cum_vol - remain)
                                    break
                                elif cum_vol == remain:
                                    self.asknq[askP].pop(seqNo)
                                    break
                                else:
                                    self.asknq[askP].pop(seqNo)
                            ######
                            fakeVol += remain
                            fakeAmount += remain * askP
                            remain = 0
                        else:
                            fakeAsk.pop(askP)
                            ######
                            for seqNo in self.asknq[askP]:
                                self.asknq[askP].pop(seqNo)
                            ######
                            fakeVol += askSize
                            fakeAmount += askSize * askP
                            remain -= askSize
                        fakeClose = askP

            elif orderSide == 2:
                curBidP = sorted(fakeBid.keys(), reverse=True)
                remain = orderQty
                for bidP in curBidP:
                    if remain > 0:
                        bidSize = fakeBid[bidP]
                        if bidSize > remain:
                            fakeBid[bidP] = bidSize - remain
                            ######
                            cum_vol = 0
                            for seqNo in self.bidnq[bidP]:
                                cum_vol += self.bidnq[bidP][seqNo]
                                if cum_vol > remain:
                                    self.bidnq[bidP][seqNo] = np.int32(cum_vol - remain)
                                    break
                                elif cum_vol == remain:
                                    self.bidnq[bidP].pop(seqNo)
                                    break
                                else:
                                    self.bidnq[bidP].pop(seqNo)
                            ######
                            fakeVol += remain
                            fakeAmount += remain * bidP
                            remain = 0
                        else:
                            fakeBid.pop(bidP)
                            ######
                            for seqNo in self.bidnq[bidP]:
                                self.asknq[bidP].pop(seqNo)
                            ######
                            fakeVol += bidSize
                            fakeAmount += bidSize * bidP
                            remain -= bidSize
                        fakeClose = bidP

        elif orderType == '2':
            if orderSide == 1:
                curAskP = sorted(fakeAsk.keys())
                remain = orderQty
                fakeTrades = 0
                for askP in curAskP:
                    if remain > 0 and askP <= orderPrice:
                        askSize = fakeAsk[askP]
                        if askSize > remain:
                            fakeAsk[askP] = askSize - remain
                            ##**##
                            self.ask_qty -= remain
                            self.ask_amount -= remain * askP
                            ##**##
                            ######
                            cum_vol = 0
                            pop_list = []
                            for seqNo in self.asknq[askP]:
                                fakeTrades += 1
                                cum_vol += self.asknq[askP][seqNo]
                                if cum_vol > remain:
                                    self.asknq[askP][seqNo] = np.int32(cum_vol - remain)
                                    break
                                elif cum_vol == remain:
                                    pop_list.append(seqNo)
                                    break
                                else:
                                    pop_list.append(seqNo)
                            for seqNo in pop_list:
                                self.asknq[askP].pop(seqNo)
                                ##**##
                                self.ask_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += remain
                            fakeAmount += remain * askP
                            remain = 0
                        else:
                            fakeAsk.pop(askP)
                            ##**##
                            self.ask_qty -= askSize
                            self.ask_amount -= askSize * askP
                            self.ask_price_levels -= 1
                            ##**##
                            ######
                            pop_list = list(self.asknq[askP].keys())
                            for seqNo in pop_list:
                                fakeTrades += 1
                                self.asknq[askP].pop(seqNo)
                                ##**##
                                self.ask_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += askSize
                            fakeAmount += askSize * askP
                            remain -= askSize
                        fakeClose = askP
                if remain > 0:
                    fakeBid[orderPrice] = remain
                    ######
                    self.bidnq[orderPrice][seqNum] = np.int32(remain)
                    ######
                    ##**##
                    self.bid_qty += remain
                    self.bid_amount += remain * orderPrice
                    self.bid_order_nums += 1
                    self.bid_price_levels += 1
                    ##**##
            elif orderSide == 2:
                curBidP = sorted(fakeBid.keys(), reverse=True)
                remain = orderQty
                fakeTrades = 0
                for bidP in curBidP:
                    if remain > 0 and bidP >= orderPrice:
                        bidSize = fakeBid[bidP]
                        if bidSize > remain:
                            fakeBid[bidP] = bidSize - remain
                            ##**##
                            self.bid_qty -= remain
                            self.bid_amount -= remain * bidP
                            ##**##
                            ######
                            cum_vol = 0
                            pop_list = []
                            for seqNo in self.bidnq[bidP]:
                                fakeTrades += 1
                                cum_vol += self.bidnq[bidP][seqNo]
                                if cum_vol > remain:
                                    self.bidnq[bidP][seqNo] = np.int32(cum_vol - remain)
                                    break
                                elif cum_vol == remain:
                                    pop_list.append(seqNo)
                                    break
                                else:
                                    pop_list.append(seqNo)
                            for seqNo in pop_list:
                                self.bidnq[bidP].pop(seqNo)
                                ##**##
                                self.bid_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += remain
                            fakeAmount += remain * bidP
                            remain = 0
                        else:
                            fakeBid.pop(bidP)
                            ##**##
                            self.bid_qty -= bidSize
                            self.bid_amount -= bidSize * bidP
                            self.bid_price_levels -= 1
                            ##**##
                            ######
                            pop_list = list(self.bidnq[bidP].keys())
                            for seqNo in pop_list:
                                fakeTrades += 1
                                self.bidnq[bidP].pop(seqNo)
                                ##**##
                                self.bid_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += bidSize
                            fakeAmount += bidSize * bidP
                            remain -= bidSize
                        fakeClose = bidP
                if remain > 0:
                    fakeAsk[orderPrice] = remain
                    ######
                    self.asknq[orderPrice][seqNum] = np.int32(remain)
                    ######
                    ##**##
                    self.ask_qty += remain
                    self.ask_amount += remain * orderPrice
                    self.ask_order_nums += 1
                    self.ask_price_levels += 1
                    ##**##

        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)
        self.bboImprove.append(1)

        curBidP = sorted(fakeBid.keys(), reverse=True)[:self.levels]
        curAskP = sorted(fakeAsk.keys())[:self.levels]
        curBidQ = [fakeBid[i] for i in curBidP]
        curBidN = [len(list(self.bidnq[i].keys())) for i in curBidP]

        self.bidp += [curBidP + [0] * (self.levels - len(curBidP))]
        self.bidq += [curBidQ + [0] * (self.levels - len(curBidQ))]
        self.bidn += [curBidN + [0] * (self.levels - len(curBidN))]

        curAskQ = [fakeAsk[i] for i in curAskP]
        curAskN = [len(list(self.asknq[i].keys())) for i in curAskP]
        self.askp += [curAskP + [0] * (self.levels - len(curAskP))]
        self.askq += [curAskQ + [0] * (self.levels - len(curAskQ))]
        self.askn += [curAskN + [0] * (self.levels - len(curAskN))]

        self.cum_volume.append(self.cur_cum_volume + fakeVol)
        self.cum_amount.append(self.cur_cum_amount + fakeAmount)
        self.cum_trades_cnt.append(self.trades + fakeTrades)
        if fakeClose == 0:
            self.close.append(self.cur_close)
        else:
            self.close.append(fakeClose)

        ######
        if len(fakeAsk) != 0:
            ask1p = curAskP[0]
        else:
            ask1p = curBidP[0] + 0.01

        if len(fakeBid) != 0:
            bid1p = curBidP[0]
        else:
            bid1p = curAskP[0] - 0.01
        self.currMid = (bid1p + ask1p) / 2
        ######

        ######
        ####record these infos
        # &#
        self.calcVwapInfo()
        # &#

    def tempToLimit(self, clockAtArrival, exchangeTime, seqNum):
        assert (len(self.tempOrder) == 1)
        tempSeqNum = list(self.tempOrder.keys())[0]
        tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
        if len(self.bid) != 0 and len(self.ask) != 0:
            assert (tempPrice < self.ask1p)
            assert (tempPrice > self.bid1p)
        
        if (tempOrderType == '1') & (tempStatus == 1):
            tempPrice = self.cur_close
            
        if tempSide == 1:
            self.bid[tempPrice] = tempQty
            ######
            self.bidnq[tempPrice][tempSeqNum] = np.int32(tempQty)
            ######
            if tempOrderType == '1':
                ##**##
                self.bid_price_levels += 1
                self.bid_qty += tempQty
                self.bid_amount += tempQty * tempPrice
                self.bid_order_nums += 1
                ##**##

        elif tempSide == 2:
            self.ask[tempPrice] = tempQty
            ######
            self.asknq[tempPrice][tempSeqNum] = np.int32(tempQty)
            ######
            if tempOrderType == '1':
                ##**##
                self.ask_price_levels += 1
                self.ask_qty += tempQty
                self.ask_amount += tempQty * tempPrice
                self.ask_order_nums += 1

        self.tempOrder = {}
        self.hasTempOrder = False
        if (tempOrderType == '1') & (tempStatus == 1):
            seqNum1 = self.ApplSeqNumLs[-2]
            self.updateMktInfo(clockAtArrival, exchangeTime, seqNum1, -1, record=True)
        else:
            self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, 1, record=False)

    def updateMktInfo(self, clockAtArrival, exchangeTime, seqNum, isImprove, record=True):

        curBidP = sorted(self.bid.keys(), reverse=True)[:self.levels]
        curAskP = sorted(self.ask.keys())[:self.levels]
        
        if len(self.ask) == 0 and len(self.bid) == 0:
            self.ask1p = 0
            self.bid1p = 0
            print(seqNum)
        else:
            if len(self.ask) != 0:
                self.ask1p = curAskP[0]
            else:
                self.ask1p = curBidP[0] + 0.01

            if len(self.bid) != 0:
                self.bid1p = curBidP[0]
            else:
                self.bid1p = curAskP[0] - 0.01

        if record == True:
            self.localTime.append(clockAtArrival)
            self.exchangeTime.append(exchangeTime)
            self.sequenceNum.append(seqNum)
            self.bboImprove.append(isImprove)


            curBidQ = [self.bid[i] for i in curBidP]
            curBidN = [len(list(self.bidnq[i].keys())) for i in curBidP]
            self.bidp += [curBidP + [0] * (self.levels - len(curBidP))]
            self.bidq += [curBidQ + [0] * (self.levels - len(curBidQ))]
            self.bidn += [curBidN + [0] * (self.levels - len(curBidN))]

            curAskQ = [self.ask[i] for i in curAskP]
            curAskN = [len(list(self.asknq[i].keys())) for i in curAskP]
            self.askp += [curAskP + [0] * (self.levels - len(curAskP))]
            self.askq += [curAskQ + [0] * (self.levels - len(curAskQ))]
            self.askn += [curAskN + [0] * (self.levels - len(curAskN))]

            self.cum_volume.append(self.cur_cum_volume)
            self.cum_trades_cnt.append(self.trades)
            self.cum_amount.append(self.cur_cum_amount)
            self.close.append(self.cur_close)

            ######
            self.currMid = (self.bid1p + self.ask1p) / 2

            ######
            ####record these infos
            # &#
            self.calcVwapInfo()


    def getAllInfo(self):
        ##get n levels OrderBook
        bp_names = []
        ap_names = []
        bq_names = []
        aq_names = []
        bn_names = []
        an_names = []

        for n in range(1, self.levels + 1):
            bp_names.append('bid{}p'.format(n))
            ap_names.append('ask{}p'.format(n))
            bq_names.append('bid{}q'.format(n))
            aq_names.append('ask{}q'.format(n))
            bn_names.append('bid{}n'.format(n))
            an_names.append('ask{}n'.format(n))
        #
        bidp = pd.DataFrame(self.bidp, columns=bp_names)
        bidq = pd.DataFrame(self.bidq, columns=bq_names)
        bidn = pd.DataFrame(self.bidn, columns=bn_names)

        askp = pd.DataFrame(self.askp, columns=ap_names)
        askq = pd.DataFrame(self.askq, columns=aq_names)
        askn = pd.DataFrame(self.askn, columns=an_names)

        mdDataBase = pd.DataFrame({'clockAtArrival': self.localTime, 'time': self.exchangeTime,
                                   'sequenceNo': self.sequenceNum, 'cum_volume': self.cum_volume, 'cum_trades_cnt': self.cum_trades_cnt,
                                   'cum_amount': self.cum_amount, 'close': self.close, 'bbo_improve': self.bboImprove})
        aggDf = pd.DataFrame([self.total_bid_qty, self.total_ask_qty,
                              self.total_bid_vwap, self.total_ask_vwap,
                              self.total_bid_levels, self.total_ask_levels,
                              self.total_bid_orders_num, self.total_ask_orders_num]).T
        aggCols = ['total_bid_quantity', 'total_ask_quantity',
                   'total_bid_vwap', 'total_ask_vwap',
                   'total_bid_levels', 'total_ask_levels',
                   'total_bid_orders', 'total_ask_orders']
        aggDf.columns = aggCols
        lst = [mdDataBase, bidp, bidq, bidn, askp, askq, askn, aggDf]
        mdData = pd.concat(lst, axis=1, sort=False)
        mdData['StockID'] = self.stockID
        targetCols = (['time', 'clockAtArrival', 'sequenceNo', 'StockID', 'cum_volume', 'cum_amount', 'cum_trades_cnt', 'close', 'bbo_improve'] +
                      bp_names[::-1] + ap_names + bq_names[::-1] + aq_names + bn_names[::-1]
                      + an_names  + aggCols)
        mdData = mdData[targetCols].reset_index(drop=True)
        ##orderbook columns formatting

        for col in (['cum_volume', 'total_bid_quantity', 'total_ask_quantity'] + bq_names + aq_names):
            mdData[col] = mdData[col].fillna(0).astype('int64')
        for col in ['StockID', 'total_bid_levels', 'total_ask_levels',
                    'total_bid_orders', 'total_ask_orders', 'bbo_improve', 'cum_trades_cnt'] + bn_names + an_names:
            mdData[col] = mdData[col].astype('int32')
        for col in ['time']:
            mdData[col] = (mdData[col] * 1000).astype('int64')
        for col in ['cum_amount']:
            mdData[col] = mdData[col].astype(float).round(2)
        return mdData

    def calcVwapInfo(self):
        self.total_bid_qty.append(self.bid_qty)
        self.total_bid_levels.append(self.bid_price_levels)
        self.total_bid_orders_num.append(self.bid_order_nums)
        bmaq = 0 if self.bid_qty == 0 else self.bid_amount / self.bid_qty
        self.total_bid_vwap.append(bmaq)
        self.total_ask_qty.append(self.ask_qty)
        self.total_ask_levels.append(self.ask_price_levels)
        self.total_ask_orders_num.append(self.ask_order_nums)
        amaq = 0 if self.ask_qty == 0 else self.ask_amount / self.ask_qty
        self.total_ask_vwap.append(amaq)
        

if __name__ == '__main__':
    import multiprocessing as mp
    import time

    db = DB("192.168.10.178", 'com_md_eq_cn', 'zhenyuy', 'bnONBrzSMGoE')
    # start date
    thisDate = datetime.date(2017, 3, 1)
    while thisDate <= datetime.date(2017, 5, 31):
        intDate = (thisDate - datetime.date(1899, 12, 30)).days
        thisDate_str = str(thisDate).replace('-', '')
    
        mdOrderLog = db.read('md_order', start_date=thisDate_str, end_date=thisDate_str)
        if mdOrderLog is None:
            thisDate = thisDate + datetime.timedelta(days=1)
            continue

        print(thisDate)
        # sl1 = read_memb_daily(db1, 'index_memb', index_id=[1000852], start_date=int(thisDate_str), end_date=int(thisDate_str))['skey'].unique()
        # assert(len(sl1) == 1000)
        # sl1 = sl1[sl1 > 2000000]
        # sl2 = read_memb_daily(db1, 'index_memb', index_id=[1000852], start_date=20170901, end_date=20201203)['skey'].unique()
        # sl2 = sl2[sl2 > 2000000]
        # assert(len(sl2) == 1028)
        # sl = list(set(sl2) - set(sl1))
        sl1 = mdOrderLog['skey'].unique()
        sl = read_memb_daily(db1, 'index_memb', index_id=[1000852], start_date=20170901, end_date=20201203)['skey'].unique()
        sl = sl[sl > 2000000]
        assert(len(sl) == 1028)
        sl = list(set(sl1) - set(sl))
        
        mdTradeLog = db.read('md_trade', start_date=thisDate_str, end_date=thisDate_str, symbol=list(sl))
        mdLog = db.read('md_snapshot_l2', start_date=thisDate_str, end_date=thisDate_str, symbol=list(sl))
        mdOrderLog = mdOrderLog[mdOrderLog['skey'].isin(sl)]
        mdLog = mdLog[(mdLog['open'] > 0)].groupby('skey')['prev_close', 'open'].first().reset_index()
        re = mdTradeLog.groupby('skey')['date'].count().reset_index().sort_values(by='date', ascending=False)
        re = re.rename(columns={"date": "count"})
        re1 = mdOrderLog.groupby('skey')['date'].count().reset_index().sort_values(by='date', ascending=False)
        re1 = re1.rename(columns={'date': "count1"})
        re = pd.merge(re, re1, on='skey')
        re['cc'] = re['count'] + re['count1']
        re = re.sort_values(by='cc', ascending=False)
        test_list = re['skey'].values
        test_list1 = test_list[0:20]
        try:
            test_list1.remove(2000725)
            proc = 19
        except:
            print('2000725 is not in the slow list')
            proc = 20
        start = time.time()
        orders_data = {}
        trades_data = {}
        prev_data = {}
        open_data = {}
        for s in test_list1:
            mdOrderLog1 = mdOrderLog[mdOrderLog['skey'] == s]
            mdTradeLog1 = mdTradeLog[mdTradeLog['skey'] == s]
            mdLog1 = mdLog[mdLog['skey'] == s]['prev_close'].iloc[0]
            mdLog2 = mdLog[mdLog['skey'] == s]['open'].iloc[0]
            if 'pandas' in str(type(mdOrderLog1)):
                orders_data[s] = mdOrderLog1
                del mdOrderLog1
            if 'pandas' in str(type(mdTradeLog1)):
                trades_data[s] = mdTradeLog1
            if 'pandas' in str(type(mdTradeLog1)):
                prev_data[s] = mdLog1
                open_data[s] = mdLog2
                assert(mdLog1 > 0)
                del mdTradeLog1
                del mdLog1
        g = go(thisDate_str, orders_data, trades_data, prev_data, open_data)
        del orders_data
        del trades_data
        del prev_data
        pool = mp.Pool(processes=proc)
        pool.map(g.run, test_list1)
        pool.close()
        pool.join()
        print(time.time() - start)
       
        del mdOrderLog
        del mdTradeLog        
        thisDate = thisDate + datetime.timedelta(days=1)


# if __name__ == '__main__':
#     import multiprocessing as mp
#     import time

#     db = DB("192.168.10.178", 'com_md_eq_cn', 'zhenyuy', 'bnONBrzSMGoE')
#     # start date
#     thisDate = datetime.date(2020, 2, 21)
#     while thisDate <= datetime.date(2020, 2, 21):
#         intDate = (thisDate - datetime.date(1899, 12, 30)).days
#         thisDate_str = str(thisDate).replace('-', '')
#         sl = [2002235]
#         mdOrderLog = db.read('md_order', start_date=thisDate_str, end_date=thisDate_str, symbol=sl)
#         if mdOrderLog is None:
#             thisDate = thisDate + datetime.timedelta(days=1)
#             continue

#         print(thisDate)
#         mdTradeLog = db.read('md_trade', start_date=thisDate_str, end_date=thisDate_str, symbol=sl)
#         mdLog = db.read('md_snapshot_l2', start_date=thisDate_str, end_date=thisDate_str, symbol=sl)
#         mdLog = mdLog[(mdLog['skey'] > 2000000) & (mdLog['open'] > 0)].groupby('skey')['prev_close', 'open'].first().reset_index()
#         re = mdTradeLog.groupby('skey')['date'].count().reset_index().sort_values(by='date', ascending=False)
#         re = re.rename(columns={"date": "count"})
#         re1 = mdOrderLog.groupby('skey')['date'].count().reset_index().sort_values(by='date', ascending=False)
#         re1 = re1.rename(columns={'date': "count1"})
#         re = pd.merge(re, re1, on='skey')
#         re['cc'] = re['count'] + re['count1']
#         re = re.sort_values(by='cc', ascending=False)
#         test_list = re['skey'].values
#         m = len(test_list)

#         orders_data = {}
#         trades_data = {}
#         prev_data = {}
#         open_data = {}

#         for s in test_list:
#             mdOrderLog1 = mdOrderLog[mdOrderLog['skey'] == s]
#             mdTradeLog1 = mdTradeLog[mdTradeLog['skey'] == s]
#             mdLog1 = mdLog[mdLog['skey'] == s]['prev_close'].iloc[0]
#             mdLog2 = mdLog[mdLog['skey'] == s]['open'].iloc[0]
#             if 'pandas' in str(type(mdOrderLog1)):
#                 orders_data[s] = mdOrderLog1
#                 del mdOrderLog1
#             if 'pandas' in str(type(mdTradeLog1)):
#                 trades_data[s] = mdTradeLog1
#             if 'pandas' in str(type(mdTradeLog1)):
#                 prev_data[s] = mdLog1
#                 open_data[s] = mdLog2
#                 assert(mdLog1 > 0)
#                 del mdTradeLog1
#                 del mdLog1
#         g = go(thisDate_str, orders_data, trades_data, prev_data, open_data)
#         del orders_data
#         del trades_data
#         del prev_data
#         pool = mp.Pool(processes=1)
#         pool.map(g.run, test_list)
#         pool.close()
#         pool.join()

#         del mdOrderLog
#         del mdTradeLog
#         thisDate = thisDate + datetime.timedelta(days=1)
