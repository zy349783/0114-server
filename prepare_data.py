def download_data(dd):
    
    import os
    import glob
    import datetime
    import numpy as np
    import pandas as pd
    import time
    import matplotlib.pyplot as plt
    %matplotlib inline
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
    import pickle

    print('start processing raw Data')
    startTm = datetime.datetime.now()

    startDate = dd
    endDate = dd

    readPath = '/data/home/zhenyu/equityTradeLogs'
    dataPathLs = np.array(glob.glob(os.path.join(readPath, 'speedCompare***.csv')))
    dateLs = np.array([os.path.basename(i).split('_')[1].split('.')[0] for i in dataPathLs])
    dataPathLs = dataPathLs[(dateLs >= startDate) & (dateLs <= endDate)]
    dateLs = dateLs[(dateLs >= startDate) & (dateLs <= endDate)]

    date = dateLs[0]
    assert(date == dd)

    readPath = '/data/home/zhenyu/equityTradeLogs'
    orderLog = pd.read_csv(os.path.join(readPath, 'speedCompare_%s.csv'%date))

    for col in ['clockAtArrival', 'secid', 'updateType', 'vai', 'absFilledThisUpdate', 'orderDirection', 'absOrderSize',
            'absOrderSizeCumFilled', 'date', 'accCode', 'mse']:
        orderLog[col] = orderLog[col].fillna(0)
        orderLog[col] = orderLog[col].astype('int64')

    orderLog = orderLog.sort_values(by=['date', 'secid', 'vai', 'accCode', 'clockAtArrival']).reset_index(drop=True)
    orderLog = orderLog[orderLog["secid"] >= 1000000]

    targetStock = orderLog['secid'].unique()
    targetStock = np.array([int(str(i)[1:]) for i in targetStock])
    targetStockSZ = sorted(targetStock[targetStock < 600000])
    targetStockSH = sorted(targetStock[targetStock >= 600000])

    readPath = '/data/home/zhenyu/Kevin_zhenyu/rawData'
    mdOrderLogPath = glob.glob(os.path.join(readPath, 'logs_%s_zs_92_01***'%date, 'mdOrderLog***.csv'))[-1]
    mdTradeLogPath = glob.glob(os.path.join(readPath, 'logs_%s_zs_92_01***'%date, 'mdTradeLog***.csv'))[-1]

    mdOrderLog = pd.read_csv(mdOrderLogPath)
    mdOrderLog = mdOrderLog[mdOrderLog['SecurityID'].isin(targetStockSZ)]
    mdOrderLog['OrderType'] = mdOrderLog['OrderType'].astype(str)
    mdOrderLog = mdOrderLog[['clockAtArrival', 'sequenceNo', 'TransactTime', 'SecurityID', 'ApplSeqNum', 'Side',
                         'OrderType', 'Price', 'OrderQty']]

    mdTradeLog = pd.read_csv(mdTradeLogPath, encoding='utf-8')
    mdTradeLog['ExecType'] = mdTradeLog['ExecType'].astype(str)
    mdTradeLog = mdTradeLog[mdTradeLog['SecurityID'].isin(targetStockSZ)]
    mdTradeLog['volumeThisUpdate'] = np.where(mdTradeLog['ExecType'] == 'F', mdTradeLog['TradeQty'], 0)
    mdTradeLog['cum_volume'] = mdTradeLog.groupby(['SecurityID'])['volumeThisUpdate'].cumsum()
    mdTradeLog = mdTradeLog[['clockAtArrival', 'sequenceNo', 'TransactTime', 'SecurityID', 'ApplSeqNum', 'cum_volume',
                         'ExecType', 'TradePrice', 'TradeQty', 'TradeMoney', 'BidApplSeqNum', 'OfferApplSeqNum']]

    rawMsgDataSZ = pd.concat([mdOrderLog, mdTradeLog], sort=False)
    del mdOrderLog
    del mdTradeLog

    rawMsgDataSZ = rawMsgDataSZ.sort_values(by=['sequenceNo']).reset_index(drop=True)

    rawMsgDataSZ['cum_volume'] = rawMsgDataSZ.groupby(['SecurityID'])['cum_volume'].ffill().bfill()
    rawMsgDataSZ['ExecType'] = rawMsgDataSZ['ExecType'].fillna('2')
    rawMsgDataSZ['TradeQty'] = rawMsgDataSZ['TradeQty'].fillna(0)

    saveCols = ['clockAtArrival', 'sequenceNo', 'TransactTime', 'SecurityID', 'cum_volume', 'ApplSeqNum', 
            'Side', 'OrderType', 'Price', 'OrderQty', 'ExecType', 'TradePrice', 'TradeQty', 'TradeMoney',
            'BidApplSeqNum', 'OfferApplSeqNum']
    rawMsgDataSZ = rawMsgDataSZ[saveCols]
    savePath = '/data/home/zhenyu/orderLog/mdData'
    savePath = os.path.join(savePath, 'mdLog_msg_%s.pkl'%startDate)
    with open(savePath, 'wb') as f:
        pickle.dump(rawMsgDataSZ, f, protocol=4)
    print(datetime.datetime.now() - startTm)

    
    

    print('start tickToMBD')

    pd.set_option('max_rows', 200)
    pd.set_option('max_columns', 200)

    perc = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

    startTm = datetime.datetime.now()

    startDate = dd
    endDate = dd

    readPath = '/data/home/zhenyu/equityTradeLogs'
    dataPathLs = np.array(glob.glob(os.path.join(readPath, 'speedCompare***.csv')))
    dateLs = np.array([os.path.basename(i).split('_')[1].split('.')[0] for i in dataPathLs])
    dataPathLs = dataPathLs[(dateLs >= startDate) & (dateLs <= endDate)]
    dateLs = dateLs[(dateLs >= startDate) & (dateLs <= endDate)]

    thisDate = dateLs[0]
    assert(thisDate == dd)

    readPath = '/data/home/zhenyu/equityTradeLogs'
    rawOrderLog = pd.read_csv(os.path.join(readPath, 'speedCompare_%s.csv'%thisDate))
    for col in ['clockAtArrival', 'caamd', 'secid', 'updateType', 'vai', 'absFilledThisUpdate', 'orderDirection', 'absOrderSize',
                'absOrderSizeCumFilled', 'date', 'accCode', 'mse']:
        rawOrderLog[col] = rawOrderLog[col].fillna(0)
        rawOrderLog[col] = rawOrderLog[col].astype('int64')   
    rawOrderLog = rawOrderLog.sort_values(by=['date', 'secid', 'vai', 'accCode', 'clockAtArrival']).reset_index(drop=True)
    original_data = rawOrderLog.copy()

    rawOrderLog = rawOrderLog[rawOrderLog["secid"] >= 1000000]

    display('There are accounts with duplicated ticks:')
    display(rawOrderLog[rawOrderLog.duplicated(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], keep=False)]\
    .groupby(['date', 'colo', 'accCode'])['ars'].size())
    rawOrderLog = rawOrderLog.drop_duplicates(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], keep='first')

    display('There are ticks with orderDirection 0')
    display(rawOrderLog[rawOrderLog['orderDirection'] == 0][['date', 'colo', 'accCode', \
                'secid', 'vai', 'updateType', 'sdd', 'orderDirection', 'absOrderSize', 'internalId', 'orderId']])

    assert(rawOrderLog[rawOrderLog['updateType'] == 0][rawOrderLog[rawOrderLog['updateType'] == 0]\
                                                       .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                    'vai', 'absOrderSize', 'internalId'], keep=False)].shape[0] == 0)
    try:
        assert(rawOrderLog[(rawOrderLog['updateType'] == 0) & (rawOrderLog['accCode'] != 8856)][rawOrderLog[(rawOrderLog['updateType'] == 0) & (rawOrderLog['accCode'] != 8856)]\
                                                           .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                        'absOrderSize', 'internalId'], keep=False)].shape[0] == 0)
    except:
        print('There are orders with all things same except sdd')
        print(rawOrderLog[(rawOrderLog['updateType'] == 0) & (rawOrderLog['accCode'] != 8856)][rawOrderLog[(rawOrderLog['updateType'] == 0) & (rawOrderLog['accCode'] != 8856)]\
                                                           .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                        'absOrderSize', 'internalId'], keep=False)])
        assert(rawOrderLog[(rawOrderLog['updateType'] == 0) & (rawOrderLog['accCode'] != 8856)][rawOrderLog[(rawOrderLog['updateType'] == 0) & (rawOrderLog['accCode'] != 8856)]\
                                                           .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                        'absOrderSize', 'internalId', 'sdd'], keep=False)].shape[0] == 0)
    try:
        assert(sum(rawOrderLog[(rawOrderLog['updateType'] != 0) & (rawOrderLog['accCode'] != 8856)].groupby(['date', 'colo', 'accCode', 'secid', 
                    'orderDirection', 'absOrderSize', 'internalId'])['orderId'].nunique() != 1) == 0) 
    except:
        print('There are orders with same internalId but different orderId other than accCode 8856 case')
        print(rawOrderLog[(rawOrderLog['updateType'] != 0) & (rawOrderLog['accCode'] != 8856)].groupby(['date', 'colo', 'accCode', 'secid', 
                    'orderDirection', 'absOrderSize', 'internalId'])['orderId'].nunique()[rawOrderLog[(rawOrderLog['updateType'] != 0) & (rawOrderLog['accCode'] != 8856)].groupby(['date', 'colo', 'accCode', 'secid', 
                    'orderDirection', 'absOrderSize', 'internalId'])['orderId'].nunique() > 1])

    r2 = rawOrderLog[(rawOrderLog['accCode'] != 8856) & (rawOrderLog['orderDirection'] != 0)]
    r1 = rawOrderLog[(rawOrderLog['accCode'] == 8856) & (rawOrderLog['orderDirection'] != 0)]
    r1['test'] = r1.groupby(['date', 'colo', 'accCode', 'secid', 
                'orderDirection', 'absOrderSize']).grouper.group_info[0]
    r1 = r1.sort_values(by=['test', 'clockAtArrival'])
    r1.loc[r1['updateType'] != 0, 'vai'] = np.nan
    r1['vai'] = r1.groupby('test')['vai'].ffill()
    r2['test'] = r2.groupby(['date', 'colo', 'accCode', 'secid', 
                'orderDirection', 'absOrderSize', 'internalId']).grouper.group_info[0]
    r2 = r2.sort_values(by=['test', 'clockAtArrival'])
    r2.loc[r2['updateType'] != 0, 'vai'] = np.nan
    r2['vai'] = r2.groupby('test')['vai'].ffill()
    try:
        assert(sum(r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1) == 0)
    except:
        print('There are orders in 8856 with same internalId and various orderId!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        a = r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique()[r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1].reset_index()
        print(pd.merge(r1, a[['test', 'vai']], on=['test', 'vai'], how='inner')[['secid', 'accCode', 'colo', 'vai', 'updateType', 'sdd', 'internalId', 'orderId', 'absOrderSize', 'absFilledThisUpdate', 'absOrderSizeCumFilled', 'orderPrice', 'tradePrice']])
   
    try:
        assert(sum(r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1) == 0)
    except:
        print('There are orders out of 8856 with same internalId and various orderId!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        a = r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique()[r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1].reset_index()
        print(pd.merge(r2, a[['test', 'vai']], on=['test', 'vai'], how='inner')[['secid', 'accCode', 'colo', 'vai', 'updateType', 'sdd', 'internalId', 'orderId', 'absOrderSize', 'absFilledThisUpdate', 'absOrderSizeCumFilled', 'orderPrice', 'tradePrice']])
    rawOrderLog = pd.concat([r1, r2])
    del r1
    del r2  

    rawOrderLog = rawOrderLog.sort_values(by=['date', 'colo', 'accCode', 'secid', 'vai', 'clockAtArrival']).reset_index(drop=True)
    rawOrderLog['clock'] = rawOrderLog['clockAtArrival'].apply(lambda x: datetime.datetime.fromtimestamp(x/1e6))
    rawOrderLog["broker"] = np.where(rawOrderLog["accCode"].astype(str).apply(lambda x: len(x) == 6), rawOrderLog['accCode'] // 10000, rawOrderLog['accCode'] // 100)
    rawOrderLog['colo_broker'] = rawOrderLog['colo'].str[:2] + '_' + rawOrderLog['broker'].astype('str')
    rawOrderLog['colo_account'] = rawOrderLog['colo'].str[:2] + '_' + rawOrderLog['accCode'].astype('str')
    rawOrderLog['order'] = rawOrderLog.groupby(['date', 'colo', 'accCode', 'secid', 'vai', 'orderDirection', 'absOrderSize', 'internalId']).grouper.group_info[0]
    rawOrderLog['group'] = rawOrderLog.groupby(['date', 'secid', 'vai']).grouper.group_info[0]
    rawOrderLog['startClock'] = rawOrderLog.groupby(['order'])['clockAtArrival'].transform('first')
    rawOrderLog['duration'] = rawOrderLog['clockAtArrival'] - rawOrderLog['startClock']
    rawOrderLog['orderPrice'] = rawOrderLog['orderPrice'].apply(lambda x: round(x, 2))
    rawOrderLog['tradePrice'] = rawOrderLog['tradePrice'].apply(lambda x: round(x, 2))
    rawOrderLog['orderDirection1'] = np.where(rawOrderLog["orderDirection"] == -2, -1, np.where(
        rawOrderLog["orderDirection"] == 2, 1, rawOrderLog["orderDirection"]))
    rawOrderLog['sdd'] = rawOrderLog.groupby('order')['sdd'].transform('first')
    rawOrderLog['caamd'] = rawOrderLog.groupby('order')['caamd'].transform('first')
    rawOrderLog["ars"] = rawOrderLog.groupby(['order'])['ars'].transform('first')
    orderLog = rawOrderLog.copy()

    orderLog['exchange'] = np.where(orderLog['secid'] >= 2000000, 'SZE', 'SSE')
    orderLog['orderNtl'] = orderLog['orderPrice'] * orderLog['absOrderSize']
    orderLog['tradeNtl'] = np.where(orderLog['updateType'] == 4, orderLog['tradePrice']*orderLog['absFilledThisUpdate'], 0)
    orderLog['isMsg'] = np.where(orderLog['updateType'] == 0, 
                                  np.where(orderLog['mse'] == 100, 1, 0), np.nan)
    orderLog['isMsg'] = orderLog.groupby(['order'])['isMsg'].ffill()
    orderLog['firstUpdateType'] = orderLog.groupby(['order'])['updateType'].transform('first')
    orderLog = orderLog[orderLog['firstUpdateType'] == 0]
    orderLog['insertNum'] = np.where(orderLog['updateType'] == 0, 1, 0)
    orderLog['insertNum'] = orderLog.groupby(['order'])['insertNum'].transform('sum')
    orderLog = orderLog[orderLog['insertNum'] == 1]
    orderLog = orderLog[orderLog['secid'] >= 2000000].reset_index(drop=True)
    def getTuple(x):
        return tuple(i for i in x)

    # 1. market orders
    orderDataSZ = rawMsgDataSZ[rawMsgDataSZ['ExecType'] == '2'][['SecurityID', 'ApplSeqNum', 'clockAtArrival', 'sequenceNo', 'Side', 'OrderQty', 'Price', 'cum_volume', "TransactTime"]].reset_index(drop=True)
    orderDataSZ['updateType'] = 0
    tradeDataSZ = pd.concat([rawMsgDataSZ[rawMsgDataSZ['ExecType'] == 'F'][['SecurityID', 'BidApplSeqNum', 'clockAtArrival', 'sequenceNo', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]],
                              rawMsgDataSZ[rawMsgDataSZ['ExecType'] == 'F'][['SecurityID', 'OfferApplSeqNum', 'clockAtArrival', 'sequenceNo', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]]], sort=False)
    tradeDataSZ['ApplSeqNum'] = np.where(tradeDataSZ['BidApplSeqNum'].isnull(), tradeDataSZ['OfferApplSeqNum'], tradeDataSZ['BidApplSeqNum'])
    tradeDataSZ['Side'] = np.where(tradeDataSZ['BidApplSeqNum'].isnull(), 2, 1)
    tradeDataSZ = tradeDataSZ[['SecurityID', 'ApplSeqNum', 'clockAtArrival', 'sequenceNo', 'Side', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]]
    tradeDataSZ['updateType'] = 4
    cancelDataSZ = rawMsgDataSZ[rawMsgDataSZ['ExecType'] == '4'][['SecurityID', 'BidApplSeqNum', 'OfferApplSeqNum', 'clockAtArrival', 'sequenceNo', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]].reset_index(drop=True)
    cancelDataSZ['ApplSeqNum'] = np.where(cancelDataSZ['BidApplSeqNum'] == 0, cancelDataSZ['OfferApplSeqNum'], cancelDataSZ['BidApplSeqNum'])
    cancelDataSZ['Side'] = np.where(cancelDataSZ['BidApplSeqNum'] == 0, 2, 1)
    cancelDataSZ = cancelDataSZ[['SecurityID', 'ApplSeqNum', 'clockAtArrival', 'sequenceNo', 'Side', 'TradeQty', 'cum_volume', "TransactTime"]]
    cancelDataSZ['updateType'] = 3

    msgDataSZ = pd.concat([orderDataSZ, tradeDataSZ, cancelDataSZ], sort=False)
    del orderDataSZ
    del tradeDataSZ
    del cancelDataSZ
    msgDataSZ = msgDataSZ.sort_values(by=['SecurityID', 'ApplSeqNum', 'sequenceNo']).reset_index(drop=True)
    msgDataSZ['TradePrice'] = np.where(msgDataSZ['updateType'] == 4, msgDataSZ['TradePrice'], 0)
    msgDataSZ['TradePrice'] = msgDataSZ['TradePrice'].astype('int64')
    msgDataSZ['TradeQty'] = np.where(msgDataSZ['updateType'] == 4, msgDataSZ['TradeQty'], 0)
    msgDataSZ['TradeQty'] = msgDataSZ['TradeQty'].astype('int64')
    msgDataSZ['secid'] = msgDataSZ['SecurityID'] + 2000000
    assert(msgDataSZ['ApplSeqNum'].max() < 1e8)
    msgDataSZ['StockSeqNum'] = msgDataSZ['SecurityID']*1e8 + msgDataSZ['ApplSeqNum']
    msgDataSZ['date'] = int(thisDate) 
    print('finish market orders')


    # 2. orderLog
    infoData = orderLog[(orderLog['date'] == int(thisDate)) & (orderLog["isMsg"] == 1) 
                        & (orderLog['updateType'].isin([0, 3, 4]))].reset_index(drop=True)
    del orderLog
    infoData['Price'] = infoData['orderPrice'].apply(lambda x: round(x*100, 0))
    infoData['Price'] = infoData['Price'].astype('int64')*100
    infoData['OrderQty'] = infoData['absOrderSize']
    infoData['Side'] = np.where(infoData['orderDirection1'] == 1, 1, 2)
    infoData['TradePrice'] = np.where(infoData['updateType'] == 4, round(infoData['tradePrice']*100, 0), 0)
    infoData['TradePrice'] = infoData['TradePrice'].astype('int64')*100
    statusInfo = infoData.groupby(['order'])['updateType'].apply(lambda x: tuple(x)).reset_index()
    statusInfo.columns = ['order', 'statusLs']
    tradePriceInfo = infoData.groupby(['order'])['TradePrice'].apply(lambda x: tuple(x)).reset_index()
    tradePriceInfo.columns = ['order', 'TradePriceLs']
    tradeQtyInfo = infoData.groupby(['order'])['absFilledThisUpdate'].apply(lambda x: tuple(x)).reset_index()
    tradeQtyInfo.columns = ['order', 'TradeQtyLs']
    infoData = infoData[infoData['updateType'] == 0]
    infoData = pd.merge(infoData, statusInfo, how='left', on=['order'], validate='one_to_one')
    infoData = pd.merge(infoData, tradePriceInfo, how='left', on=['order'], validate='one_to_one')
    infoData = pd.merge(infoData, tradeQtyInfo, how='left', on=['order'], validate='one_to_one')
    infoData['brokerNum'] = infoData.groupby(['date', 'secid', 'vai', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum'])['colo_account'].transform('count')
    display(infoData[infoData['brokerNum'] >= 2].groupby(['colo', 'accCode'])['date'].count())
    display('%.2f%%'%(infoData[infoData['brokerNum'] >= 2].shape[0] / infoData.shape[0]*100))
    display(infoData[infoData['brokerNum'] >= 2].shape[0])
    display(infoData.shape[0])
    infoData = infoData[infoData['brokerNum'] == 1]
    infoData = infoData[['date', 'secid', 'vai', 'ars', "mrstaat", "mrstauc", 'exchange', 'group', 'Price', 'OrderQty', 
                          'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum', 'order', 'colo', 'accCode', 
                          'clockAtArrival', 'orderDirection', 'absOrderSize']]   

    print('finish our orders')

    # 3. find the position of our orders
    checkLog = msgDataSZ[msgDataSZ['updateType'] == 0]
    checkLog = pd.merge(checkLog, infoData.drop_duplicates(subset=['secid', 'ApplSeqNum'])[['secid', 'ApplSeqNum', 'ars']], 
                    on=['secid', 'ApplSeqNum'], how='left')
    checkLog['ApplSeqNum'] = np.where(~checkLog['ars'].isnull(), checkLog['ApplSeqNum'], checkLog['ars'])
    checkLog['start_time'] = np.where(~checkLog['ars'].isnull(), checkLog['clockAtArrival'], checkLog['ars'])
    checkLog.drop(["ars"],axis=1,inplace=True)
    checkLog['ApplSeqNum'] = checkLog.groupby(['secid'])['ApplSeqNum'].ffill()
    checkLog['start_time'] = checkLog.groupby(['secid'])['start_time'].ffill()
    checkLog['end_time'] = checkLog['start_time'] + 100*1e3
    checkLog = checkLog[(~checkLog['ApplSeqNum'].isnull()) & (checkLog['clockAtArrival'] > checkLog['start_time']) & 
                        (checkLog['clockAtArrival'] < checkLog['end_time'])]
    msgDataSZ = msgDataSZ[msgDataSZ['StockSeqNum'].isin(checkLog['StockSeqNum'].values)]
    print('finish get the interval')

    statusInfo = msgDataSZ.groupby(['StockSeqNum'])['updateType'].apply(lambda x: getTuple(x)).reset_index()
    statusInfo.columns = ['StockSeqNum', 'statusLs']
    tradePriceInfo = msgDataSZ.groupby(['StockSeqNum'])['TradePrice'].apply(lambda x: tuple(x)).reset_index()
    tradePriceInfo.columns = ['StockSeqNum', 'TradePriceLs']
    tradeQtyInfo = msgDataSZ.groupby(['StockSeqNum'])['TradeQty'].apply(lambda x: tuple(x)).reset_index()
    tradeQtyInfo.columns = ['StockSeqNum', 'TradeQtyLs']
    del msgDataSZ
    checkLog = pd.merge(checkLog, statusInfo, how='left', on=['StockSeqNum'], validate='one_to_one')
    checkLog = pd.merge(checkLog, tradePriceInfo, how='left', on=['StockSeqNum'], validate='one_to_one')
    checkLog = pd.merge(checkLog, tradeQtyInfo, how='left', on=['StockSeqNum'], validate='one_to_one')  
    del statusInfo
    del tradePriceInfo
    del tradeQtyInfo
    checkLog = checkLog.rename(columns={'clockAtArrival':'caa_orderLog'})

    try:
        checkLog = pd.merge(checkLog, infoData, how='left', on=['date', 'secid', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum'], validate='many_to_one')
    except:
        print('There are orders with same pattern but different vai, same ApplSeqNum')
        display([infoData[infoData.duplicated(['date', 'secid', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum'], keep=False)]])
        checkLog = pd.merge(checkLog, infoData, how='left', on=['date', 'secid', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum'])
    del infoData
    checkLog = checkLog.drop_duplicates(['date', 'secid', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum'], keep=False)
    checkLog = checkLog[~checkLog['accCode'].isnull()]
    checkLog = checkLog.reset_index(drop=True)
    if original_data[original_data.duplicated(['date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], keep=False)].shape[0] == 0:
        original_data = pd.merge(original_data, checkLog[['caa_orderLog', 'start_time', 'date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs']], validate='one_to_one',
                                on=['date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], how='left')
    else:
        original_data = pd.merge(original_data, checkLog[['caa_orderLog', 'start_time', 'date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs']], validate='many_to_one',
                                on=['date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], how='left')    

    print(datetime.datetime.now() - startTm)



    def DB(host, db_name, user, passwd):
        auth_db = db_name if user not in ('admin', 'root') else 'admin'
        url = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
        client = pymongo.MongoClient(url, maxPoolSize=None)
        db = client[db_name]
        return db

    def read_memb_daily(db, name, start_date=None, end_date=None, skey=None, index_id=None, interval=None, col=None, return_sdi=True):
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

    def read_beta_daily(db, name, start_date=None, end_date=None, skey=None, interval=None, col=None, return_sdi=True): 
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


    database_name = 'com_md_eq_cn'
    user = "zhenyuy"
    password = "bnONBrzSMGoE"

    pd.set_option('max_columns', 200)
    db1 = DB("192.168.10.178", database_name, user, password)


    import os
    import glob
    import datetime
    import numpy as np
    import pandas as pd

    pd.set_option('max_rows', 100)
    pd.set_option('max_columns', 100)

    perc = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

    print('start generate return')
    startTm = datetime.datetime.now()

    readPath = '/data/home/zhenyu/equityTradeLogs'
    dataPathLs = np.array(glob.glob(os.path.join(readPath, 'speedCompare***.csv')))
    dateLs = np.array([os.path.basename(i).split('_')[1].split('.')[0] for i in dataPathLs])
    dateLs = dateLs[(dateLs >= startDate) & (dateLs <= endDate)]

    date = dateLs[0]
    assert(date == dd)

    index = read_memb_daily(db1, 'index_memb', int(startDate), int(endDate), index_id=[1000300, 1000905, 1000852, 1000985])
    index = index[['index_id', 'skey', 'date']].reset_index(drop=True)
    index = index.rename(columns={'index_id':'indexCat', 'skey':'secid'})
    index.loc[index['indexCat'] == 1000985, 'indexCat'] = 1000852
    index['ID'] = index['secid']

    d = read_beta_daily(db1, 'mktbeta', int(startDate), int(endDate))
    d1 = d[['skey', 'beta_60d_IF', 'date']]
    d1 = d1.rename(columns={'beta_60d_IF':"beta_60"})
    d1['indexCat'] = 1000300
    d2 = d[['skey', 'beta_60d_IC', 'date']]
    d2 = d2.rename(columns={'beta_60d_IC':"beta_60"})
    d2['indexCat'] = 1000905
    d3 = d[['skey', 'beta_60d_CSI1000', 'date']]
    d3 = d3.rename(columns={'beta_60d_CSI1000':"beta_60"})
    d3['indexCat'] = 1000852
    betaData = pd.concat([d1, d2, d3]).reset_index(drop=True)
    betaData['date'] = betaData['date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d').date())
    betaData = betaData.rename(columns={'skey':'secid'})


    dateDate = datetime.datetime.strptime(date, '%Y%m%d').date()
    dateBetaData = betaData[betaData['date'] == dateDate]

    readPath = '/data/home/zhenyu/equityTradeLogs'
    orderLog = pd.read_csv(os.path.join(readPath, 'speedCompare_%s.csv'%date))
    for col in ['clockAtArrival', 'caamd', 'secid', 'updateType', 'vai', 'absFilledThisUpdate', 'orderDirection', 'absOrderSize',
                'absOrderSizeCumFilled', 'date', 'accCode', 'mse']:
        orderLog[col] = orderLog[col].fillna(0)
        orderLog[col] = orderLog[col].astype('int64') 
#     orderLog = orderLog[~orderLog['vai'].isnull()]
    orderLog = orderLog.rename(columns={'mdClockAtArrival': 'caamd'})
    
    display('There are accounts with duplicated ticks:')
    display(orderLog[orderLog.duplicated(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], keep=False)]\
    .groupby(['date', 'colo', 'accCode'])['ars'].size())
    orderLog = orderLog.drop_duplicates(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], keep='first')
    
    display('There are ticks with orderDirection 0')
    display(orderLog[orderLog['orderDirection'] == 0][['date', 'colo', 'accCode', \
                'secid', 'vai', 'updateType', 'sdd', 'orderDirection', 'absOrderSize', 'internalId', 'orderId']])

    assert(orderLog[orderLog['updateType'] == 0][orderLog[orderLog['updateType'] == 0]\
                                                        .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                    'vai', 'absOrderSize', 'internalId'], keep=False)].shape[0] == 0)
    try:
        assert(orderLog[(orderLog['updateType'] == 0) & (orderLog['accCode'] != 8856)][orderLog[(orderLog['updateType'] == 0) & (orderLog['accCode'] != 8856)]\
                                                            .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                        'absOrderSize', 'internalId'], keep=False)].shape[0] == 0)
    except:
        print('There are orders with all things same except sdd')
        print(orderLog[(orderLog['updateType'] == 0) & (orderLog['accCode'] != 8856)][orderLog[(orderLog['updateType'] == 0) & (orderLog['accCode'] != 8856)]\
                                                            .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                        'absOrderSize', 'internalId'], keep=False)])
        assert(orderLog[(orderLog['updateType'] == 0) & (orderLog['accCode'] != 8856)][orderLog[(orderLog['updateType'] == 0) & (orderLog['accCode'] != 8856)]\
                                                            .duplicated(['date', 'colo', 'accCode', 'secid', 'orderDirection',
                                                                        'absOrderSize', 'internalId', 'sdd'], keep=False)].shape[0] == 0)
    try:
        assert(sum(orderLog[(orderLog['updateType'] != 0) & (orderLog['accCode'] != 8856)].groupby(['date', 'colo', 'accCode', 'secid', 
                    'orderDirection', 'absOrderSize', 'internalId'])['orderId'].nunique() != 1) == 0) 
    except:
        print('There are orders with same internalId but different orderId other than accCode 8856 case')
        print(orderLog[(orderLog['updateType'] != 0) & (orderLog['accCode'] != 8856)].groupby(['date', 'colo', 'accCode', 'secid', 
                    'orderDirection', 'absOrderSize', 'internalId'])['orderId'].nunique()[orderLog[(orderLog['updateType'] != 0) & (orderLog['accCode'] != 8856)].groupby(['date', 'colo', 'accCode', 'secid', 
                    'orderDirection', 'absOrderSize', 'internalId'])['orderId'].nunique() > 1])

    r2 = orderLog[(orderLog['accCode'] != 8856) & (orderLog['orderDirection'] != 0)]
    r1 = orderLog[(orderLog['accCode'] == 8856) & (orderLog['orderDirection'] != 0)]
    r1['test'] = r1.groupby(['date', 'colo', 'accCode', 'secid', 
                'orderDirection', 'absOrderSize']).grouper.group_info[0]
    r1 = r1.sort_values(by=['test', 'clockAtArrival'])
    r1.loc[r1['updateType'] != 0, 'vai'] = np.nan
    r1['vai'] = r1.groupby('test')['vai'].ffill()
    r2['test'] = r2.groupby(['date', 'colo', 'accCode', 'secid', 
                'orderDirection', 'absOrderSize', 'internalId']).grouper.group_info[0]
    r2 = r2.sort_values(by=['test', 'clockAtArrival'])
    r2.loc[r2['updateType'] != 0, 'vai'] = np.nan
    r2['vai'] = r2.groupby('test')['vai'].ffill()
    try:
        assert(sum(r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1) == 0)
    except:
        print('There are orders in 8856 with same internalId and various orderId!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        a = r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique()[r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1].reset_index()
        print(pd.merge(r1, a[['test', 'vai']], on=['test', 'vai'], how='inner')[['secid', 'accCode', 'colo', 'vai', 'updateType', 'sdd', 'internalId', 'orderId', 'absOrderSize', 'absFilledThisUpdate', 'absOrderSizeCumFilled', 'orderPrice', 'tradePrice']])
   
    try:
        assert(sum(r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1) == 0)
    except:
        a = r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique()[r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1].reset_index()
        print('There are orders out of 8856 with same internalId and various orderId!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(pd.merge(r2, a[['test', 'vai']], on=['test', 'vai'], how='inner')[['secid', 'accCode', 'colo', 'vai', 'updateType', 'sdd', 'internalId', 'orderId', 'absOrderSize', 'absFilledThisUpdate', 'absOrderSizeCumFilled', 'orderPrice', 'tradePrice']])
    orderLog = pd.concat([r1, r2])
    del r1
    del r2  
    
    orderLog = orderLog.sort_values(by=['date', 'colo', 'accCode', 'secid', 'vai', 'clockAtArrival']).reset_index(drop=True)
    orderLog['order'] = orderLog.groupby(['date', 'colo', 'accCode', 'secid', 'vai', 'orderDirection', 'absOrderSize', 'internalId']).grouper.group_info[0]
    orderLog['firstUpdateType'] = orderLog.groupby(['order'])['updateType'].transform('first')
    orderLog = orderLog[orderLog['firstUpdateType'] == 0]

    orderLog['insertNum'] = np.where(orderLog['updateType'] == 0, 1, 0)
    orderLog['insertNum'] = orderLog.groupby(['order'])['insertNum'].transform('sum')
    orderLog = orderLog[orderLog['insertNum'] == 1]

    orderLog['innerSeq'] = orderLog.index.values
    targetStockLs = orderLog['secid'].unique()
    orderLog['firstUpdateType'] = orderLog.groupby(['order'])['updateType'].transform('first')
    orderLog['firstClock'] = orderLog.groupby(['order'])['clockAtArrival'].transform('first')
    orderLog['caamd'] = orderLog.groupby(['order'])['caamd'].transform('first')

    assert(orderLog[orderLog['firstUpdateType'] != 0].shape[0] == 0)
    indexCatData = index[index['date'] == int(date)]
    orderLog = pd.merge(orderLog, indexCatData[['secid', 'indexCat']], how='left', on=['secid'], validate='many_to_one')
    orderLog = pd.merge(orderLog, dateBetaData[['secid', 'indexCat', 'beta_60']], how='left', on=['secid', 'indexCat'], validate='many_to_one')

    readPath = '/data/home/zhenyu/Kevin_zhenyu/rawData/logs_%s_***'%date
    mdDataSHPath = glob.glob(os.path.join(readPath, 'mdLog_SH***.csv'))[-1]
    mdDataSH = pd.read_csv(mdDataSHPath)
    mdDataSH['ID'] = mdDataSH['StockID'] + 1000000
    mdDataSH['time'] = mdDataSH.time.str.slice(0, 2) + mdDataSH.time.str.slice(3, 5) + mdDataSH.time.str.slice(6, 8) + '000'
    mdDataSH['time'] = mdDataSH['time'].astype('int64')
    mdDataSH['time'] = mdDataSH.groupby(['ID'])['time'].cummax()
    mdDataSH['max_cum_volume'] = mdDataSH.groupby(['StockID'])['cum_volume'].cummax()
    indexData = mdDataSH[mdDataSH['StockID'].isin([300, 852, 905])][['ID', 'sequenceNo', 'close']].reset_index(drop=True)
    mdDataSH = mdDataSH[mdDataSH['StockID'] >= 600000]
    mdDataSH = mdDataSH[(mdDataSH['cum_volume'] > 0) & (mdDataSH['time'] >= 93000000) &\
                        (mdDataSH['cum_volume'] == mdDataSH['max_cum_volume'])]
    mdDataSH = mdDataSH[['ID', 'clockAtArrival', 'sequenceNo', 'time', 'cum_volume', 'bid1p', 'ask1p', 'bid1q', 'ask1q', 'bid5q', 'ask5q']]

    mdDataSZPath = glob.glob(os.path.join(readPath, 'mdLog_SZ***.csv'))[-1]
    mdDataSZ = pd.read_csv(mdDataSZPath)
    mdDataSZ['ID'] = mdDataSZ['StockID'] + 2000000
    mdDataSZ['time'] = mdDataSZ.time.str.slice(0, 2) + mdDataSZ.time.str.slice(3, 5) + mdDataSZ.time.str.slice(6, 8) + '000'
    mdDataSZ['time'] = mdDataSZ['time'].astype('int64')
    mdDataSZ['time'] = mdDataSZ.groupby(['ID'])['time'].cummax()
    mdDataSZ['max_cum_volume'] = mdDataSZ.groupby(['StockID'])['cum_volume'].cummax()
    mdDataSZ = mdDataSZ[(mdDataSZ['cum_volume'] > 0) & (mdDataSZ['time'] >= 93000000) &\
                        (mdDataSZ['cum_volume'] == mdDataSZ['max_cum_volume'])]
    mdDataSZ = mdDataSZ[['ID', 'clockAtArrival', 'sequenceNo', 'time', 'cum_volume', 'bid1p', 'ask1p', 'bid1q', 'ask1q', 'bid5q', 'ask5q']]

    mdData = pd.concat([mdDataSH, mdDataSZ, indexData]).reset_index(drop=True)
    mdData = mdData.sort_values(by=['sequenceNo']).reset_index(drop=True)

    addData = pd.DataFrame({'indexCat': [1000300, 1000905, 1000852], 'ID': [1000300, 1000905, 1000852], 
                            'secid':[1000300, 1000905, 1000852]})
    indexCatData = pd.concat([indexCatData, addData], sort=False).reset_index(drop=True)
    mdData = pd.merge(mdData, indexCatData, how='left', on=['ID'], validate='many_to_one')
    mdData = mdData[~mdData['indexCat'].isnull()].reset_index(drop=True)
    mdData['indexClose'] = np.where(mdData['ID'].isin([1000300, 1000852, 1000905]), mdData['close'], np.nan)
    mdData['indexClose'] = mdData.groupby(['indexCat'])['indexClose'].ffill()
    mdData = mdData[~mdData['ID'].isin([1000300, 1000852, 1000905])].reset_index(drop=True)

    mdData = mdData.sort_values(by=['ID', 'sequenceNo']).reset_index(drop=True)
    mdData['safeBid1p'] = np.where(mdData['bid1p'] == 0, mdData['ask1p'], mdData['bid1p'])
    mdData['safeAsk1p'] = np.where(mdData['ask1p'] == 0, mdData['bid1p'], mdData['ask1p'])
    mdData['adjMid'] = (mdData['safeBid1p']*mdData['ask1q'] + mdData['safeAsk1p']*mdData['bid1q'])/(mdData['bid1q'] + mdData['ask1q'])

    mdData['session'] = np.where(mdData['time'] >= 130000000, 1, 0)
    def findTmValue(clockLs, tm, method='L', buffer=0):
        maxIx = len(clockLs)
        orignIx = np.arange(maxIx)
        if method == 'F':
            ix = np.searchsorted(clockLs, clockLs+(tm-buffer))
            ## if target future index is next tick, mask
            mask = (orignIx == (ix - 1))|(orignIx == ix)|(ix == maxIx)
        elif method == 'L':
            ## if target future index is last tick, mask
            ix = np.searchsorted(clockLs, clockLs-(tm-buffer))
            ix = ix - 1
            ix[ix<0] = 0
            ## !!!ATTENTION: model3 change
            mask = (orignIx == ix) | ((clockLs-(tm-buffer)).values < clockLs.values[0])
        ix[mask] = -1
        return ix

    mdData = mdData.reset_index(drop=True)
    groupAllData = mdData.groupby(['ID', 'session'])
    mdData['sessionStartCLA'] = groupAllData['clockAtArrival'].transform('min')
    mdData['relativeClock'] = mdData['clockAtArrival'] - mdData['sessionStartCLA']
    mdData['trainFlag'] = np.where(mdData['relativeClock'] > 179.5*1e6, 1, 0)
    mdData['index'] = mdData.index.values
    mdData['sessionStartIx'] = groupAllData['index'].transform('min')
    for tm in [30, 90, 300]:
        tmCol = 'F{}s_ix'.format(tm)
        mdData[tmCol] = groupAllData['relativeClock'].transform(lambda x: findTmValue(x, tm*1e6, 'F', 5*1e5)).astype(int)
    nearLimit = ((mdData.ask5q.values == 0) | (mdData.bid5q.values == 0))

    for tm in [30, 90, 300]:
        tmIx = mdData['F{}s_ix'.format(tm)].values + mdData['sessionStartIx'].values
        adjMid_tm = mdData['adjMid'].values[tmIx]
        adjMid_tm[mdData['F{}s_ix'.format(tm)].values == -1] = np.nan
        mdData['adjMid_F{}s'.format(tm)] = adjMid_tm

    for tm in [30, 90, 300]:
        tmIx = mdData['F{}s_ix'.format(tm)].values + mdData['sessionStartIx'].values
        adjMid_tm = mdData['indexClose'].values[tmIx]
        adjMid_tm[mdData['F{}s_ix'.format(tm)].values == -1] = np.nan
        mdData['indexClose_F{}s'.format(tm)] = adjMid_tm

    mdData = mdData[mdData['ID'].isin(targetStockLs)]
    mdStartPos = mdData.drop_duplicates(subset=['ID', 'cum_volume'], keep='last')
    mdStartPos = mdStartPos[['ID', 'cum_volume', 'clockAtArrival']].reset_index(drop=True)
    mdStartPos.columns = ['secid', 'vai', 'mdStartClock']
    mdStartPos['isOrder'] = 0
    tradeStartPos = orderLog[orderLog['updateType'] == 0][['secid', 'vai', 'order']].reset_index(drop=True)
    tradeStartPos['isOrder'] = 1
    tradeStartPos = pd.concat([mdStartPos, tradeStartPos], sort=False)
    tradeStartPos = tradeStartPos.sort_values(by=['secid', 'vai', 'isOrder'])
    tradeStartPos['mdStartClock'] = tradeStartPos.groupby(['secid'])['mdStartClock'].ffill()
    tradeStartPos['mdStartClock'] = tradeStartPos.groupby(['secid'])['mdStartClock'].backfill()
    tradeStartPos = tradeStartPos[tradeStartPos['isOrder'] == 1][['secid', 'vai', 'order', 'mdStartClock']]

    orderLog = pd.merge(orderLog, tradeStartPos[['order', 'mdStartClock']], how='left', on=['order'], validate='many_to_one')
    orderLog['mdClockAtArrival'] = orderLog['clockAtArrival'] - orderLog['caamd'] + orderLog['mdStartClock']

    tradeData = orderLog[['secid', 'mdClockAtArrival', 'innerSeq']].reset_index(drop=True)
    tradeData.columns = ['ID', 'clockAtArrival', 'innerSeq']
    tradeData['isOrder'] = 1

    mdData = pd.concat([mdData, tradeData], sort=False)
    mdData = mdData.sort_values(by=['ID', 'clockAtArrival', 'isOrder', 'innerSeq']).reset_index(drop=True)
    for col in ['indexClose', 'adjMid_F30s', 'indexClose_F30s', 'adjMid_F90s', 'indexClose_F90s',
                'adjMid_F300s', 'indexClose_F300s']:
        mdData[col] = mdData.groupby(['ID'])[col].backfill()
        mdData[col] = mdData.groupby(['ID'])[col].ffill()

    tradeData = mdData[mdData['isOrder'] == 1][['ID', 'innerSeq', 'adjMid_F30s', 'adjMid_F90s', 'adjMid_F300s',
                                                'indexClose', 'indexClose_F30s', 'indexClose_F90s', 'indexClose_F300s']].reset_index(drop=True)
    tradeData = tradeData.rename(columns={'ID': 'secid'})
    orderLog = pd.merge(orderLog, tradeData, how='left', on=['secid', 'innerSeq'], validate='one_to_one')

    if original_data[original_data.duplicated(['date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], keep=False)].shape[0] == 0:
        original_data = pd.merge(original_data, orderLog[['beta_60', 'adjMid_F30s', 'adjMid_F90s', 'adjMid_F300s', 'indexClose', \
        'indexClose_F30s', 'indexClose_F90s', 'indexClose_F300s', 'date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
        'orderDirection', 'absOrderSize']], validate='one_to_one', on=['date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], how='left')
    else:
        original_data = pd.merge(original_data, orderLog[['beta_60', 'adjMid_F30s', 'adjMid_F90s', 'adjMid_F300s', 'indexClose', \
        'indexClose_F30s', 'indexClose_F90s', 'indexClose_F300s', 'date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
        'orderDirection', 'absOrderSize']], validate='many_to_one', on=['date', 'secid', 'accCode', 'clockAtArrival', 'updateType', \
                                        'orderDirection', 'absOrderSize'], how='left')  
    del orderLog
    with open('/data/home/zhenyu/orderLog/data/'+startDate + '.pkl', 'wb') as f:
        pickle.dump(original_data, f, protocol=4)
    del original_data
    print(datetime.datetime.now() - startTm)
    
    
    
    
    
    
    
    
    


    orderLog = rawOrderLog.copy()
    del rawOrderLog
    startTm = datetime.datetime.now()
    print('start generate speed compared with market orders')
    
    ### Assertion 1:  make sure same direction in same date, secid, vai
    print('=======================================================================================')
    print('1. same date, secid, vai: same direction')
    taking = orderLog[orderLog['ars'].isin([1, 7])]
    making = orderLog[orderLog['ars'].isin([2, 3])]
    else_orders = orderLog[~(orderLog['ars'].isin([1, 2, 3, 7]))]
    display('orders with abnormal ars values')
    display(else_orders[else_orders['updateType'] == 0].groupby('ars')['date'].size().sort_values(ascending=False))
    display(else_orders[else_orders['updateType'] == 0].groupby('accCode')['date'].size().sort_values(ascending=False))
    taking['directNum'] = taking.groupby(['date', 'secid', 'vai', 'sdd'])['orderDirection1'].transform('nunique')
    if len(taking[taking['directNum'] != 1]) > 0:
        print('opposite direction for same date, same secid, same vai')
        display(taking[(taking['directNum'] != 1) & (taking['updateType'] == 0)].groupby(['accCode'])['orderDirection'].size())
        taking = taking[taking['directNum'] == 1]

    assert((taking.groupby(['date', 'secid', 'vai', 'sdd'])['orderDirection1'].nunique() == 1).all() == True)
    orderLog = pd.concat([taking, making]).sort_values(by=['date', 'colo', 'accCode', 'secid', 'vai', 'clockAtArrival']).reset_index(drop=True)

    ## Assertion 2:  make sure each account, secid, vai only has one insertion
    print('=======================================================================================')
    print('2. same date, secid, vai, accCode: one insertion')
    a = orderLog[orderLog['updateType'] == 0].groupby(['date', 'colo', 'accCode', 'secid', 'vai', 'sdd'])['clockAtArrival'].count()
    if len(a[a > 1]) > 0:
        print('more than one insertion at same time')
        a = a[a>1].reset_index()
        display(a)
        d_el = pd.merge(orderLog, a, on=['date', 'colo', 'accCode', 'secid', 'vai', 'sdd'])['order'].unique()
        orderLog = orderLog[~(orderLog['order'].isin(d_el))]       


    orderLog['isMsg'] = np.where(orderLog['updateType'] == 0, 
                                 np.where(orderLog['mse'] == 100, 1, 0), np.nan)
    orderLog['isMsg'] = orderLog.groupby(['order'])['isMsg'].ffill()

    placeSZE = orderLog[(orderLog['secid'] >= 2000000) & (orderLog['updateType'] == 0)]
    print('%.2f%% SZE orders triggered by msg data'%(placeSZE[placeSZE['isMsg'] == 1].shape[0]/placeSZE.shape[0]*100))


    ### Assertion 3:  check IPO stocks selling status
    print('=======================================================================================')
    print('3. IPO stocks selling (ars = 301, 302)')
    if orderLog[orderLog['ars'].isin([301, 302])].shape[0] != 0:
        kk = orderLog[orderLog['ars'].isin([301, 302])]
        print(kk)
        try:
            assert(kk[kk['orderDirection1'] == 1].shape[0] == 0)
            print('we only sell, never buy')
        except:
            print('There are IPO buy side orders!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(kk[kk['orderDirection1'] == 1])
        kk1 = kk[kk['updateType'] == 0]
        kk1 = kk1.sort_values(by=['accCode', 'secid','clockAtArrival'])
        kk1['diff'] = kk1.groupby(['accCode', 'secid'])['clockAtArrival'].apply(lambda x: x-x.shift(1))
        kk1['diff'] = kk1['diff'].fillna(0)
        try:
            assert(kk1[kk1['diff'] < 10e6].shape[0] == 0)
            print('for each stock in the same account, there is no insertion within 10 seconds of the previous insertion')
        except:
            print('There are insertion within 10 seconds for orders under same account same stock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(kk1[kk1['diff'] < 10e6])
        kk2 = kk[(kk['updateType'] == 1)]
        try:
            assert(kk2[kk2['duration'] < 3e6].shape[0] == 0)
            print('for each stock in the same account, the cancellation of an order happens more than 3 seconds after the insertion')
        except:
            print('There are cancellation within 3 seconds for orders under same account same stock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(kk2[kk2['duration'] < 3e6])


    ### Assertion 4: check updateType == 7 orders, make sure updateType == 7 orders < 20 per account, < 100 in total
    print('=======================================================================================')
    print('4. updateType 7 orders')
    if orderLog[orderLog['updateType'] == 7].shape[0] != 0:
        assert(orderLog[orderLog['updateType'] == 7].groupby('accCode')['order'].nunique().max() < 20)
        assert(orderLog[orderLog['updateType'] == 7].groupby('accCode')['order'].nunique().sum() < 100)

    ### Assertion 5: check updateType == 6 orders, make sure updateType == 6 orders < 5% per account
    print('=======================================================================================')
    print('5. updateType 6 orders')
    k1 = orderLog[orderLog['updateType'] == 6].groupby('accCode')['order'].nunique().reset_index()
    k2 = orderLog.groupby('accCode')['order'].nunique().reset_index()
    k = pd.merge(k1, k2, on='accCode', how='left')
    k['prob'] = k['order_x']/k['order_y']
    try:
        assert(sum(k['prob'] >= 0.05) == 0)
    except:
        print('There are accounts with more than 5% updateType 6 orders!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(k[k['prob'] >= 0.05])

    ### Assertion 6: check CYB orders, make sure CYB stocks total absOrderSize < 30w
    print('=======================================================================================')
    print('6. CYB stocks total order size < 30w')
    try:
        assert(orderLog[(orderLog['secid'] >= 2300000) & (orderLog['updateType'] == 0)]['absOrderSize'].max() <= 300000)
    except:
        print('CYB stocks total absOrderSize >= 30w!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')


    ### Assertion 7:  make sure there is no unexpected updateType 
    print('=======================================================================================')
    print('7. unexpected updateType')
    def getTuple(x):
        return tuple(i for i in x)

    checkLog = orderLog[~((orderLog['updateType'] == 4) & (orderLog.groupby(['order'])['updateType'].shift(-1) == 4))]
    checkLog = checkLog.groupby(['order'])['updateType'].apply(lambda x: getTuple(x)).reset_index()
    checkLog['status'] = np.where(checkLog['updateType'].isin([(0, 2, 4), (0, 2, 1, 4), (0, 2, 1, 2, 4), (0, 2, 4, 1, 4), (0, 4), (0, 1, 4), (0, 4, 1, 4), (0, 2, 2, 4), (0, 4, 2, 4), (0, 2, 2, 1, 4), (0, 2, 2, 4, 1, 4)]),0,
                         np.where(checkLog['updateType'].isin([(0, 2, 4, 1, 3), (0, 2, 4, 1, 4, 3), (0, 2, 1, 4, 3), (0, 4, 1, 3), (0, 1, 4, 3),
                                                                   (0, 2, 2, 4, 1, 3), (0, 2, 2, 4, 1, 4, 3), (0, 2, 2, 1, 4, 3), (0, 4, 2, 4, 1, 3),
                                                                   (0, 4, 2, 1, 3), (0, 4, 1, 4, 3), (0, 4, 1)]), 1,
                         np.where(checkLog['updateType'].isin([(0, 2, 1, 3), (0, 2, 2, 1, 3), (0, 2, 3), (0, 3), (0, 1, 3), (0, ), (0, 2), (0, 2, 1), (0, 2, 2)]), 2, 3)))
    display(checkLog[checkLog['status'] == 3].groupby('updateType')['order'].size())
    orderLog = pd.merge(orderLog, checkLog[['order', 'status']], how='left', on=['order'], validate='many_to_one')
    orderLog = orderLog[orderLog['status'].isin([0, 1, 2])].reset_index(drop=True)

    ### Assertion 8:  make sure status==0 got all traded
    print('=======================================================================================')
    print('8. status == 0: all traded')
    a = orderLog[orderLog['status'] == 0]
    a = a.groupby(['order'])[['absOrderSizeCumFilled', 'absOrderSize']].max().reset_index()
    a.columns = ['order', 'filled', 'total']
    print('in total trade, any fill != total cases')
    display(a[a['filled'] != a['total']])
    if a[a['filled'] != a['total']].shape[0] > 0:
        removeOrderLs = a[a['filled'] != a['total']]['order'].unique()
        orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]

    ### Assertion 9:  make sure status==1 got partial traded
    print('=======================================================================================')
    print('9. status == 1: partial traded')
    a = orderLog[orderLog['status'] == 1]
    a = a.groupby(['order'])[['absOrderSizeCumFilled', 'absOrderSize']].max().reset_index()
    a.columns = ['order', 'filled', 'total']
    print('in partial trade, any fill >= total or fill is 0 cases for updateType 4')
    display(a[(a['filled'] >= a['total']) | (a['filled'] == 0)])
    if a[(a['filled'] >= a['total']) | (a['filled'] == 0)].shape[0] > 0:
        removeOrderLs = a[(a['filled'] >= a['total']) | (a['filled'] == 0)]['order'].unique()
        orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]

    ### Assertion 10: make sure no cancellation within 1 sec
    print('=======================================================================================')
    print('10. no cancellation within 1 sec')
    a = orderLog[(orderLog['updateType'] == 1) & (orderLog['duration'] < 1e6)]
    print('any cancellation within 1 sec')
    display(a)
    if a.shape[0] > 0:
        removeOrderLs = a['order'].unique()
        orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]


    ### Assertion 11: make sure no order has shares > 80w or notional > 800w
    print('=======================================================================================')
    print('11. Orders with size > 80w or notional > 800w')
    orderLog['orderNtl'] = orderLog['absOrderSize'] * orderLog['orderPrice']
    if orderLog[orderLog['absOrderSize'] > 800000].shape[0] > 0:
        print('some order quantity are > 80w')
        print(orderLog[orderLog['absOrderSize'] > 800000].groupby(['colo', 'accCode'])['order'].nunique())
        display(orderLog[orderLog['absOrderSize'] > 800000][['date', 'accCode', 'secid', 'vai', 'absOrderSize', 'orderPrice',
                                                             'orderNtl', 'orderDirection', 'clock', 'order']])

    if orderLog[orderLog['orderNtl'] > 8000000].shape[0] > 0:
        print('some order ntl are > 800w')
        print(orderLog[orderLog['orderNtl'] > 8000000].groupby(['colo', 'accCode'])['order'].nunique())
        display(orderLog[orderLog['orderNtl'] > 8000000][['date', 'accCode', 'secid', 'vai', 'absOrderSize', 'orderPrice',
                                                          'orderNtl', 'orderDirection', 'clock', 'order', "updateType", 
                                                          "tradePrice", "absOrderSizeCumFilled", "absFilledThisUpdate"]])

    removeOrderLs = list(set(orderLog[orderLog['absOrderSize'] > 800000]['order'].unique()) | set(orderLog[orderLog['orderNtl'] > 8000000]['order'].unique()))
    orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]
    orderLog = orderLog.sort_values(by=['date', 'secid', 'vai', 'accCode', 'clockAtArrival']).reset_index(drop=True)
    orderLog["mrsb90"] = orderLog.groupby(['order'])['mrsb90'].transform('first')
    orderLog["mrss90"] = orderLog.groupby(['order'])['mrss90'].transform('first')
    orderLog["mrstauc"] = orderLog.groupby(['order'])['mrstauc'].transform('first')
    orderLog["mrstaat"] = orderLog.groupby(['order'])['mrstaat'].transform('first')
    orderLog["aaa"] = orderLog.groupby(['order'])['aaa'].transform('first')

    orderLog['m1'] = orderLog['mrstaat'].apply(lambda x: x - (x // 10000) * 10000)
    orderLog['m2'] = orderLog['mrstauc'].apply(lambda x: x - (x // 10000) * 10000)
    try:
        orderLog['mrsb90'] = orderLog['mrsb90'].astype(float)
    except:
        print(orderLog[orderLog['mrsb90'] == '-'])
        orderLog = orderLog[orderLog['mrsb90'] != '-']
        orderLog['mrsb90'] = orderLog['mrsb90'].astype(float)
 
    try:
        orderLog['mrss90'] = orderLog['mrss90'].astype(float)
    except:
        print(orderLog[orderLog['mrss90'] == '-'])
        orderLog = orderLog[orderLog['mrss90'] != '-']
        orderLog['mrss90'] = orderLog['mrss90'].astype(float)

    try:
        orderLog['aaa'] = orderLog['aaa'].astype(float)
    except:
        print(orderLog[orderLog['aaa'] == '-'])
        orderLog = orderLog[orderLog['aaa'] != '-']
        orderLog['aaa'] = orderLog['aaa'].astype(float)

    orderLog.loc[(orderLog['orderDirection'] >= 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrsb90']) < 1e-12), 'mrstauc'] = \
    orderLog.loc[(orderLog['orderDirection'] >= 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrsb90']) < 1e-12), 'm2']

    orderLog.loc[(orderLog['orderDirection'] >= 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrsb90']) < 1e-12), 'mrstaat'] = \
    orderLog.loc[(orderLog['orderDirection'] >= 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrsb90']) < 1e-12), 'm1']

    orderLog.loc[(orderLog['orderDirection'] < 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrss90']) < 1e-12), 'mrstauc'] = \
    orderLog.loc[(orderLog['orderDirection'] < 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrss90']) < 1e-12), 'm2']

    orderLog.loc[(orderLog['orderDirection'] < 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrss90']) < 1e-12), 'mrstaat'] = \
    orderLog.loc[(orderLog['orderDirection'] < 1) &\
             (orderLog['mrstaat'].isin([11000, 13000])) & (abs(orderLog['aaa'] - orderLog['mrss90']) < 1e-12), 'm1']  

    orderLog = orderLog.sort_values(by=['date', 'secid', 'vai', 'accCode', 'clockAtArrival']).reset_index(drop=True)


    orderLog['exchange'] = np.where(orderLog['secid'] >= 2000000, 'SZE', 'SSE')
    orderLog['orderNtl'] = orderLog['orderPrice'] * orderLog['absOrderSize']
    orderLog['tradeNtl'] = np.where(orderLog['updateType'] == 4, orderLog['tradePrice']*orderLog['absFilledThisUpdate'], 0)
    orderLog = orderLog[orderLog['secid'] >= 2000000].reset_index(drop=True)
    orderDataSZ = rawMsgDataSZ[rawMsgDataSZ['ExecType'] == '2'][['SecurityID', 'ApplSeqNum', 'clockAtArrival', 'sequenceNo', 'Side', 'OrderQty', 'Price', 'cum_volume', "TransactTime"]].reset_index(drop=True)
    orderDataSZ['updateType'] = 0
    tradeDataSZ = pd.concat([rawMsgDataSZ[rawMsgDataSZ['ExecType'] == 'F'][['SecurityID', 'BidApplSeqNum', 'clockAtArrival', 'sequenceNo', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]],
                             rawMsgDataSZ[rawMsgDataSZ['ExecType'] == 'F'][['SecurityID', 'OfferApplSeqNum', 'clockAtArrival', 'sequenceNo', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]]], sort=False)
    tradeDataSZ['ApplSeqNum'] = np.where(tradeDataSZ['BidApplSeqNum'].isnull(), tradeDataSZ['OfferApplSeqNum'], tradeDataSZ['BidApplSeqNum'])
    tradeDataSZ['Side'] = np.where(tradeDataSZ['BidApplSeqNum'].isnull(), 2, 1)
    tradeDataSZ = tradeDataSZ[['SecurityID', 'ApplSeqNum', 'clockAtArrival', 'sequenceNo', 'Side', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]]
    tradeDataSZ['updateType'] = 4
    cancelDataSZ = rawMsgDataSZ[rawMsgDataSZ['ExecType'] == '4'][['SecurityID', 'BidApplSeqNum', 'OfferApplSeqNum', 'clockAtArrival', 'sequenceNo', 'TradePrice', 'TradeQty', 'cum_volume', "TransactTime"]].reset_index(drop=True)
    cancelDataSZ['ApplSeqNum'] = np.where(cancelDataSZ['BidApplSeqNum'] == 0, cancelDataSZ['OfferApplSeqNum'], cancelDataSZ['BidApplSeqNum'])
    cancelDataSZ['Side'] = np.where(cancelDataSZ['BidApplSeqNum'] == 0, 2, 1)
    cancelDataSZ = cancelDataSZ[['SecurityID', 'ApplSeqNum', 'clockAtArrival', 'sequenceNo', 'Side', 'TradeQty', 'cum_volume', "TransactTime"]]
    cancelDataSZ['updateType'] = 3

    msgDataSZ = pd.concat([orderDataSZ, tradeDataSZ, cancelDataSZ], sort=False)
    del orderDataSZ
    del tradeDataSZ
    del cancelDataSZ
    msgDataSZ = msgDataSZ.sort_values(by=['SecurityID', 'ApplSeqNum', 'sequenceNo']).reset_index(drop=True)
    
    msgDataSZ['TradePrice'] = np.where(msgDataSZ['updateType'] == 4, msgDataSZ['TradePrice'], 0)
    msgDataSZ['TradePrice'] = msgDataSZ['TradePrice'].astype('int64')
    msgDataSZ['TradeQty'] = np.where(msgDataSZ['updateType'] == 4, msgDataSZ['TradeQty'], 0)
    msgDataSZ['TradeQty'] = msgDataSZ['TradeQty'].astype('int64')
    msgDataSZ['secid'] = msgDataSZ['SecurityID'] + 2000000
    assert(msgDataSZ['ApplSeqNum'].max() < 1e8)
    msgDataSZ['StockSeqNum'] = msgDataSZ['SecurityID']*1e8 + msgDataSZ['ApplSeqNum']
    msgDataSZ['date'] = int(thisDate)
    msgDataSZ['startVolume'] = msgDataSZ.groupby(['StockSeqNum'])['cum_volume'].transform('first')
            
    ### order insertion position
    startPos = orderLog[(orderLog['date'] == int(thisDate)) & (orderLog['updateType'] == 0) & (orderLog['secid'] >= 2000000) & (orderLog["isMsg"] == 1)]
    # here!!!!!!!!!! drop duplicates
    startPos = startPos.drop_duplicates(subset=['date', 'secid', 'vai'])
    
    startPos = startPos[((startPos.clock.dt.time >= datetime.time(9, 33)) & (startPos.clock.dt.time <= datetime.time(11, 30))) |\
                        ((startPos.clock.dt.time >= datetime.time(13, 3)) & (startPos.clock.dt.time <= datetime.time(14, 55)))]
#     startPos = startPos[startPos.clock.dt.time < datetime.time(9, 33)]
    
    startPos['SecurityID'] = startPos['secid']-2000000
    startPos['orderDirection1'] = np.where(startPos['orderDirection1'] == 1, 1, 2)
    startPos['cum_volume'] = startPos['vai']
    startPos = startPos[['SecurityID', 'cum_volume', 'orderDirection1', 'accCode', 'absOrderSize', 'vai', 'group']]
    startPos['stockGroup'] = startPos.groupby(['accCode', 'SecurityID']).grouper.group_info[0]
    startPos = startPos.sort_values(by=['SecurityID', 'cum_volume']).reset_index(drop=True)

    
    ### generate order status change data
    infoData = orderLog[(orderLog['date'] == int(thisDate)) & (orderLog['group'].isin(startPos['group'].unique())) & (orderLog['updateType'].isin([0, 3, 4]))].reset_index(drop=True)
    infoData['Price'] = infoData['orderPrice'].apply(lambda x: round(x*100, 0))
    infoData['Price'] = infoData['Price'].astype('int64')*100
    infoData['OrderQty'] = infoData['absOrderSize']
    infoData['Side'] = np.where(infoData['orderDirection1'] == 1, 1, 2)
    infoData['TradePrice'] = np.where(infoData['updateType'] == 4, round(infoData['tradePrice']*100, 0), 0)
    infoData['TradePrice'] = infoData['TradePrice'].astype('int64')*100
    statusInfo = infoData.groupby(['order'])['updateType'].apply(lambda x: tuple(x)).reset_index()
    statusInfo.columns = ['order', 'statusLs']
    tradePriceInfo = infoData.groupby(['order'])['TradePrice'].apply(lambda x: tuple(x)).reset_index()
    tradePriceInfo.columns = ['order', 'TradePriceLs']
    tradeQtyInfo = infoData.groupby(['order'])['absFilledThisUpdate'].apply(lambda x: tuple(x)).reset_index()
    tradeQtyInfo.columns = ['order', 'TradeQtyLs']
    infoData["abstradeNtl"] = infoData["absFilledThisUpdate"] * infoData["TradePrice"]
    infoData["TradeNtl"] = infoData.groupby(['order'])['abstradeNtl'].transform('sum')
    infoData = infoData[infoData['updateType'] == 0]
    infoData = pd.merge(infoData, statusInfo, how='left', on=['order'], validate='one_to_one')
    infoData = pd.merge(infoData, tradePriceInfo, how='left', on=['order'], validate='one_to_one')
    infoData = pd.merge(infoData, tradeQtyInfo, how='left', on=['order'], validate='one_to_one')
    infoData['brokerNum'] = infoData.groupby(['date', 'secid', 'vai', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs'])['colo_account'].transform('count')
    infoData['brokerLs'] = infoData.groupby(['date', 'secid', 'vai', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs'])['colo_account'].transform(lambda x: ','.join(sorted(x.unique())))
    display(infoData[(infoData["brokerNum"] >= 2) & (infoData["orderDirection"].isin([-2, 2]))]["group"].nunique())
    display(infoData[(infoData["brokerNum"] < 2) & (infoData["orderDirection"].isin([-2, 2]))]["group"].nunique())
    display(infoData[(infoData["brokerNum"] >= 2) & (infoData["orderDirection"].isin([-2, 2]))]["group"].unique())
    gl = infoData[(infoData["brokerNum"] >= 2) & (infoData["orderDirection"].isin([-2, 2]))]["group"].unique()
    gl1 = infoData["group"].unique()
    print(len(gl1))
    print(len(startPos["group"].unique()))
    infoData = infoData.drop_duplicates(subset=['date', 'secid', 'vai', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs']).reset_index(drop=True)
    infoData = infoData[['date', 'secid', 'vai', 'ars', "mrstaat", "mrstauc", 'isMsg', 'TradeNtl', 'exchange', 'group', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'brokerNum', 'brokerLs', 'order']]
    
    ### find all orders in market that inserted with us
    checkLog = msgDataSZ[msgDataSZ['updateType'] == 0][['StockSeqNum', 'SecurityID', 'cum_volume', 'sequenceNo', 'clockAtArrival', 'Side', 'OrderQty', 'Price', "TransactTime"]].reset_index(drop=True)
    checkLog = checkLog.sort_values(by=['SecurityID', 'sequenceNo'])
    checkLog = pd.merge(checkLog, startPos, how='outer', on=['SecurityID', 'cum_volume'], validate='many_to_one')
#     del startPos
    if checkLog[(~checkLog['vai'].isnull()) & (checkLog['clockAtArrival'].isnull())].shape[0] > 0:
        print('some order vai are wrong')
        removeGroup = checkLog[(~checkLog['vai'].isnull()) & (checkLog['clockAtArrival'].isnull())]['stockGroup'].unique()
        print(removeGroup)
        print(len(gl1))
        print(len(set(checkLog["group"].unique())))
        checkLog = checkLog[~checkLog['stockGroup'].isin(removeGroup)].reset_index(drop=True)
        display(len(set(gl1) - set(checkLog["group"].unique())))
    
    assert(checkLog[(~checkLog['vai'].isnull()) & (checkLog['clockAtArrival'].isnull())].shape[0] == 0)
    checkLog['lastClockInVol'] = checkLog.groupby(['SecurityID', 'cum_volume'])['clockAtArrival'].transform('last')
    checkLog['startClock'] = np.where((~checkLog['group'].isnull()) & (checkLog['clockAtArrival'] == checkLog['lastClockInVol']),
                                      checkLog['clockAtArrival'], np.nan)
    checkLog['group'] = checkLog['group'].ffill()
    checkLog['startClock'] = checkLog.groupby(['SecurityID', 'group'])['startClock'].transform('max')
    checkLog['endClock'] = checkLog['startClock'] + 20*1e3
    checkLog = pd.concat([checkLog[checkLog['clockAtArrival'] == checkLog['startClock']], 
                          checkLog[(checkLog['clockAtArrival'] > checkLog['startClock'] + 10) & 
                                   (checkLog['clockAtArrival'] <= checkLog['endClock'])]])
    checkLog = checkLog.sort_values(by=['SecurityID', 'group'])
    checkLog['vai'] = checkLog.groupby(['SecurityID', 'group'])['vai'].ffill()
    checkLog['orderDirection1'] = checkLog.groupby(['SecurityID', 'group'])['orderDirection1'].ffill()
    checkLog = pd.concat([checkLog[checkLog['clockAtArrival'] == checkLog['startClock']],
                         checkLog[(checkLog['Side'] == checkLog['orderDirection1']) & (checkLog['clockAtArrival'] != checkLog['startClock'])]]).sort_values(by=['SecurityID', 'group'])
     
    group_list = checkLog["group"].unique()

    
    checkLog = pd.merge(msgDataSZ, checkLog[['StockSeqNum', 'vai', 'group']], how='left', on=['StockSeqNum'], validate='many_to_one')
    del msgDataSZ
    checkLog = checkLog[~checkLog['group'].isnull()]
    # 上面能不能简化成inner merge!!!!!!!!!!!!!!!!!!!!!!!!!
    statusInfo = checkLog.groupby(['StockSeqNum'])['updateType'].apply(lambda x: getTuple(x)).reset_index()
    statusInfo.columns = ['StockSeqNum', 'statusLs']
    tradePriceInfo = checkLog.groupby(['StockSeqNum'])['TradePrice'].apply(lambda x: tuple(x)).reset_index()
    tradePriceInfo.columns = ['StockSeqNum', 'TradePriceLs']
    tradeQtyInfo = checkLog.groupby(['StockSeqNum'])['TradeQty'].apply(lambda x: tuple(x)).reset_index()
    tradeQtyInfo.columns = ['StockSeqNum', 'TradeQtyLs']
    checkLog = checkLog[checkLog['updateType'] == 0]
    checkLog = pd.merge(checkLog, statusInfo, how='left', on=['StockSeqNum'], validate='one_to_one')
    checkLog = pd.merge(checkLog, tradePriceInfo, how='left', on=['StockSeqNum'], validate='one_to_one')
    checkLog = pd.merge(checkLog, tradeQtyInfo, how='left', on=['StockSeqNum'], validate='one_to_one')
    
    infoData = infoData[infoData["group"].isin(group_list)]

    
    checkLog = pd.merge(checkLog, infoData, how='outer', on=['date', 'secid', 'group', 'vai', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs'], validate='many_to_one')
    display(set(gl) & set(checkLog["group"].unique()))
    display(len(set(gl1) & set(checkLog["group"].unique())))
    checkLog = checkLog.sort_values(by=['date', 'secid', 'vai', 'sequenceNo']).reset_index(drop=True)
    ### orderType 1 orders have 0 order price, replace 0 with group price
    checkLog['groupPrice'] = checkLog.groupby(['group'])['Price'].transform('median')
    checkLog['Price'] = np.where(checkLog['Price'] == 0, checkLog['groupPrice'], checkLog['Price'])
    checkLog['OrderNtl'] = checkLog['Price'] * checkLog['OrderQty'] / 10000
    
    savePath = '/data/home/zhenyu/orderLog/result/marketPos'
    savePath = os.path.join(savePath, 'marketPos1_%s.pkl'%thisDate)
    with open(savePath, 'wb') as f:
        pickle.dump(checkLog.reset_index(drop=True), f, protocol=4)
    del checkLog
    
    print(datetime.datetime.now() - startTm)

    
from twisted.internet import task, reactor
import schedule


def sleeptime(hour,min,sec):
    return hour*3600 + min*60 + sec

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



import tarfile
import os
import numpy as np
import glob
import shutil
import datetime
def un_gz(file_name, dirs):
    t = tarfile.open(file_name)
    dirs = dirs + '/' + os.path.basename(file_name).split('.')[0]
    try:
        os.mkdir(dirs)
    except:
        'The directory has already been created'
    t.extractall(path=dirs)
    da_te = os.path.basename(file_name).split('_')[1]
    readPath = dirs + '/md***' + da_te + '***'
    dataPathLs = np.array(glob.glob(readPath))
    readPath = dirs + '/***'
    dataPathLs1 = np.array(glob.glob(readPath))
    for i in list(set(dataPathLs1) - set(dataPathLs)):
        if os.path.exists(i):
            if os.path.isfile(i):
                os.remove(i)
            if os.path.isdir(i):
                shutil.rmtree(i) 
    for i in dataPathLs:
        if os.path.getsize(i) < 1000000000:
            os.remove(i)
    print('finish ' + da_te)
    

class Test(object):
    def __init__(self):
        self.status = True        
    def test(self):
        while self.status == True:
            try:
                print(datetime.datetime.now())
                date = '20210113'
                readPath = '/data/home/zhenyu/equityTradeLogs'
                dataPathLs = np.array(glob.glob(os.path.join(readPath, 'speedCompare***.csv')))
                dateLs = np.array([int(os.path.basename(i).split('_')[1].split('.')[0]) for i in dataPathLs])
                assert(np.max(dateLs) == int(date))
                print('orderLog is ready')
                
                readPath = '/data/home/zhenyu/dailyRawData/' + date + '/***zs_92_01***'
                dataPathLs = np.array(glob.glob(readPath))
                assert(len(dataPathLs) == 1)
                dateLs = np.array(glob.glob('/data/home/zhenyu/Kevin_zhenyu/rawData/***'))
                dateLs = [int(os.path.basename(i).split('_')[1]) for i in dateLs]
                if np.max(dateLs) < int(date):
                    un_gz(dataPathLs[0], '/data/home/zhenyu/Kevin_zhenyu/rawData')
                print('market data is ready')
                
                database_name = 'com_md_eq_cn'
                user = 'zhenyuy'
                password = 'bnONBrzSMGoE'
                db1 = DB("192.168.10.178", database_name, user, password)
                beta = db1.read_daily('mktbeta', start_date=int(date), end_date=int(date))
                assert(beta.shape[0] != 0)
                print('beta data is ready')
                
                print('We start to generate data now')  
                download_data(date)
                self.status = False
            except Exception as e:
                print(e)
                print('still wait for data coming')
                second = sleeptime(0,5,0)
                time.sleep(second)

test1 = Test()
schedule.every().day.at("05:16").do(test1.test)
while True:
    schedule.run_pending()
    time.sleep(1)
