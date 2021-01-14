import numpy as np
import os
import glob
import datetime
import numpy as np
import pandas as pd
import time
import pickle


dd = '20201216'
print('start')
readPath = '/data/home/zhenyu/orderLog/data/***'
dataPathLs = np.array(glob.glob(readPath))
path = np.sort(dataPathLs)[-11:]
cur = np.max([int(os.path.basename(i).split('.')[0]) for i in path])
assert(cur == int(dd))


body = "<html><body><div>" + 'Hi all,<p>The following is daily report based on ' + str(cur) + ' data.</p>'
body += '<p><b>I. Assertions</p></b>'
count = 0

# load data
print('-----------------------------------------------------------------------------------------------')
print('load data')
rawOrderLog = []
for thisPath in path:
    data = pickle.load(open(thisPath, 'rb'))
    data = data.rename(columns={'mdClockAtArrival': 'caamd'})
    rawOrderLog += [data]
rawOrderLog = pd.concat(rawOrderLog, sort=False)

for col in ['clockAtArrival', 'caamd', 'secid', 'updateType', 'vai', 'absFilledThisUpdate', 'orderDirection', 'absOrderSize',
            'absOrderSizeCumFilled', 'date', 'accCode', 'mse']:
    rawOrderLog[col] = rawOrderLog[col].fillna(0).astype('int64')   
rawOrderLog = rawOrderLog.sort_values(by=['date', 'secid', 'vai', 'accCode', 'clockAtArrival']).reset_index(drop=True)

num1 = rawOrderLog[rawOrderLog['secid'] < 1000000].shape[0]
if num1 != 0:
    count += 1
    body += str(count) + '. There are in total ' + str(num1) + ' ticks with secid < 1000000.<div>'
rawOrderLog = rawOrderLog[rawOrderLog["secid"] >= 1000000]


if rawOrderLog[rawOrderLog.duplicated(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                    'orderDirection', 'absOrderSize'], keep=False)].shape[0] != 0:
    display('There are accounts with duplicated ticks:')
    display(rawOrderLog[rawOrderLog.duplicated(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                    'orderDirection', 'absOrderSize'], keep=False)]\
.groupby(['date', 'colo', 'accCode'])['ars'].size())
    rawOrderLog = rawOrderLog.drop_duplicates(['date', 'secid', 'vai', 'accCode', 'clockAtArrival', 'updateType', \
                                    'orderDirection', 'absOrderSize'], keep='first')

display('There are ticks with orderDirection 0')
display(rawOrderLog[rawOrderLog['orderDirection'] == 0][['date', 'colo', 'accCode', \
            'secid', 'vai', 'updateType', 'sdd', 'orderDirection', 'absOrderSize', 'internalId', 'orderId']])
try:
    num1 = rawOrderLog[(rawOrderLog['orderDirection'] == 0) & (rawOrderLog['date'] == cur)].shape[0]
    assert(num1 < 30)
except:
    count += 1
    body += str(count) + '. There are in total ' + str(num1) + ' ticks with orderDirection 0.<div>'



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
assert(sum(r1[r1['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1) == 0)
try:
    assert(sum(r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1) == 0)
except:
    a = r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique()[r2[r2['updateType'] != 0].groupby(['test', 'vai'])['orderId'].nunique() != 1].reset_index()
    print(pd.merge(r2, a[['test', 'vai']], on=['test', 'vai'], how='inner')[['secid', 'accCode', 'colo', 'vai', 'updateType', 'sdd', 'internalId', 'orderId', 'absOrderSize', 'absFilledThisUpdate', 'absOrderSizeCumFilled', 'orderPrice', 'tradePrice']])
orderLog = pd.concat([r1, r2])
del r1
del r2    

orderLog['clock'] = orderLog['clockAtArrival'].apply(lambda x: datetime.datetime.fromtimestamp(x/1e6))
orderLog['broker'] = orderLog['accCode'] // 100
orderLog["broker"] = np.where(orderLog["accCode"].astype(str).apply(lambda x: len(x) == 6), orderLog['accCode'] // 10000, orderLog["broker"])
orderLog['colo_broker'] = orderLog['colo'].str[:2] + '_' + orderLog['broker'].astype('str')
orderLog['order'] = orderLog.groupby(['date', 'colo', 'accCode', 'secid', 'vai']).grouper.group_info[0]
orderLog['group'] = orderLog.groupby(['date', 'secid', 'vai']).grouper.group_info[0]
orderLog['startClock'] = orderLog.groupby(['order'])['clockAtArrival'].transform('first')
orderLog['duration'] = orderLog['clockAtArrival'] - orderLog['startClock']
orderLog['orderPrice'] = orderLog['orderPrice'].apply(lambda x: round(x, 2))
orderLog['tradePrice'] = orderLog['tradePrice'].apply(lambda x: round(x, 2))
orderLog['orderDirection1'] = np.where(orderLog["orderDirection"] == -2, -1, np.where(
    orderLog["orderDirection"] == 2, 1, orderLog["orderDirection"]))
orderLog["ars"] = orderLog.groupby(['order'])['ars'].transform('first')


### Assertion 1:  make sure same direction in same date, secid, vai
print('=======================================================================================')
print('1. same date, secid, vai: same direction')
orderLog['directNum'] = orderLog.groupby(['date', 'secid', 'vai'])['orderDirection1'].transform('nunique')
if len(orderLog[orderLog['directNum'] != 1]) > 0:
    print('opposite direction for same date, same secid, same vai')
    display(orderLog[(orderLog['directNum'] != 1) & (orderLog['updateType'] == 0)].groupby(['date'])['orderDirection'].size())
    display(orderLog[(orderLog['directNum'] != 1) & (orderLog['updateType'] == 0)].groupby(['date', 'accCode'])['orderDirection'].size())
    try:
        num1 = orderLog[(orderLog['directNum'] != 1) & (orderLog['updateType'] == 0) & (orderLog['date'] == cur)].shape[0]
        assert(num1 < 200)
    except:
        count += 1
        body += str(count) + '. There are in total ' + str(num1) + ' orders with opposite directions under same date, secid, vai.<div>'
    try:
        num2 = list(orderLog[(orderLog['directNum'] != 1) & (orderLog['updateType'] == 0) & (orderLog['date'] == \
            cur)].groupby('accCode')['date'].size()[orderLog[(orderLog['directNum'] != 1) & \
           (orderLog['updateType'] == 0) & (orderLog['date'] == cur)].\
                                                                       groupby('accCode')['date'].size() > 50].index)
        assert(len(num2) == 0)
    except:
        count += 1
        num2 = ', '.join([str(x) for x in num2])
        body += str(count) + '. ' + num2 + ' has more than 50 orders with opposite directions under same date, secid, vai.<div>'
    orderLog = orderLog[orderLog['directNum'] == 1]

assert((orderLog.groupby(['date', 'secid', 'vai'])['orderDirection1'].nunique() == 1).all() == True)

## Assertion 2:  make sure each account, secid, vai only has one insertion
print('=======================================================================================')
print('2. same date, secid, vai, accCode: one insertion')
a = orderLog[orderLog['updateType'] == 0].groupby(['date', 'accCode', 'secid', 'vai', 'order'])['clockAtArrival'].count()
if len(a[a > 1]) > 0:
    print('more than one insertion at same time')
    a = a[a>1].reset_index()
    display(a)
    display(a.groupby(['date'])['accCode'].size())
    display(a.groupby(['date', 'accCode'])['order'].size())
    try:
        num1 = a[a['date'] == cur].shape[0]
        assert(num1 < 60)
    except:
        count += 1
        body += str(count) + '. There are in total ' + str(num1) + ' orders with more than one insertion in same order.<div>'
    try:
        num2 = list(a[a['date'] == cur].groupby(['accCode'])['date'].size()[a[a['date'] == cur].groupby(['accCode'])['date'].size() > 15].index)
        assert(len(num2) == 0)
    except:
        count += 1
        num2 = ', '.join([str(x) for x in num2])
        body += str(count) + '. ' + num2 + ' has more than 15 orders with more than one insertion in same order.<div>'        

    orderLog = orderLog[~(orderLog['order'].isin(a['order'].unique()))]

orderLog['isMsg'] = np.where(orderLog['updateType'] == 0, 
                             np.where(orderLog['mse'] == 100, 1, 0), np.nan)
orderLog['isMsg'] = orderLog.groupby(['order'])['isMsg'].ffill()


### Assertion 3:  check IPO stocks selling status
print('=======================================================================================')
print('3. IPO stocks selling (ars = 301, 302)')
if orderLog[(orderLog['ars'].isin([301, 302])) & (orderLog['date'] == cur)].shape[0] != 0:
    kk = orderLog[(orderLog['ars'].isin([301, 302])) & (orderLog['date'] == cur)]
    print(kk)
    try:
        assert(kk[kk['orderDirection1'] == 1].shape[0] == 0)
        print('we only sell, never buy')
    except:
        print('There are IPO buy side orders!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        count += 1
        num1 = kk[kk['orderDirection1'] == 1].shape[0]
        body += str(count) + '. There are ' + str(num1) + ' IPO buy side orders.<div>'
        print(kk[kk['orderDirection1'] == 1])
    kk1 = kk[kk['updateType'] == 0]
    kk1 = kk1.sort_values(by=['accCode', 'secid','clockAtArrival'])
    kk1['diff'] = kk1.groupby(['accCode', 'secid'])['clockAtArrival'].apply(lambda x: x-x.shift(1))
    kk1['diff'] = kk1['diff'].fillna(0)
    try:
        assert(kk1[kk1['diff'] < 10e6].shape[0] == 0)
        print('for each stock in the same account, there is no insertion within 10 seconds of the previous insertion')
    except:
        count += 1
        kk1 = kk1.reset_index()
        num2 = kk1[kk1['diff'] < 10e6].shape[0]
        body += str(count) + '. There are ' + str(num2) + ' over ' + str(kk1.shape[0]) + ' orders with insertion within 10 seconds for orders under same account same stock.<div>'
        print('There are insertion within 10 seconds for orders under same account same stock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')        
        print(kk1[kk1['diff'] < 10e6])
    kk2 = kk[(kk['updateType'] == 1)]

    try:
        assert(kk2[kk2['duration'] < 3e6].shape[0] == 0)
        print('for each stock in the same account, the cancellation of an order happens more than 3 seconds after the insertion')
    except:
        count += 1
        num2 = kk2[kk2['duration'] < 3e6].shape[0]
        body += str(count) + '. There are ' + str(num2) + ' over ' + str(kk1.shape[0]) + ' orders with cancellation within 3 seconds after insertion.<div>'        
        print('There are cancellation within 3 seconds for orders under same account same stock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print(kk2[kk2['duration'] < 3e6])


### Assertion 4: check updateType == 7 orders, make sure updateType == 7 orders < 20 per account, < 100 in total
print('=======================================================================================')
print('4. updateType 7 orders')
if orderLog[(orderLog['updateType'] == 7) & (orderLog['date'] == cur)].shape[0] != 0:
    try:
        assert(orderLog[(orderLog['updateType'] == 7) & (orderLog['date'] == cur)].groupby(['date', 'accCode'])['order'].nunique().max() < 20)
    except:
        print('There are more than 20 updateType 7 orders per account')
        count += 1
        a = list(orderLog[(orderLog['updateType'] == 7) & (orderLog['date'] == cur)].groupby(['accCode'])['order'].nunique()[
            orderLog[(orderLog['updateType'] == 7) & (orderLog['date'] == cur)].groupby(['accCode'])['order'].nunique() >= 20
        ].index)
        a = ', '.join([str(x) for x in a])
        body += str(count) + '. ' + a + ' has more than 20 updateType 7 orders.<div>'
    try:      
        assert(orderLog[(orderLog['updateType'] == 7) & (orderLog['date'] == cur)]['order'].nunique() < 100)
    except:
        print('Ther are more than 100 updateType 7 orders in total')
        count += 1
        body += str(count) + '. There are more than 100 updateType 7 orders in total.<div>'


### Assertion 5: check updateType == 6 orders, make sure updateType == 6 orders < 5% per account
print('=======================================================================================')
print('5. updateType 6 orders')
k1 = orderLog[(orderLog['updateType'] == 6) & (orderLog['date'] == cur)].groupby(['accCode'])['order'].nunique().reset_index()
k2 = orderLog[(orderLog['date'] == cur)].groupby(['accCode'])['order'].nunique().reset_index()
k = pd.merge(k1, k2, on=['accCode'], how='left')
k['prob'] = k['order_x']/k['order_y']
try:
    assert(sum(k['prob'] >= 0.05) == 0)
except:
    print('There are accounts with more than 5% updateType 6 orders!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print(k[k['prob'] >= 0.05])
    a = k[k['prob'] >= 0.05]['accCode'].unique()
    a = ', '.join([str(x) for x in a])
    count += 1
    body += str(count) + '. ' + a + ' has more than 5% updateType 6 orders.<div>'

### Assertion 6: check CYB orders, make sure all CYB stocks have absOrderSize < 30w
print('=======================================================================================')
print('6. CYB stocks order size < 30w')
try:
    cyb = orderLog[(orderLog['secid'] >= 2300000) & (orderLog['updateType'] == 0) & (orderLog['date'] == cur)]
    assert(cyb[cyb['absOrderSize'] > 300000].shape[0] == 0)
except:
    print('CYB stocks total absOrderSize >= 30w!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    num1 = cyb[cyb['absOrderSize'] > 300000].shape[0]
    count += 1
    body += str(count) + '. There are ' + str(num1) + ' orders with CYB absOrderSize > 30w.<div>'


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

orderLog = pd.merge(orderLog, checkLog[['order', 'status']], how='left', on=['order'], validate='many_to_one')
orderLog = orderLog[orderLog['status'].isin([0, 1, 2])].reset_index(drop=True)

### Assertion 8:  make sure status==0 got all traded
print('=======================================================================================')
print('8. status == 0: all traded')
a = orderLog[(orderLog['status'] == 0)]
a = a.groupby(['date', 'order'])[['absOrderSizeCumFilled', 'absOrderSize']].max().reset_index()
a.columns = ['date', 'order', 'filled', 'total']
print('in total trade, any fill != total cases')
display(a[a['filled'] != a['total']])
if a[a['filled'] != a['total']].shape[0] > 0:
    removeOrderLs = a[a['filled'] != a['total']]['order'].unique()
    count += 1
    body += str(count) + '. There are ' + str(a[(a['filled'] != a['total']) & (a['date'] == cur)]['order'].nunique()) + \
    ' over ' + str(a[(a['date'] == cur)]['order'].nunique()) + ' orders status == 0 but not all traded.<div>'
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
    count += 1
    body += str(count) + '. There are ' + str(a[((a['filled'] >= a['total']) | (a['filled'] == 0)) & (a['date'] == cur)]['order'].nunique()) + \
    ' over ' + str(a[(a['date'] == cur)]['order'].nunique()) + ' orders status == 1 but not partial traded.<div>'
    orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]

### Assertion 10: make sure no cancellation within 1 sec
print('=======================================================================================')
print('10. no cancellation within 1 sec')
a = orderLog[(orderLog['updateType'] == 1) & (orderLog['duration'] < 1e6)]
print('any cancellation within 1 sec')
display(a)
if a[a['date'] == cur].shape[0] > 0:
    count += 1
    body += str(count) + '. There are ' + str(a[(a['date'] == cur)]['order'].nunique()) + ' orders cancel within 1s.<div>'    
if a.shape[0] > 0:
    removeOrderLs = a['order'].unique()
    orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]


### Assertion 11: make sure no order has shares > 80w or notional > 800w
print('=======================================================================================')
print('11. Orders with size > 80w or notional > 800w')
orderLog['orderNtl'] = orderLog['absOrderSize'] * orderLog['orderPrice']
if orderLog[(orderLog['absOrderSize'] > 800000) & (orderLog['date'] == cur)].shape[0] > 0:
    count += 1
    body += str(count) + '. There are ' + str(orderLog[(orderLog['absOrderSize'] > 800000) & (orderLog['date'] == cur)]['order'].nunique()) + ' orders shares > 80w.<div>'    
if orderLog[orderLog['absOrderSize'] > 800000].shape[0] > 0:
    print('some order quantity are > 80w')
    print(orderLog[orderLog['absOrderSize'] > 800000].groupby(['colo', 'accCode'])['order'].nunique())
    display(orderLog[orderLog['absOrderSize'] > 800000][['date', 'accCode', 'secid', 'vai', 'absOrderSize', 'orderPrice',
                                                         'orderNtl', 'orderDirection', 'clock', 'order']])
if orderLog[(orderLog['orderNtl'] > 8000000) & (orderLog['date'] == cur)].shape[0] > 0:
    count += 1
    body += str(count) + '. There are ' + str(orderLog[(orderLog['orderNtl'] > 8000000) & (orderLog['date'] == cur)]['order'].nunique()) + ' orders notional > 800w.<div>'                
if orderLog[orderLog['orderNtl'] > 8000000].shape[0] > 0:
    print('some order ntl are > 800w')
    print(orderLog[orderLog['orderNtl'] > 8000000].groupby(['colo', 'accCode'])['order'].nunique())
    display(orderLog[orderLog['orderNtl'] > 8000000][['date', 'accCode', 'secid', 'vai', 'absOrderSize', 'orderPrice',
                                                      'orderNtl', 'orderDirection', 'clock', 'order', "updateType", 
                                                      "tradePrice", "absOrderSizeCumFilled", "absFilledThisUpdate"]])

removeOrderLs = list(set(orderLog[orderLog['absOrderSize'] > 800000]['order'].unique()) | set(orderLog[orderLog['orderNtl'] > 8000000]['order'].unique()))
orderLog = orderLog[~(orderLog['order'].isin(removeOrderLs))]


orderLog = orderLog.sort_values(by=['date', 'secid', 'vai', 'accCode', 'clockAtArrival']).reset_index(drop=True)

orderLog['exchange'] = np.where(orderLog['secid'] >= 2000000, 'SZE', 'SSE')
orderLog['orderNtl'] = orderLog['orderPrice'] * orderLog['absOrderSize']
orderLog['tradeNtl'] = np.where(orderLog['updateType'] == 4, orderLog['tradePrice']*orderLog['absFilledThisUpdate'], 0)
orderLog["mrstaat"] = orderLog.groupby(['order'])['mrstaat'].transform('first')
orderLog["ars"] = orderLog.groupby(['order'])['ars'].transform('first')
orderLog["mrstauc"] = orderLog.groupby(['order'])['mrstauc'].transform('first')
orderLog["mrsb90"] = orderLog.groupby(['order'])['mrsb90'].transform('first')
orderLog["mrss90"] = orderLog.groupby(['order'])['mrss90'].transform('first')
orderLog["aaa"] = orderLog.groupby(['order'])['aaa'].transform('first')
orderLog = orderLog[~orderLog['ars'].isnull()]
# orderLog = orderLog[orderLog['ars'] % 10 == 1]


orderLog['m1'] = orderLog['mrstaat'].apply(lambda x: x - (x // 10000) * 10000)
orderLog['m2'] = orderLog['mrstauc'].apply(lambda x: x - (x // 10000) * 10000)
if orderLog[orderLog['mrsb90'] == '-'].shape[0] != 0:
    display(orderLog[orderLog['mrsb90'] == '-'])
orderLog = orderLog[orderLog['mrsb90'] != '-']
orderLog['mrsb90'] = orderLog['mrsb90'].astype(float)
if orderLog[orderLog['aaa'] == '-'].shape[0] != 0:
    display(orderLog[orderLog['aaa'] == '-'])
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


orderLog['sta'] = np.where(orderLog['mrstaat'] == 1000, '1. staone', np.where(
orderLog['mrstaat'] == 3000, '2. statwo', np.where(
orderLog['mrstaat'].isin([11000, 13000]), '3. sta300', 'else')))
display(orderLog[(orderLog['sta'] == 'else') & (orderLog['updateType'] == 0)].groupby(['date', 'accCode'])['secid'].size())
display(orderLog[(orderLog['sta'] == 'else') & (orderLog['date'] == cur) & (orderLog['updateType'] == 0)])
m1 = orderLog[(orderLog['sta'] == 'else') & (orderLog['date'] == cur) & (orderLog['updateType'] == 0)].shape[0]
if m1 != 0:
    count += 1
    body += str(count) + '. There are ' + str(m1) + ' orders with invalid strategy.<div>'                    
orderLog = orderLog[orderLog['mrstaat'].isin([11000, 13000, 1000, 3000])]
display(orderLog[orderLog['updateType'] == 0].groupby(['mrstaat', 'mrstauc'])['date'].size())

 



  
body += '<p><b>II. fill rate</b></p>'

import pandas as pd
def convertToHtml(result,title):
    #将数据转换为html的table
    #result是list[list1,list2]这样的结构
    #title是list结构；和result一一对应。titleList[0]对应resultList[0]这样的一条数据对应html表格中的一列
    d = {}
    index = 0
    for t in title:
        d[t]=result[index]
        index = index+1
    df = pd.DataFrame(d)
    df = df[title]
    h = df.to_html(index=False)
    return h

placeSZE = orderLog[(orderLog['secid'] >= 2000000) & (orderLog['updateType'] == 0)]
print('%.2f%% SZE orders triggered by msg data'%(placeSZE[placeSZE['isMsg'] == 1].shape[0]/placeSZE.shape[0]*100))
body += '%.2f%% SZE orders triggered by msg data<div>'%(placeSZE[placeSZE['isMsg'] == 1].shape[0]/placeSZE.shape[0]*100)
placeSZE = orderLog[(orderLog['secid'] >= 2000000) & (orderLog['updateType'] == 0)]
r1 = placeSZE[placeSZE['isMsg'] == 1].groupby(['date', 'accCode'])['secid'].size().reset_index()
r2 = placeSZE.groupby(['date', 'accCode'])['secid'].size().reset_index()
re = pd.merge(r1, r2, on=['date', 'accCode'], how='inner')
re['perc'] = re['secid_x'] / re['secid_y']
print(re[re['perc'] < 0.8])
for i in re[re['perc'] < 0.8]['accCode'].unique():
    a = list(re[(re['perc'] < 0.8) & (re['accCode'] == i)]['date'].unique())
    a = ', '.join([str(x) for x in a])
    body += 'accCode ' + str(i) + ' SZE orders on ' + a + ' has msg triggered percentage < 80%<div>'

orderLog['tag'] = 'previous'
orderLog.loc[orderLog['date'] == cur, 'tag'] = 'current'
o1 = orderLog[orderLog['updateType'] == 0].groupby(['tag', 'exchange'])['orderNtl'].sum().reset_index()
o2 = orderLog[orderLog['updateType'] == 4].groupby(['tag', 'exchange'])['tradeNtl'].sum().reset_index()
o = pd.merge(o1, o2, on=['tag', 'exchange'])
o['perc'] = o['tradeNtl'] / o['orderNtl']
o['perc'] = o['perc'].apply(lambda x: '%.f'%(x*100))
o1 = o[o['tag'] == 'current']
o1 = o1.rename(columns={'perc':'cur_perc'})
o2 = o[o['tag'] == 'previous']
o2 = o2.rename(columns={'perc':'prev_perc'})
re = pd.merge(o1[['exchange', 'cur_perc']], o2[['exchange', 'prev_perc']], on='exchange')
from IPython.display import display, HTML
display(HTML(re.groupby(['exchange']).first().to_html()))
result = [re['exchange'].values, re['cur_perc'].values, re['prev_perc'].values]
title=['exchange', 'cur_perc', 'prev_perc']
body += convertToHtml(result,title)

orderLog['num'] = orderLog.groupby(['exchange', 'colo', 'accCode', 'secid'])['tag'].transform('nunique')
checkLog = orderLog[orderLog['num'] == 2]
# checkLog = orderLog
d1 = checkLog[(checkLog['updateType'] == 0)].groupby(['tag', 'exchange', 'sta', 'colo', 'accCode'])['orderNtl'].sum().reset_index()
d2 = checkLog[(checkLog['updateType'] == 4)].groupby(['tag', 'exchange', 'sta', 'colo', 'accCode'])['tradeNtl'].sum().reset_index()
dd = pd.merge(d1, d2, on=['tag', 'exchange', 'sta', 'colo', 'accCode'])
dd['fill rate'] = dd['tradeNtl'] / dd['orderNtl']
add = checkLog[(checkLog['updateType'] == 0)].groupby(['tag', 'exchange', 'sta', 'colo', 'accCode'])['order'].nunique().reset_index()
add = add.rename(columns={'order':'num'})
dd = pd.merge(dd, add, on=['tag', 'exchange', 'sta', 'colo', 'accCode'])
prev = dd[dd['tag'] == 'previous']
prev = prev.rename(columns={'num':'prev_num', 'fill rate':'prev_fillRate'})
cur = dd[dd['tag'] == 'current']
cur = cur.rename(columns={'num':'cur_num', 'fill rate':'cur_fillRate'})
report = pd.merge(prev[['exchange', 'sta', 'colo', 'accCode', 'prev_num', 'prev_fillRate']], 
              cur[['exchange', 'sta', 'colo', 'accCode', 'cur_num', 'cur_fillRate']], on=['exchange', 'sta', 'colo', 'accCode'], how='inner')

a1 = checkLog[(checkLog['updateType'] == 0) & (checkLog['tag'] == 'previous')].groupby(['date', 'exchange', 'sta', 'colo', 'accCode'])['orderNtl'].sum().reset_index()
a2 = checkLog[(checkLog['updateType'] == 4) & (checkLog['tag'] == 'previous')].groupby(['date', 'exchange', 'sta', 'colo', 'accCode'])['tradeNtl'].sum().reset_index()
aa = pd.merge(a1, a2, on=['date', 'exchange', 'sta', 'colo', 'accCode'])
aa['fill rate'] = aa['tradeNtl'] / aa['orderNtl']
aa['count'] = aa.groupby(['exchange', 'sta', 'colo', 'accCode'])['date'].transform('nunique')
aa1 = aa.groupby(['exchange', 'sta', 'colo', 'accCode'])['fill rate'].describe()[['mean', 'std']].reset_index()
aa1 = aa1.fillna(0)
aa = pd.merge(aa, aa1, on=['exchange', 'sta', 'colo', 'accCode'])
for j in [1]:
    aa[str(j) + 'std_low'] = aa['mean'] - j*aa['std']
    aa[str(j) + 'std_high'] = aa['mean'] + j*aa['std']
    aa['count1'] = np.where((aa['fill rate'] <= aa[str(j) + 'std_high']) & (aa['fill rate'] >= aa[str(j) + 'std_low']), 1, 0)
    re1 = aa.groupby(['exchange', 'sta', 'colo', 'accCode'])['count1'].sum().reset_index()
    re2 = aa.groupby(['exchange', 'sta', 'colo', 'accCode'])['count'].first().reset_index()
    re1 = pd.merge(re2, re1, on=['exchange', 'sta', 'colo', 'accCode'])
    re1['count1'] = re1['count1'] / re1['count']
    re1 = re1.rename(columns={'count1':str(j)+'*std'})
for j in [1.5, 2, 2.5, 3, 3.5, 4, 4.5]:
    aa[str(j) + 'std_low'] = aa['mean'] - j*aa['std']
    aa[str(j) + 'std_high'] = aa['mean'] + j*aa['std']
    aa['count1'] = np.where((aa['fill rate'] <= aa[str(j) + 'std_high']) & (aa['fill rate'] >= aa[str(j) + 'std_low']), 1, 0)
    re = aa.groupby(['exchange', 'sta', 'colo', 'accCode'])['count1'].sum().reset_index()
    re2 = aa.groupby(['exchange', 'sta', 'colo', 'accCode'])['count'].first().reset_index()
    re = pd.merge(re2, re, on=['exchange', 'sta', 'colo', 'accCode'])
    re['count1'] = re['count1'] / re['count']
    re = re.rename(columns={'count1':str(j)+'*std'})
    re1 = pd.merge(re1, re[['exchange', 'sta', 'colo', 'accCode', str(j)+'*std']], on=['exchange', 'sta', 'colo', 'accCode'])
nc = []
for i in range(0, re1.shape[0]):
    nc.append(np.float(re1.columns[5:][(re1.iloc[i, 5:] == 1)][0].split('*')[0]))
re1['n'] = nc
display(re1.shape[0])
display(aa1.shape[0])
aa1 = pd.merge(aa1, re1[['exchange', 'sta', 'colo', 'accCode', 'n']], on=['exchange', 'sta', 'colo', 'accCode'])
aa1['min'] = aa1['mean'] - aa1['std'] * aa1['n']
aa1['max'] = aa1['mean'] + aa1['std'] * aa1['n']
report = pd.merge(report, aa1, on=['exchange', 'sta', 'colo', 'accCode'], how='left')
assert(report[report['mean'].isnull()].shape[0] == 0)
display(report[((report['cur_fillRate'] > report['max']) | (report['cur_fillRate'] < report['min'])) & (report['cur_num'] > 100) & (abs(report['cur_fillRate'] - report['prev_fillRate']) > 0.15)].groupby(['exchange', 'sta', 'colo', 'accCode'])['prev_fillRate', 'cur_fillRate'].first())
report = report[((report['cur_fillRate'] > report['max']) | (report['cur_fillRate'] < report['min'])) & (report['cur_num'] > 100) & (abs(report['cur_fillRate'] - report['prev_fillRate']) > 0.15)].groupby(['exchange', 'sta', 'colo', 'accCode'])['prev_fillRate', 'cur_fillRate'].first().reset_index()

for cols in ['prev_fillRate', 'cur_fillRate']:
    report[cols] = report[cols].apply(lambda x: '%.f%%'%(100*x))
from IPython.display import display, HTML
display(HTML(report.groupby(['exchange', 'sta', 'colo', 'accCode']).first().to_html()))
body += '<div>In the following cases, fill rate under given accCode pass the hurdle we set:'
result = [report['exchange'].values, report['sta'].values, report['colo'].values, report['accCode'].values, 
         report['prev_fillRate'].values, report['cur_fillRate'].values]
title = ['exchange', 'sta', 'colo', 'accCode', 'prev_fillRate', 'cur_fillRate']
body += convertToHtml(result,title)





body += '<p><b>III. internal latency</b></p>'

orderLog['num'] = orderLog.groupby(['exchange', 'colo', 'accCode', 'secid'])['tag'].transform('nunique')
checkLog = orderLog[(orderLog["updateType"] == 0) & (orderLog['num'] == 2)]
checkLog = checkLog[checkLog['caamd'] != 0]
checkLog['internal_latency'] = checkLog["clockAtArrival"] - checkLog["caamd"]
SZE = checkLog[checkLog['secid'] >= 2000000]
SSE = checkLog[checkLog['secid'] < 2000000]
SZE = SZE[SZE['isMsg'] == 1]
c1 = SZE.groupby(['tag', "exchange", "colo", "accCode", "sta", "isMsg"])["internal_latency"].quantile(.95).reset_index()
c2 = SZE.groupby(['tag', "exchange", "colo", "accCode", "sta", "isMsg"])["internal_latency"].median().reset_index()
c3 = SZE.groupby(['tag', "exchange", "colo", "accCode", "sta", "isMsg"])["internal_latency"].count().reset_index()

re1 = pd.merge(c3, c1, on=['tag', "exchange", "colo", "accCode", "sta", "isMsg"])
re1 = re1.rename(columns = {'internal_latency_x': 'count', 'internal_latency_y': '95 percentile'})
re1 = pd.merge(re1, c2, on=['tag', "exchange", "colo", "accCode", "sta", "isMsg"])
re1 = re1.rename(columns = {'internal_latency': 'median'})
re1['isMsg'] = 1

c1 = SSE.groupby(['tag', "exchange", "colo", "accCode", "sta", "isMsg"])["internal_latency"].quantile(.95).reset_index()
c2 = SSE.groupby(['tag', "exchange", "colo", "accCode", "sta", "isMsg"])["internal_latency"].median().reset_index()
c3 = SSE.groupby(['tag', "exchange", "colo", "accCode", "sta", "isMsg"])["internal_latency"].count().reset_index()

re2 = pd.merge(c3, c1, on=['tag', "exchange", "colo", "accCode", "sta", "isMsg"])
re2 = re2.rename(columns = {'internal_latency_x': 'count', 'internal_latency_y': '95 percentile'})
re2 = pd.merge(re2, c2, on=['tag', "exchange", "colo", "accCode", "sta", "isMsg"])
re2 = re2.rename(columns = {'internal_latency': 'median'})
re2

re = pd.concat([re1, re2]).reset_index(drop=True)

for col in ['isMsg','median', '95 percentile']:
    re[col] = re[col].astype(int)

re1 = re[re['tag'] == 'current']
re1 = re1.rename(columns={'count':'cur_count', 'median':'cur_med', '95 percentile':'cur_95p'})
re2 = re[re['tag'] == 'previous']
re2 = re2.rename(columns={'count':'prev_count', 'median':'prev_med', '95 percentile':'prev_95p'})
report = pd.merge(re1, re2, on=['exchange', 'colo', 'accCode', 'sta', 'isMsg'])

checkLog[(checkLog['exchange'] == 'SSE') | ((checkLog['exchange'] == 'SZE') & (checkLog['isMsg'] == 1))]
aa = checkLog[(checkLog['tag'] == 'previous')].groupby(['date', 'exchange', 'sta', 'colo', 'accCode', 'isMsg'])['internal_latency'].median().reset_index()
aa['count'] = aa.groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['date'].transform('nunique')
aa1 = aa.groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['internal_latency'].describe()[['mean', 'std']].reset_index()
aa1 = aa1.fillna(0)
aa = pd.merge(aa, aa1, on=['exchange', 'sta', 'colo', 'accCode', 'isMsg'])
for j in [1]:
    aa[str(j) + 'std_low'] = aa['mean'] - j*aa['std']
    aa[str(j) + 'std_high'] = aa['mean'] + j*aa['std']
    aa['count1'] = np.where((aa['internal_latency'] <= aa[str(j) + 'std_high']) & (aa['internal_latency'] >= aa[str(j) + 'std_low']), 1, 0)
    re1 = aa.groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['count1'].sum().reset_index()
    re2 = aa.groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['count'].first().reset_index()
    re1 = pd.merge(re2, re1, on=['exchange', 'sta', 'colo', 'accCode', 'isMsg'])
    re1['count1'] = re1['count1'] / re1['count']
    re1 = re1.rename(columns={'count1':str(j)+'*std'})
for j in [1.5, 2, 2.5, 3, 3.5, 4, 4.5]:
    aa[str(j) + 'std_low'] = aa['mean'] - j*aa['std']
    aa[str(j) + 'std_high'] = aa['mean'] + j*aa['std']
    aa['count1'] = np.where((aa['internal_latency'] <= aa[str(j) + 'std_high']) & (aa['internal_latency'] >= aa[str(j) + 'std_low']), 1, 0)
    re = aa.groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['count1'].sum().reset_index()
    re2 = aa.groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['count'].first().reset_index()
    re = pd.merge(re2, re, on=['exchange', 'sta', 'colo', 'accCode', 'isMsg'])
    re['count1'] = re['count1'] / re['count']
    re = re.rename(columns={'count1':str(j)+'*std'})
    re1 = pd.merge(re1, re[['exchange', 'sta', 'colo', 'accCode', 'isMsg', str(j)+'*std']], on=['exchange', 'sta', 'colo', 'accCode', 'isMsg'])
nc = []
for i in range(0, re1.shape[0]):
    nc.append(np.float(re1.columns[6:][(re1.iloc[i, 6:] == 1)][0].split('*')[0]))
re1['n'] = nc
display(re1.shape[0])
display(aa1.shape[0])
aa1 = pd.merge(aa1, re1[['exchange', 'sta', 'colo', 'accCode', 'isMsg', 'n']], on=['exchange', 'sta', 'colo', 'accCode', 'isMsg'])
aa1['min'] = aa1['mean'] - aa1['std'] * aa1['n']
aa1['max'] = aa1['mean'] + aa1['std'] * aa1['n']
report = pd.merge(report, aa1, on=['exchange', 'sta', 'colo', 'accCode', 'isMsg'], how='left')
assert(report[report['mean'].isnull()].shape[0] == 0)

from IPython.display import display, HTML
display(HTML(report[((report['cur_med'] > report['max']) | (report['cur_med'] < report['min'])) \
                    & (report['cur_count'] > 100) & (abs(report['cur_med'] - report['prev_med']) > 10)] \
             .groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['prev_med', 'cur_med', 'prev_95p', 'cur_95p'].first().to_html()))
report = report[((report['cur_med'] > report['max']) | (report['cur_med'] < report['min'])) \
                    & (report['cur_count'] > 100) & (abs(report['cur_med'] - report['prev_med']) > 10)] \
             .groupby(['exchange', 'sta', 'colo', 'accCode', 'isMsg'])['prev_med', 'cur_med', 'prev_95p', 'cur_95p'].first().reset_index()

body += '<div>In the following cases, internal latency under given accCode pass the hurdle we set:'
result = [report['exchange'].values, report['sta'].values, report['colo'].values, report['accCode'].values, report['isMsg'].values, 
         report['prev_med'].values, report['prev_95p'].values, report['cur_med'].values, report['cur_95p'].values]
title = ['exchange', 'sta', 'colo', 'accCode', 'isMsg', 'prev_med', 'prev_95p', 'cur_med', 'cur_95p']
body += convertToHtml(result,title)
 




body += '<p><b>IV. tickToMBD</b></p>'

import os
import glob
import datetime
import numpy as np
import pandas as pd

checkLog = orderLog[~orderLog['start_time'].isnull()]
checkLog = checkLog.drop_duplicates(['date', 'secid', 'Price', 'OrderQty', 'Side', 'statusLs', 'TradePriceLs', 'TradeQtyLs', 'ApplSeqNum'], keep=False)
checkLog = checkLog[~checkLog['accCode'].isnull()]
checkLog['tag'] = 'previous'
checkLog.loc[checkLog['date'] == orderLog['date'].max(), 'tag'] = 'current'

cc1 = checkLog
cc1 = cc1.reset_index(drop=True)
cc1['ordering'] = cc1.index
cc1['time_diff'] = cc1['caa_orderLog'] - cc1['start_time']
cc1['colo1'] = cc1['colo'].str[:2] + cc1['colo'].str[3:5] + cc1['colo'].str[6:8]
cc1['colo_broker'] = cc1['colo1'] + '_' + cc1["accCode"].astype(int).astype(str)
cc1['accCode'] = cc1['accCode'].fillna(0).astype(int)
cc1['sta'] = np.where(cc1['mrstaat'] == 1000, 'staone', np.where(
cc1['mrstaat'] == 3000, 'statwo', 'sta300'))


re1 = cc1.groupby(['tag', 'sta', 'colo', 'accCode'])['time_diff'].describe().fillna(0).astype(int).reset_index()
# re1 = re1[re1['count'] > 20].reset_index()
c1 = cc1.groupby(['tag', 'sta', 'colo', 'accCode'])['time_diff'].apply(lambda x: x.describe([0.1])['10%']).astype(int).reset_index()
c1 = c1.rename(columns={"time_diff":"10%"})
re1 = pd.merge(re1, c1[['tag', 'sta', 'colo', 'accCode', '10%']], on=['tag', 'sta', 'colo', 'accCode'])
c1 = cc1.groupby(['tag', 'sta', 'colo', 'accCode'])['time_diff'].apply(lambda x: x.describe([0.9])['90%']).astype(int).reset_index()
c1 = c1.rename(columns={"time_diff":"90%"})
re1 = pd.merge(re1, c1[['tag', 'sta', 'colo', 'accCode', '90%']], on=['tag', 'sta', 'colo', 'accCode'])
ree1 = re1[re1['tag'] == 'previous']
ree1 = ree1.rename(columns={'50%':'prev_med', 'count':'prev_count'})
ree2 = re1[re1['tag'] == 'current']
ree2 = ree2.rename(columns={'50%':'cur_med', 'count':'cur_count'})
report = pd.merge(ree1[['sta', 'colo', 'accCode', 'prev_count', 'prev_med']], 
               ree2[['sta', 'colo', 'accCode', 'cur_count', 'cur_med']], on=['sta', 'colo', 'accCode'])


aa = cc1[(cc1['updateType'] == 0) & (cc1['tag'] == 'previous')].groupby(['date', 'sta', 'colo', 'accCode'])['time_diff'].median().reset_index()
aa['count'] = aa.groupby(['sta', 'colo', 'accCode'])['date'].transform('nunique')
aa1 = aa.groupby(['sta', 'colo', 'accCode'])['time_diff'].describe()[['mean', 'std']].reset_index()
aa1 = aa1.fillna(0)
aa = pd.merge(aa, aa1, on=['sta', 'colo', 'accCode'])
for j in [1]:
    aa[str(j) + 'std_low'] = aa['mean'] - j*aa['std']
    aa[str(j) + 'std_high'] = aa['mean'] + j*aa['std']
    aa['count1'] = np.where((aa['time_diff'] <= aa[str(j) + 'std_high']) & (aa['time_diff'] >= aa[str(j) + 'std_low']), 1, 0)
    re1 = aa.groupby(['sta', 'colo', 'accCode'])['count1'].sum().reset_index()
    re2 = aa.groupby(['sta', 'colo', 'accCode'])['count'].first().reset_index()
    re1 = pd.merge(re2, re1, on=['sta', 'colo', 'accCode'])
    re1['count1'] = re1['count1'] / re1['count']
    re1 = re1.rename(columns={'count1':str(j)+'*std'})
for j in [1.5, 2, 2.5, 3, 3.5, 4, 4.5]:
    aa[str(j) + 'std_low'] = aa['mean'] - j*aa['std']
    aa[str(j) + 'std_high'] = aa['mean'] + j*aa['std']
    aa['count1'] = np.where((aa['time_diff'] <= aa[str(j) + 'std_high']) & (aa['time_diff'] >= aa[str(j) + 'std_low']), 1, 0)
    re = aa.groupby(['sta', 'colo', 'accCode'])['count1'].sum().reset_index()
    re2 = aa.groupby(['sta', 'colo', 'accCode'])['count'].first().reset_index()
    re = pd.merge(re2, re, on=['sta', 'colo', 'accCode'])
    re['count1'] = re['count1'] / re['count']
    re = re.rename(columns={'count1':str(j)+'*std'})
    re1 = pd.merge(re1, re[['sta', 'colo', 'accCode', str(j)+'*std']], on=['sta', 'colo', 'accCode'])
nc = []
for i in range(0, re1.shape[0]):
    nc.append(np.float(re1.columns[6:][(re1.iloc[i, 6:] == 1)][0].split('*')[0]))
re1['n'] = nc
display(re1.shape[0])
display(aa1.shape[0])
aa1 = pd.merge(aa1, re1[['sta', 'colo', 'accCode', 'n']], on=['sta', 'colo', 'accCode'])
aa1['min'] = aa1['mean'] - aa1['std'] * aa1['n']
aa1['max'] = aa1['mean'] + aa1['std'] * aa1['n']
report = pd.merge(report, aa1, on=['sta', 'colo', 'accCode'], how='left')
assert(report[report['mean'].isnull()].shape[0] == 0)

from IPython.display import display, HTML
display(HTML(report[((report['cur_med'] > report['max']) | (report['cur_med'] < report['min'])) \
                    & (report['cur_count'] > 100) & (abs(report['cur_med'] - report['prev_med']) > 1000)] \
             .groupby(['sta', 'colo', 'accCode'])['prev_med', 'cur_med'].first().to_html()))
report = report[((report['cur_med'] > report['max']) | (report['cur_med'] < report['min'])) \
                    & (report['cur_count'] > 100) & (abs(report['cur_med'] - report['prev_med']) > 1000)] \
             .groupby(['sta', 'colo', 'accCode'])['prev_med', 'cur_med'].first().reset_index()
body += '<div>In the following cases, tickToMBD under given accCode pass the hurdle we set:<div>'
result = [report['sta'].values, report['colo'].values, report['accCode'].values,
         report['prev_med'].values, report['cur_med'].values]
title = ['sta', 'colo', 'accCode', 'prev_med', 'cur_med']
body += convertToHtml(result,title)





title = str(orderLog['date'].max()) + ' daily report'
body = body + "</div></body></html>"
smtp_server = '42.120.226.4' # 'smtp.mxhichina.com'
user = 'zhenyu.yin@general-int.com'
passwd = 'Yqzy0063!'
from_addr = 'zhenyu.yin@general-int.com'
to_addr = ['zhenyu.yin@general-int.com']

msg = MIMEMultipart()
msg['From'] = from_addr
msg['To'] = ', '.join(to_addr)
msg['Subject'] = title
txt = MIMEText(body, _subtype='html', _charset='UTF-8')
msg.attach(txt)

smtp = None
while True:
    try:
        smtp = smtplib.SMTP(smtp_server)
        print('smtp server connected')
        smtp.login(user, passwd)
        print('login')
        break
    except Exception as e:
        print(e)
print('send mail')
smtp.sendmail(from_addr, to_addr, msg.as_string())
smtp.quit()
