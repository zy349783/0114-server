#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 23:44:56 2020

@author: zhenyu
"""

import pandas as pd
import numpy as np
import glob
import os
import random
import pyarrow.parquet as pq
import time
from multiprocessing import Pool
from random import randint
import sys
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['OMP_THREAD_LIMIT'] = '1'


def f1(x):
    random.seed(1)
    pathLs = list(np.array(glob.glob('/data/home/zhenyu/mongoDB/md_trade/20201211/***')))
    pathLs = random.sample(pathLs, 500)
    start_time = time.time()
    df = []
    for p in pathLs:
      data = pq.read_table(p, use_threads=False).to_pandas() 
      df += [data]
    time1 = time.time() - start_time
    start_time = time.time()
    df = pd.concat(df, ignore_index=True)
    time2 = time.time() - start_time
    print(str(time1) + ',' + str(time2))
    return time1, time2, 'case1', 'HPC load home', 1

def f2(x):
    random.seed(1)
    pathLs = list(np.array(glob.glob('/data/home/zhenyu/mongoDB/md_trade/***/2000001.parquet')))
    pathLs = pathLs[-500:]
    start_time = time.time()
    df = []
    for p in pathLs:
      data = pq.read_table(p, use_threads=False).to_pandas() 
      df += [data]
    time1 = time.time() - start_time
    start_time = time.time()
    df = pd.concat(df, ignore_index=True)
    time2 = time.time() - start_time    
    print(str(time1) + ',' + str(time2))
    return time1, time2, 'case2', 'HPC load home', 1

def f3(x):
    pathLs = '/data/home/zhenyu/mongoDB/md_trade/20201211/2000001.parquet'
    start_time = time.time()
    df = []
    data = pq.read_table(pathLs, use_threads=False).to_pandas() 
    df += [data]
    time1 = time.time() - start_time
    start_time = time.time()
    df = pd.concat(df, ignore_index=True)
    time2 = time.time() - start_time
    print(str(time1) + ',' + str(time2))
    return time1, time2, 'case3', 'HPC load home', 1

# if __name__ == '__main__':
#     n = [randint(0,96) for i in range(96)]
#     with Pool(96) as p:
#         re = p.map(f1, n)
#     re = pd.DataFrame(re)
#     re.columns = ['time1', 'time2', 'case', 'mode', 'core']
#     re.to_csv('/data/home/zhenyu/result/HPC_home/core_96/case1.csv')   
    
#     with Pool(96) as p:
#         re = p.map(f2, n)
#     re = pd.DataFrame(re)
#     re.columns = ['time1', 'time2', 'case', 'mode', 'core']
#     re.to_csv('/data/home/zhenyu/result/HPC_home/core_96/case2.csv')    

#     with Pool(96) as p:
#         re = p.map(f3, n)
#     re = pd.DataFrame(re)
#     re.columns = ['time1', 'time2', 'case', 'mode', 'core']
#     re.to_csv('/data/home/zhenyu/result/HPC_home/core_96/case3.csv')    
t1 = []
t2 = []
case = []
mode = []
core = []
for i in range(20):
    re = f1(i)
    t1.append(re[0])
    t2.append(re[1])
    case.append(re[2])
    mode.append(re[3])
    core.append(re[4])
        
for i in range(20):
    re = f2(i)
    t1.append(re[0])
    t2.append(re[1])
    case.append(re[2])
    mode.append(re[3])
    core.append(re[4])
       
for i in range(20):
    re = f3(i)
    t1.append(re[0])
    t2.append(re[1])
    case.append(re[2])
    mode.append(re[3])
    core.append(re[4])
    
df = pd.DataFrame()
df['time1'] = t1
df['time2'] = t2
df['case'] = case
df['mode'] = mode
df['core'] = core
df.to_csv('/data/home/zhenyu/result/HPC_home/core_1/' + '0' + '.csv')

# random.seed(1)
# pathLs = list(np.array(glob.glob('/data/home/zhenyu/mongoDB/md_trade/20201211/***')))
# pathLs = random.sample(pathLs, 500)
# print([int(os.path.basename(i).split('.')[0]) for i in pathLs])

# pathLs = list(np.array(glob.glob('/data/home/zhenyu/mongoDB/md_trade/***/2000001.parquet')))
# pathLs = pathLs[-500:]
# print(min([int(i.split('/')[6]) for i in pathLs]))
# print(max([int(i.split('/')[6]) for i in pathLs]))