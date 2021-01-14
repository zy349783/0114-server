#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 22:44:56 2021

@author: zhenyu
"""

import pickle
import os
import pandas as pd
import numpy as np
import glob

dataPathLs = np.array(glob.glob('/data/home/zhenyu/orderLog/data/***.csv'))
dateLs = np.array([os.path.basename(i).split('.')[0] for i in dataPathLs])
dataPathLs = dataPathLs[dateLs >= '20210104']
print(dataPathLs)
for d in dataPathLs:
    data = pd.read_csv(d)
    d = d.split('.')[0] + '.pkl'
    with open(d, 'wb') as f:
        pickle.dump(data, f, protocol=4)
    print(d)