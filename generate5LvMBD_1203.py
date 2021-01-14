#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 13:13:57 2020

@author: work11
"""
import os
import glob
import datetime
import numpy as np
import pandas as pd
from collections import defaultdict, OrderedDict
from copy import deepcopy
import time


class generateMBD():

    def __init__(self, skey, date, hasAuction):

        self.date = date
        self.skey = skey

        self.bidDict = {}
        self.askDict = {}
        self.bidp = []
        self.bidq = []
        self.askp = []
        self.askq = []
        self.bidn = []
        self.askn = []
        self.orderDict = {}
        self.hasTempOrder = False
        self.isAuction = hasAuction

        self.cum_volume = 0
        self.cum_amount = 0
        self.trades = 0
        self.close = 0
        self.bid1p = 0
        self.ask1p = 0
        self.cum_volume_ls = []
        self.cum_amount_ls = []
        self.cum_trades_cnt = []
        self.closeLs = []
        self.caaLs = []
        self.exchgTmLs = []
        self.ApplSeqNumLs = []
        self.bboImproveLs = []
        self.hasUnfrozenLs = []
        self.Num = []

        self.frozenOrderDict = {}
        self.frozenBidApplSeqNumDict = {}
        self.frozenAskApplSeqNumDict = {}
        self.frozenBidDict = {}
        self.frozenAskDict = {}
        self.maxLmtBuyP = 100000000000
        self.minLmtSellP = 0

        self.total_bid_qty = []
        self.total_bid_vwap = []
        self.total_bid_levels = []
        self.total_bid_orders_num = []
        self.total_ask_qty = []
        self.total_ask_vwap = []
        self.total_ask_levels = []
        self.total_ask_orders_num = []

        self.bidnq1 = {}
        self.bidnq2 = {}
        self.asknq1 = {}
        self.asknq2 = {}
        self.bidTopq = []
        self.askTopq = []

        self.bid_qty = 0
        self.ask_qty = 0
        self.bid_amount = 0
        self.ask_amount = 0
        self.bid_price_levels = 0
        self.ask_price_levels = 0
        self.bid_order_nums = 0
        self.ask_order_nums = 0

        self.aggSide = 0
        self.prevMktApplSeqNum = 0

        self.outputLv = 10

        self.EPSILON = 1e-6

    def insertAuctionOrder(self, caa, exchgTm, ApplSeqNum, side, orderType, price, qty):

        if side == 1:
            if price in self.bidDict:
                self.bidDict[price] += qty
            else:
                self.bidDict[price] = qty
                self.bid_price_levels += 1
            if price in self.bidnq1:
                self.bidnq1[price] = self.bidnq1[price] + (ApplSeqNum,)
            else:
                self.bidnq1[price] = (ApplSeqNum,)
            self.bidnq2[ApplSeqNum] = np.int32(qty)
            self.bid_qty += qty
            self.bid_amount += qty * price
            self.bid_order_nums += 1
        else:
            if price in self.askDict:
                self.askDict[price] += qty
            else:
                self.askDict[price] = qty
                self.ask_price_levels += 1
            if price in self.asknq1:
                self.asknq1[price] = self.asknq1[price] + (ApplSeqNum,)
            else:
                self.asknq1[price] = (ApplSeqNum,)
            self.asknq2[ApplSeqNum] = np.int32(qty)
            self.ask_qty += qty
            self.ask_amount += qty * price
            self.ask_order_nums += 1
        tag = 'auction'
        self.orderDict[ApplSeqNum] = (price, qty, side, orderType, tag, 0)
        self.caaLs.append(caa)
        self.exchgTmLs.append(exchgTm)
        self.ApplSeqNumLs.append(ApplSeqNum)

    def removeOrderByAuctionCancel(self, caa, exchgTm, ApplSeqNum, cancelQty, BidApplSeqNum, OfferApplSeqNum):

        cancelApplSeqNum = max(BidApplSeqNum, OfferApplSeqNum)
        cancelPrice, cancelOrderQty, cancelSide, cancelOrderType, cancelTag, hasTrade = self.orderDict[cancelApplSeqNum]
        assert (cancelOrderQty == cancelQty)
        self.orderDict.pop(cancelApplSeqNum)

        if cancelApplSeqNum in self.asknq2:
            self.asknq2[cancelApplSeqNum] = np.int32(self.asknq2[cancelApplSeqNum] - cancelQty)
            if self.asknq2[cancelApplSeqNum] == 0:
                self.asknq2.pop(cancelApplSeqNum)
                a1 = list(self.asknq1[cancelPrice])
                a1.pop(a1.index(cancelApplSeqNum))
                self.asknq1[cancelPrice] = tuple(a1, )
        else:
            self.bidnq2[cancelApplSeqNum] = np.int32(self.bidnq2[cancelApplSeqNum] - cancelQty)
            if self.bidnq2[cancelApplSeqNum] == 0:
                self.bidnq2.pop(cancelApplSeqNum)
                b1 = list(self.bidnq1[cancelPrice])
                b1.pop(b1.index(cancelApplSeqNum))
                self.bidnq1[cancelPrice] = tuple(b1, )

        if cancelSide == 1:
            remain = self.bidDict[cancelPrice] - cancelQty
            if remain == 0:
                self.bidDict.pop(cancelPrice)
                self.bid_price_levels -= 1
            else:
                self.bidDict[cancelPrice] = remain
            self.bid_qty -= cancelQty
            self.bid_amount -= cancelQty * cancelPrice
            self.bid_order_nums -= 1
        elif cancelSide == 2:
            remain = self.askDict[cancelPrice] - cancelQty
            if remain == 0:
                self.askDict.pop(cancelPrice)
                self.ask_price_levels -= 1
            else:
                self.askDict[cancelPrice] = remain
            self.ask_qty -= cancelQty
            self.ask_amount -= cancelQty * cancelPrice
            self.ask_order_nums -= 1
        self.caaLs.append(caa)
        self.exchgTmLs.append(exchgTm)
        self.ApplSeqNumLs.append(ApplSeqNum)

    def removeOrderByAuctionTrade(self, caa, exchgTm, ApplSeqNum, price, qty, BidApplSeqNum, OfferApplSeqNum):

        bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, bidTag, hasTrade = self.orderDict[BidApplSeqNum]
        askOrderPrice, askOrderQty, askOrderSide, askOrderType, askTag, hasTrade = self.orderDict[OfferApplSeqNum]

        bidRemain = self.bidDict[bidOrderPrice] - qty
        if bidRemain == 0:
            self.bidDict.pop(bidOrderPrice)
            self.bid_price_levels -= 1
        elif bidRemain > 0:
            self.bidDict[bidOrderPrice] = bidRemain
        bidOrderQty -= qty
        if bidOrderQty == 0:
            self.orderDict.pop(BidApplSeqNum)
        else:
            self.orderDict[BidApplSeqNum] = (bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, bidTag, 1)
        cum_vol = 0
        for seqNo in self.bidnq1[bidOrderPrice]:
            cum_vol += self.bidnq2[seqNo]
            if cum_vol > qty:
                useful_qty = (self.bidnq2[seqNo] - (cum_vol - qty))
                self.bidnq2[seqNo] = np.int32(cum_vol - qty)
                self.bid_qty -= useful_qty
                self.bid_amount -= useful_qty * bidOrderPrice
                break
            elif cum_vol == qty:
                useful_qty = self.bidnq2[seqNo]
                self.bidnq2.pop(seqNo)
                b1 = list(self.bidnq1[bidOrderPrice])
                b1.pop(b1.index(seqNo))
                self.bidnq1[bidOrderPrice] = tuple(b1, )
                self.bid_qty -= useful_qty
                self.bid_amount -= useful_qty * bidOrderPrice
                self.bid_order_nums -= 1
                break
            else:
                useful_qty = self.bidnq2[seqNo]
                self.bidnq2.pop(seqNo)
                b1 = list(self.bidnq1[bidOrderPrice])
                b1.pop(b1.index(seqNo))
                self.bidnq1[bidOrderPrice] = tuple(b1, )
                self.bid_qty -= useful_qty
                self.bid_amount -= useful_qty * bidOrderPrice
                self.bid_order_nums -= 1

        askRemain = self.askDict[askOrderPrice] - qty
        if askRemain == 0:
            self.askDict.pop(askOrderPrice)
            self.ask_price_levels -= 1
        elif askRemain > 0:
            self.askDict[askOrderPrice] = askRemain
        askOrderQty -= qty
        if askOrderQty == 0:
            self.orderDict.pop(OfferApplSeqNum)
        else:
            self.orderDict[OfferApplSeqNum] = (askOrderPrice, askOrderQty, askOrderSide, askOrderType, askTag, 1)
        cum_vol = 0
        for seqNo in self.asknq1[askOrderPrice]:
            cum_vol += self.asknq2[seqNo]
            if cum_vol > qty:
                useful_qty = (self.asknq2[seqNo] - (cum_vol - qty))
                self.asknq2[seqNo] = np.int32(cum_vol - qty)
                self.ask_qty -= useful_qty
                self.ask_amount -= useful_qty * askOrderPrice
                break
            elif cum_vol == qty:
                useful_qty = self.asknq2[seqNo]
                self.asknq2.pop(seqNo)
                a1 = list(self.asknq1[askOrderPrice])
                a1.pop(a1.index(seqNo))
                self.asknq1[askOrderPrice] = tuple(a1, )
                self.ask_qty -= useful_qty
                self.ask_amount -= useful_qty * askOrderPrice
                self.ask_order_nums -= 1
                break
            else:
                useful_qty = self.asknq2[seqNo]
                self.asknq2.pop(seqNo)
                a1 = list(self.asknq1[askOrderPrice])
                a1.pop(a1.index(seqNo))
                self.asknq1[askOrderPrice] = tuple(a1, )
                self.ask_qty -= useful_qty
                self.ask_amount -= useful_qty * askOrderPrice
                self.ask_order_nums -= 1

        self.cum_volume += qty
        self.cum_amount += price * qty
        self.close = price
        self.trades += 1

        self.caaLs.append(caa)
        self.exchgTmLs.append(exchgTm)
        self.ApplSeqNumLs.append(ApplSeqNum)

    def insertOrder(self, caa, exchgTm, ApplSeqNum, side, orderType, price, qty):

        if self.isAuction:
            auctionCaa, auctionExchgTm, auctionApplSeqNum = self.caaLs[-1], self.exchgTmLs[-1], self.ApplSeqNumLs[-1]
            self.caaLs, self.exchgTmLs, self.ApplSeqNumLs = [], [], []
            self.updateMktInfo(auctionCaa, auctionExchgTm, auctionApplSeqNum, 1, record=True)
            self.isAuction = False

        if self.prevMktApplSeqNum > 0:
            ApplSeqNum1 = self.Num[-2]
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum1, self.aggSide, -1, record=True)

        if orderType == 2:
            if side == 1 and price < self.ask1p:
                if price in self.bidDict:
                    self.bidDict[price] += qty
                else:
                    self.bidDict[price] = qty
                    self.bid_price_levels += 1
                isImprove = 1 if price > self.bid1p or self.prevMktApplSeqNum > 0 else 0
                if price in self.bidnq1:
                    self.bidnq1[price] = self.bidnq1[price] + (ApplSeqNum,)
                else:
                    self.bidnq1[price] = (ApplSeqNum,)
                self.bidnq2[ApplSeqNum] = np.int32(qty)
                self.bid_qty += qty
                self.bid_amount += qty * price
                self.bid_order_nums += 1
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 'passive_limit_bid', 0)
                self.aggSide = 0

            elif side == 2 and price > self.bid1p:
                if price in self.askDict:
                    self.askDict[price] += qty
                else:
                    self.askDict[price] = qty
                    self.ask_price_levels += 1
                isImprove = 1 if price < self.ask1p or self.prevMktApplSeqNum > 0 else 0
                if price in self.asknq1:
                    self.asknq1[price] = self.asknq1[price] + (ApplSeqNum,)
                else:
                    self.asknq1[price] = (ApplSeqNum,)
                self.asknq2[ApplSeqNum] = np.int32(qty)
                self.ask_qty += qty
                self.ask_amount += qty * price
                self.ask_order_nums += 1
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 'passive_limit_ask', 0)
                self.aggSide = 0


            elif (side == 1 and price >= self.ask1p and price <= self.maxLmtBuyP) or \
                    (side == 2 and price <= self.bid1p and price >= self.minLmtSellP):
                if side == 1:
                    if price in self.bidDict:
                        self.bidDict[price] += qty
                    else:
                        self.bidDict[price] = qty
                        self.bid_price_levels += 1
                    self.aggSide = 1
                    if price in self.bidnq1:
                        self.bidnq1[price] = self.bidnq1[price] + (ApplSeqNum,)
                    else:
                        self.bidnq1[price] = (ApplSeqNum,)
                    self.bidnq2[ApplSeqNum] = np.int32(qty)
                    self.bid_qty += qty
                    self.bid_amount += qty * price
                    self.bid_order_nums += 1
                    self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 'aggressive_limit_bid', 0)
                else:
                    if price in self.askDict:
                        self.askDict[price] += qty
                    else:
                        self.askDict[price] = qty
                        self.ask_price_levels += 1
                    self.aggSide = 2
                    if price in self.asknq1:
                        self.asknq1[price] = self.asknq1[price] + (ApplSeqNum,)
                    else:
                        self.asknq1[price] = (ApplSeqNum,)
                    self.asknq2[ApplSeqNum] = np.int32(qty)
                    self.ask_qty += qty
                    self.ask_amount += qty * price
                    self.ask_order_nums += 1
                    self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 'aggressive_limit_ask', 0)
                self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, 1, record=True)

            elif ((side == 1 and price > self.maxLmtBuyP) or (side == 2 and price < self.minLmtSellP)) & (
                    self.skey >= 2300000) & (self.date >= 20200824):
                self.frozenOrderDict[ApplSeqNum] = (price, qty, side)
                if side == 1:
                    if price in self.frozenBidDict:
                        self.frozenBidDict[price] += qty
                    else:
                        self.frozenBidDict[price] = qty
                    if price in self.frozenBidApplSeqNumDict:
                        self.frozenBidApplSeqNumDict[price].append(ApplSeqNum)
                    else:
                        self.frozenBidApplSeqNumDict[price] = [ApplSeqNum]
                elif side == 2:
                    if price in self.frozenAskDict:
                        self.frozenAskDict[price] += qty
                    else:
                        self.frozenAskDict[price] = qty
                    if price in self.frozenAskApplSeqNumDict:
                        self.frozenAskApplSeqNumDict[price].append(ApplSeqNum)
                    else:
                        self.frozenAskApplSeqNumDict[price] = [ApplSeqNum]
                self.orderDict[ApplSeqNum] = (price, qty, side, 4, 'aggressive_limit_frozen', 0)
                isImprove = 1 if self.prevMktApplSeqNum > 0 else 0
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 'aggressive_limit_frozen', 0)
                self.aggSide = 0
            else:
                print('no suitable order ', ApplSeqNum, price, side)

        elif orderType == 1:
            if side == 1:
                self.bidDict[self.ask1p] = qty
                if self.ask1p in self.bidnq1:
                    self.bidnq1[self.ask1p] = self.bidnq1[self.ask1p] + (ApplSeqNum,)
                else:
                    self.bidnq1[self.ask1p] = (ApplSeqNum,)
                self.bidnq2[ApplSeqNum] = np.int32(qty)
                self.orderDict[ApplSeqNum] = (self.ask1p, qty, side, orderType, 'market_order_bid', 0)
                self.bid_qty += qty
                self.bid_amount += qty * self.ask1p
                self.bid_order_nums += 1
                self.bid_price_levels += 1
                self.aggSide = 1
            else:
                self.askDict[self.bid1p] = qty
                if self.bid1p in self.asknq1:
                    self.asknq1[self.bid1p] = self.asknq1[self.bid1p] + (ApplSeqNum,)
                else:
                    self.asknq1[self.bid1p] = (ApplSeqNum,)
                self.asknq2[ApplSeqNum] = np.int32(qty)
                self.ask_qty += qty
                self.ask_amount += qty * self.bid1p
                self.ask_order_nums += 1
                self.ask_price_levels += 1
                self.orderDict[ApplSeqNum] = (self.bid1p, qty, side, orderType, 'market_order_ask', 0)
                self.aggSide = 2

        elif orderType == 3:
            if side == 1:
                self.orderDict[ApplSeqNum] = (self.bid1p, qty, side, orderType, 'own_optimal_bid', 0)
                if self.bid1p in self.bidDict:
                    self.bidDict[self.bid1p] += qty
                    if self.bid1p in self.bidnq1:
                        self.bidnq1[self.bid1p] = self.bidnq1[self.bid1p] + (ApplSeqNum,)
                    else:
                        self.bidnq1[self.bid1p] = (ApplSeqNum,)
                    self.bidnq2[ApplSeqNum] = np.int32(qty)
                    self.bid_qty += qty
                    self.bid_amount += qty * self.bid1p
                    self.bid_order_nums += 1
                    isImprove = 1 if self.prevMktApplSeqNum > 0 else 0
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                else:
                    # Under this condition, the current order will be deleted immediately
                    self.bidDict[self.bid1p] = qty
                    self.bidnq1[self.bid1p] = (ApplSeqNum,)
                    self.bidnq2[ApplSeqNum] = np.int32(qty)
                    self.bid_qty += qty
                    self.bid_amount += qty * self.bid1p
                    self.bid_order_nums += 1
                    self.bid_price_levels += 1
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, 0, record=False)
            else:
                self.orderDict[ApplSeqNum] = (self.ask1p, qty, side, orderType, 'own_optimal_ask', 0)
                if self.ask1p in self.askDict:
                    self.askDict[self.ask1p] += qty
                    if self.ask1p in self.asknq1:
                        self.asknq1[self.ask1p] = self.asknq1[self.ask1p] + (ApplSeqNum,)
                    else:
                        self.asknq1[self.ask1p] = (ApplSeqNum,)
                    self.asknq2[ApplSeqNum] = np.int32(qty)
                    self.ask_qty += qty
                    self.ask_amount += qty * self.ask1p
                    self.ask_order_nums += 1
                    isImprove = 1 if self.prevMktApplSeqNum > 0 else 0
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                else:
                    self.askDict[self.ask1p] = qty
                    self.asknq1[self.ask1p] = (ApplSeqNum,)
                    self.asknq2[ApplSeqNum] = np.int32(qty)
                    self.ask_qty += qty
                    self.ask_amount += qty * self.ask1p
                    self.ask_order_nums += 1
                    self.ask_price_levels += 1
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, 0, record=False)
            self.aggSide = 0

        self.prevMktApplSeqNum = 0

    def removeOrderByTrade(self, caa, exchgTm, ApplSeqNum, price, qty, BidApplSeqNum, OfferApplSeqNum):

        bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, bidTag, hasTrade = self.orderDict[BidApplSeqNum]
        askOrderPrice, askOrderQty, askOrderSide, askOrderType, askTag, hasTrade = self.orderDict[OfferApplSeqNum]

        if self.prevMktApplSeqNum > 0 and BidApplSeqNum != self.prevMktApplSeqNum and OfferApplSeqNum != self.prevMktApplSeqNum:
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, 1, record=True)

        hasUnfrozen = 0
        if BidApplSeqNum in self.frozenOrderDict:
            self.bidDict, self.frozenBidDict, self.frozenOrderDict, self.frozenBidApplSeqNumDict, self.bid_price_levels, self.bid_order_nums, self.bidnq1, self.bidnq2, self.bid_qty, self.bid_amount, \
            self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2, self.ask_qty, self.ask_amount = \
                self.unfrozen(bidOrderPrice, 1, self.bidDict, self.frozenBidDict, self.frozenOrderDict,
                              self.frozenBidApplSeqNumDict, self.bid_price_levels, self.bid_order_nums, self.bidnq1,
                              self.bidnq2, self.bid_qty, self.bid_amount, \
                              self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2, self.ask_qty,
                              self.ask_amount)
            hasUnfrozen = 1

        if OfferApplSeqNum in self.frozenOrderDict:
            self.askDict, self.frozenAskDict, self.frozenOrderDict, self.frozenAskApplSeqNumDict, self.bid_price_levels, self.bid_order_nums, self.bidnq1, self.bidnq2, self.bid_qty, self.bid_amount, \
            self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2, self.ask_qty, self.ask_amount = \
                self.unfrozen(askOrderPrice, 2, self.askDict, self.frozenAskDict, self.frozenOrderDict,
                              self.frozenAskApplSeqNumDict, self.bid_price_levels, self.bid_order_nums, self.bidnq1,
                              self.bidnq2, self.bid_qty, self.bid_amount, \
                              self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2, self.ask_qty,
                              self.ask_amount)
            hasUnfrozen = 1

        bidRemain = self.bidDict[bidOrderPrice] - qty
        bidOrderQty -= qty
        self.bid_qty -= qty
        self.bid_amount -= qty * bidOrderPrice
        if bidRemain == 0:
            self.bidDict.pop(bidOrderPrice)
            self.bid_price_levels -= 1
        else:
            if (bidOrderType == 1) & (price != bidOrderPrice):
                if bidRemain > bidOrderQty:
                    self.bidDict[bidOrderPrice] -= bidOrderQty
                else:
                    assert(bidRemain == bidOrderQty)
                    self.bidDict.pop(bidOrderPrice)
                    self.bid_price_levels -= 1
                if price in self.bidDict:
                    self.bidDict[price] += bidOrderQty
                else:
                    self.bidDict[price] = bidOrderQty
                    self.bid_price_levels += 1
            else:
                self.bidDict[bidOrderPrice] = bidRemain

        if bidOrderQty == 0:
            self.orderDict.pop(BidApplSeqNum)
            self.bidnq2.pop(BidApplSeqNum)
            b1 = list(self.bidnq1[bidOrderPrice])
            b1.pop(b1.index(BidApplSeqNum))
            self.bidnq1[bidOrderPrice] = tuple(b1, )
            self.bid_order_nums -= 1
        else:
            if (bidOrderType == 1) & (price != bidOrderPrice):
                self.orderDict[BidApplSeqNum] = (price, bidOrderQty, bidOrderSide, bidOrderType, bidTag, 1)
                b1 = list(self.bidnq1[bidOrderPrice])
                b1.pop(b1.index(BidApplSeqNum))
                self.bidnq1[bidOrderPrice] = tuple(b1, )
                if price in self.bidnq1:
                    self.bidnq1[price] = self.bidnq1[price] + (BidApplSeqNum,)
                else:
                    self.bidnq1[price] = (BidApplSeqNum,)
                self.bidnq2[BidApplSeqNum] = np.int32(bidOrderQty)
            else:
                self.orderDict[BidApplSeqNum] = (bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, bidTag, 1)
                self.bidnq2[BidApplSeqNum] = np.int32(bidOrderQty)

        if askOrderPrice not in self.askDict:
            print(exchgTm, ApplSeqNum, price, qty)

        askRemain = self.askDict[askOrderPrice] - qty
        askOrderQty -= qty
        self.ask_qty -= qty
        self.ask_amount -= qty * askOrderPrice
        if askRemain == 0:
            self.askDict.pop(askOrderPrice)
            self.ask_price_levels -= 1
        else:
            if (askOrderType == 1) & (askOrderPrice != price):
                if askRemain > askOrderQty:
                    self.askDict[askOrderPrice] -= askOrderQty
                else:
                    assert (askRemain == askOrderQty)
                    self.askDict.pop(askOrderPrice)
                    self.ask_price_levels -= 1
                if price in self.askDict:
                    self.askDict[price] += askOrderQty
                else:
                    self.askDict[price] = askOrderQty
                    self.ask_price_levels += 1
            else:
                self.askDict[askOrderPrice] = askRemain

        if askOrderQty == 0:
            self.orderDict.pop(OfferApplSeqNum)
            self.asknq2.pop(OfferApplSeqNum)
            a1 = list(self.asknq1[askOrderPrice])
            a1.pop(a1.index(OfferApplSeqNum))
            self.asknq1[askOrderPrice] = tuple(a1, )
            self.ask_order_nums -= 1
        else:
            if (askOrderType == 1) & (price != askOrderPrice):
                self.orderDict[OfferApplSeqNum] = (price, askOrderQty, askOrderSide, askOrderType, askTag, 1)
                a1 = list(self.asknq1[askOrderPrice])
                a1.pop(a1.index(OfferApplSeqNum))
                self.asknq1[askOrderPrice] = tuple(a1, )
                if price in self.asknq1:
                    self.asknq1[price] = self.asknq1[price] + (OfferApplSeqNum,)
                else:
                    self.asknq1[price] = (OfferApplSeqNum,)
                self.asknq2[OfferApplSeqNum] = np.int32(askOrderQty)
            else:
                self.orderDict[OfferApplSeqNum] = (askOrderPrice, askOrderQty, askOrderSide, askOrderType, askTag, 1)
                self.asknq2[OfferApplSeqNum] = np.int32(askOrderQty)

        self.cum_volume += qty
        self.cum_amount += price * qty
        self.close = price
        self.trades += 1

        if (self.aggSide == 1 and bidOrderType == 1 and bidOrderQty > 0) or (
                self.aggSide == 2 and askOrderType == 1 and askOrderQty > 0):
            self.prevMktApplSeqNum = BidApplSeqNum if self.aggSide == 1 else OfferApplSeqNum
        else:
            self.prevMktApplSeqNum = 0

        if (self.aggSide == 1 and bidOrderType == 1 and bidOrderQty == 0) or (
                self.aggSide == 2 and askOrderType == 1 and askOrderQty == 0):
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, 1, True)
        else:
            isImprove, hasUnfrozen = 0, 0
            self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, False)

    def removeOrderByCancel(self, caa, exchgTm, ApplSeqNum, cancelQty, BidApplSeqNum, OfferApplSeqNum):

        if self.isAuction:
            auctionCaa, auctionExchgTm, auctionApplSeqNum = self.caaLs[-1], self.exchgTmLs[-1], self.ApplSeqNumLs[-1]
            self.caaLs, self.exchgTmLs, self.ApplSeqNumLs = [], [], []
            self.updateMktInfo(auctionCaa, auctionExchgTm, auctionApplSeqNum, 1, True)
            self.isAuction = False

        hasUnfrozen = 0

        cancelApplSeqNum = max(BidApplSeqNum, OfferApplSeqNum)
        cancelPrice, cancelOrderQty, cancelSide, cancelOrderType, cancelTag, hasTrade = self.orderDict[cancelApplSeqNum]

        if ((self.prevMktApplSeqNum > 0) & (cancelApplSeqNum == self.prevMktApplSeqNum) & (cancelOrderType == 1)) | (
                (cancelApplSeqNum != self.prevMktApplSeqNum) & (self.prevMktApplSeqNum > 0)):
            ApplSeqNum1 = self.Num[-2]
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum1, self.aggSide, -1, record=True)

        assert (cancelOrderQty == cancelQty)
        self.orderDict.pop(cancelApplSeqNum)

        if cancelApplSeqNum in self.frozenOrderDict:
            self.frozenOrderDict.pop(cancelApplSeqNum)
            if cancelSide == 1:
                remain = self.frozenBidDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.frozenBidDict.pop(cancelPrice)
                else:
                    self.frozenBidDict[cancelPrice] = remain
                cancelIx = self.frozenBidApplSeqNumDict[cancelPrice].index(cancelApplSeqNum)
                self.frozenBidApplSeqNumDict[cancelPrice].pop(cancelIx)
            else:
                remain = self.frozenAskDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.frozenAskDict.pop(cancelPrice)
                else:
                    self.frozenAskDict[cancelPrice] = remain
                cancelIx = self.frozenAskApplSeqNumDict[cancelPrice].index(cancelApplSeqNum)
                self.frozenAskApplSeqNumDict[cancelPrice].pop(cancelIx)

            if self.prevMktApplSeqNum != 0:
                isImprove = 1
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, True)

        else:
            if cancelSide == 1:
                remain = self.bidDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.bidDict.pop(cancelPrice)
                    self.bid_price_levels -= 1
                else:
                    self.bidDict[cancelPrice] = remain
                self.bid_qty -= cancelQty
                self.bid_amount -= cancelQty * cancelPrice
                self.bid_order_nums -= 1

            elif cancelSide == 2:
                remain = self.askDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.askDict.pop(cancelPrice)
                    self.ask_price_levels -= 1
                else:
                    self.askDict[cancelPrice] = remain
                self.ask_qty -= cancelQty
                self.ask_amount -= cancelQty * cancelPrice
                self.ask_order_nums -= 1

            if cancelOrderType == 1 and self.prevMktApplSeqNum > 0:
                isImprove = 1 if hasTrade == 1 else 0
            else:
                if (cancelSide == 1 and cancelPrice == self.bid1p and remain == 0) or \
                        (cancelSide == 2 and cancelPrice == self.ask1p and remain == 0) or \
                        (self.prevMktApplSeqNum > 0):
                    isImprove = 1
                else:
                    isImprove = 0

            if (cancelPrice in self.asknq1) & (cancelSide == 2):
                if cancelApplSeqNum in self.asknq1[cancelPrice]:
                    self.asknq2[cancelApplSeqNum] = np.int32(self.asknq2[cancelApplSeqNum] - cancelQty)
                    if self.asknq2[cancelApplSeqNum] == 0:
                        self.asknq2.pop(cancelApplSeqNum)
                        a1 = list(self.asknq1[cancelPrice])
                        a1.pop(a1.index(cancelApplSeqNum))
                        self.asknq1[cancelPrice] = tuple(a1, )
            elif (cancelPrice in self.bidnq1) & (cancelSide == 1):
                if cancelApplSeqNum in self.bidnq1[cancelPrice]:
                    self.bidnq2[cancelApplSeqNum] = np.int32(self.bidnq2[cancelApplSeqNum] - cancelQty)
                    if self.bidnq2[cancelApplSeqNum] == 0:
                        self.bidnq2.pop(cancelApplSeqNum)
                        b1 = list(self.bidnq1[cancelPrice])
                        b1.pop(b1.index(cancelApplSeqNum))
                        self.bidnq1[cancelPrice] = tuple(b1, )

            if remain == 0 and (
                    (cancelSide == 1 and cancelPrice == self.bid1p) or (cancelSide == 2 and cancelPrice == self.ask1p)):
                hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, cancelSide, 1, record=True)

            else:
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, True)

        self.prevMktApplSeqNum = 0

    def guessNBBO(self, caa, exchgTm, ApplSeqNum, aggSide, bbo, record=True):
        if ApplSeqNum in self.orderDict.keys():
            OrderPrice, OrderQty, OrderSide, OrderType, Tag, hasTrade = self.orderDict[ApplSeqNum]
        else:
            Tag = 'None'

        fakeBidnq1 = self.bidnq1.copy()
        fakeBidnq2 = self.bidnq2.copy()
        fakeAsknq1 = self.asknq1.copy()
        fakeAsknq2 = self.asknq2.copy()

        fakeBidDict = self.bidDict.copy()
        fakeAskDict = self.askDict.copy()

        hasTrade = 0
        fakeBidDict, fakeAskDict, fake_trades, fake_cum_volume, fake_cum_amount, fake_close, fakeBid1p, fakeAsk1p, fakeBidnq1, fakeBidnq2, fakeAsknq1, fakeAsknq2, fakeBid_qty, fakeAsk_qty, fakeBid_amount, \
        fakeAsk_amount, fakeBid_price_levels, fakeAsk_price_levels, fakeBid_order_nums, fakeAsk_order_nums, fakeHasTrade = \
            self.matchTrades(fakeBidDict, fakeAskDict, self.trades, self.cum_volume, self.cum_amount, self.close,
                             fakeBidnq1, fakeBidnq2, fakeAsknq1, fakeAsknq2, self.bid_qty, \
                             self.ask_qty, self.bid_amount, self.ask_amount, self.bid_price_levels,
                             self.ask_price_levels, self.bid_order_nums, self.ask_order_nums, aggSide, Tag)
        hasTrade = max(hasTrade, fakeHasTrade)

        # fakeTrade_on_1p_buy_qty = self.cur_cum_aggressive_limit_trade_on_1p_buy_qty
        # fakeTrade_on_1p_sell_qty = self.cur_cum_aggressive_limit_trade_on_1p_sell_qty
        # fakeTrade_on_1p_buy_amount = self.cur_cum_aggressive_limit_trade_on_1p_buy_amount
        # fakeTrade_on_1p_sell_amount = self.cur_cum_aggressive_limit_trade_on_1p_sell_amount
        # fakeTrade_over_1p_buy_qty = self.cur_cum_aggressive_limit_trade_over_1p_buy_qty
        # fakeTrade_over_1p_sell_qty = self.cur_cum_aggressive_limit_trade_over_1p_sell_qty
        # fakeTrade_over_1p_buy_amount = self.cur_cum_aggressive_limit_trade_over_1p_buy_amount
        # fakeTrade_over_1p_sell_amount = self.cur_cum_aggressive_limit_trade_over_1p_sell_amount

        hasUnfrozen = 0

        if (self.skey >= 2300000) & (self.date >= 20200824):
            if aggSide == 1 and len(self.frozenBidDict) > 0:
                fakeMinBuyP = max(round(fakeAsk1p * 1.02 + self.EPSILON, -2), fakeAsk1p + 100)
                while len(self.frozenBidDict) > 0 and min(self.frozenBidDict.keys()) <= fakeMinBuyP:
                    fakeFrozenBidDict, fakeFrozenOrderDict, fakeFrozenBidApplSeqNumDict = self.frozenBidDict.copy(), self.frozenOrderDict.copy(), self.frozenBidApplSeqNumDict.copy()
                    self.bidDict, self.frozenBidDict, self.frozenOrderDict, self.frozenBidApplSeqNumDict, self.bid_price_levels, self.bid_order_nums, self.bidnq1, self.bidnq2, self.bid_qty, self.bid_amount, \
                    self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2, self.ask_qty, self.ask_amount = \
                        self.unfrozen(fakeMinBuyP, 1, self.bidDict, self.frozenBidDict, self.frozenOrderDict,
                                      self.frozenBidApplSeqNumDict, self.bid_price_levels, self.bid_order_nums,
                                      self.bidnq1, self.bidnq2, self.bid_qty, self.bid_amount, \
                                      self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2,
                                      self.ask_qty, self.ask_amount)
                    fakeBidDict, fakeFrozenBidDict, fakeFrozenOrderDict, fakeFrozenBidApplSeqNumDict, fakeBid_price_levels, fakeBid_order_nums, fakeBidnq1, fakeBidnq2, fakeBid_qty, fakeBid_amount, \
                    fakeAsk_price_levels, fakeAsk_order_nums, fakeAsknq1, fakeAsknq2, fakeAsk_qty, fakeAsk_amount = \
                        self.unfrozen(fakeMinBuyP, 1, fakeBidDict, fakeFrozenBidDict, fakeFrozenOrderDict,
                                      fakeFrozenBidApplSeqNumDict, fakeBid_price_levels, fakeBid_order_nums, fakeBidnq1,
                                      fakeBidnq2, fakeBid_qty, fakeBid_amount, \
                                      fakeAsk_price_levels, fakeAsk_order_nums, fakeAsknq1, fakeAsknq2, fakeAsk_qty,
                                      fakeAsk_amount)

                    fakeBidDict, fakeAskDict, fake_trades, fake_cum_volume, fake_cum_amount, fake_close, fakeBid1p, fakeAsk1p, fakeBidnq1, fakeBidnq2, fakeAsknq1, fakeAsknq2, fakeBid_qty, fakeAsk_qty, fakeBid_amount, \
                    fakeAsk_amount, fakeBid_price_levels, fakeAsk_price_levels, fakeBid_order_nums, fakeAsk_order_nums, fakeHasTrade = \
                        self.matchTrades(fakeBidDict, fakeAskDict, fake_trades, fake_cum_volume, fake_cum_amount,
                                         fake_close, fakeBidnq1, fakeBidnq2, fakeAsknq1, fakeAsknq2, fakeBid_qty,
                                         fakeAsk_qty, fakeBid_amount, \
                                         fakeAsk_amount, fakeBid_price_levels, fakeAsk_price_levels, fakeBid_order_nums,
                                         fakeAsk_order_nums, aggSide, Tag)

                    hasTrade = max(hasTrade, fakeHasTrade)
                    fakeMinBuyP = max(round(fakeAsk1p * 1.02 + self.EPSILON, -2), fakeAsk1p + 100)
                    hasUnfrozen = 1

            if aggSide == 2 and len(self.frozenAskDict) > 0:
                fakeMaxSellP = min(round(fakeBid1p * 0.98 + self.EPSILON, -2), fakeBid1p - 100)
                while len(self.frozenAskDict) > 0 and max(self.frozenAskDict.keys()) >= fakeMaxSellP:
                    fakeFrozenAskDict, fakeFrozenOrderDict, fakeFrozenAskApplSeqNumDict = self.frozenAskDict.copy(), self.frozenOrderDict.copy(), self.frozenAskApplSeqNumDict.copy()
                    self.askDict, self.frozenAskDict, self.frozenOrderDict, self.frozenAskApplSeqNumDict, self.bid_price_levels, self.bid_order_nums, self.bidnq1, self.bidnq2, self.bid_qty, self.bid_amount, \
                    self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2, self.ask_qty, self.ask_amount = \
                        self.unfrozen(fakeMaxSellP, 2, self.askDict, self.frozenAskDict, self.frozenOrderDict,
                                      self.frozenAskApplSeqNumDict, self.bid_price_levels, self.bid_order_nums,
                                      self.bidnq1, self.bidnq2, self.bid_qty, self.bid_amount, \
                                      self.ask_price_levels, self.ask_order_nums, self.asknq1, self.asknq2,
                                      self.ask_qty, self.ask_amount)
                    fakeAskDict, fakeFrozenAskDict, fakeFrozenOrderDict, fakeFrozenAskApplSeqNumDict, fakeBid_price_levels, fakeBid_order_nums, fakeBidnq1, fakeBidnq2, fakeBid_qty, fakeBid_amount, \
                    fakeAsk_price_levels, fakeAsk_order_nums, fakeAsknq1, fakeAsknq2, fakeAsk_qty, fakeAsk_amount = \
                        self.unfrozen(fakeMaxSellP, 2, fakeAskDict, fakeFrozenAskDict, fakeFrozenOrderDict,
                                      fakeFrozenAskApplSeqNumDict, fakeBid_price_levels, fakeBid_order_nums, fakeBidnq1,
                                      fakeBidnq2, fakeBid_qty, fakeBid_amount, \
                                      fakeAsk_price_levels, fakeAsk_order_nums, fakeAsknq1, fakeAsknq2, fakeAsk_qty,
                                      fakeAsk_amount)

                    fakeBidDict, fakeAskDict, fake_trades, fake_cum_volume, fake_cum_amount, fake_close, fakeBid1p, fakeAsk1p, fakeBidnq1, fakeBidnq2, fakeAsknq1, fakeAsknq2, fakeBid_qty, fakeAsk_qty, fakeBid_amount, \
                    fakeAsk_amount, fakeBid_price_levels, fakeAsk_price_levels, fakeBid_order_nums, fakeAsk_order_nums, fakeHasTrade = \
                        self.matchTrades(fakeBidDict, fakeAskDict, fake_trades, fake_cum_volume, fake_cum_amount,
                                         fake_close, fakeBidnq1, fakeBidnq2, fakeAsknq1, fakeAsknq2, fakeBid_qty,
                                         fakeAsk_qty, fakeBid_amount, \
                                         fakeAsk_amount, fakeBid_price_levels, fakeAsk_price_levels, fakeBid_order_nums,
                                         fakeAsk_order_nums, aggSide, Tag)

                    hasTrade = max(hasTrade, fakeHasTrade)
                    fakeMaxSellP = min(round(fakeBid1p * 0.98 + self.EPSILON, -2), fakeBid1p - 100)
                    hasUnfrozen = 1

        curBidP = sorted(fakeBidDict.keys(), reverse=True)[:self.outputLv]
        curAskP = sorted(fakeAskDict.keys())[:self.outputLv]

        if hasTrade == 0:
            if len(self.askDict) != 0:
                self.ask1p = curAskP[0]
            else:
                self.ask1p = curBidP[0] + 100

            if len(self.bidDict) != 0:
                self.bid1p = curBidP[0]
            else:
                self.bid1p = curAskP[0] - 100

            if (self.skey >= 2300000) & (self.date >= 20200824):
                self.maxLmtBuyP = max(round(self.ask1p * 1.02 + self.EPSILON, -2), self.ask1p + 100)
                self.minLmtSellP = min(round(self.bid1p * 0.98 + self.EPSILON, -2), self.bid1p - 100)

        if record:
            self.caaLs.append(caa)
            self.exchgTmLs.append(exchgTm)
            self.ApplSeqNumLs.append(ApplSeqNum)
            self.bboImproveLs.append(bbo)
            self.hasUnfrozenLs.append(hasUnfrozen)

            curBidQ = [fakeBidDict[i] for i in curBidP]
            curBidN = [len(fakeBidnq1[i]) for i in curBidP]
            self.bidp += [curBidP + [0] * (self.outputLv - len(curBidP))]
            self.bidq += [curBidQ + [0] * (self.outputLv - len(curBidQ))]
            self.bidn += [curBidN + [0] * (self.outputLv - len(curBidN))]

            curAskQ = [fakeAskDict[i] for i in curAskP]
            curAskN = [len(fakeAsknq1[i]) for i in curAskP]
            self.askp += [curAskP + [0] * (self.outputLv - len(curAskP))]
            self.askq += [curAskQ + [0] * (self.outputLv - len(curAskQ))]
            self.askn += [curAskN + [0] * (self.outputLv - len(curAskN))]

            self.cum_volume_ls.append(fake_cum_volume)
            self.cum_amount_ls.append(fake_cum_amount)
            self.closeLs.append(fake_close)
            self.cum_trades_cnt.append(fake_trades)

            self.total_bid_qty.append(fakeBid_qty)
            self.total_bid_levels.append(fakeBid_price_levels)
            self.total_bid_orders_num.append(fakeBid_order_nums)
            bmaq = 0 if fakeBid_qty == 0 else fakeBid_amount / fakeBid_qty
            self.total_bid_vwap.append(bmaq)
            self.total_ask_qty.append(fakeAsk_qty)
            self.total_ask_levels.append(fakeAsk_price_levels)
            self.total_ask_orders_num.append(fakeAsk_order_nums)
            amaq = 0 if fakeAsk_qty == 0 else fakeAsk_amount / fakeAsk_qty
            self.total_ask_vwap.append(amaq)

        return hasUnfrozen

    def matchTrades(self, bidDict, askDict, trades, cum_volume, cum_amount, close, bidnq1, bidnq2, asknq1, asknq2,
                    bid_qty, ask_qty, bid_amount, ask_amount, bid_price_levels, ask_price_levels, \
                    bid_order_nums, ask_order_nums, aggSide, Tag):
        bidPLs = sorted(bidDict.keys(), reverse=True)
        askPLs = sorted(askDict.keys())
        if len(bidPLs) > 0 and len(askPLs) > 0:
            bid1p = bidPLs[0]
            ask1p = askPLs[0]
        elif len(bidPLs) == 0:
            ask1p = askPLs[0]
            bid1p = ask1p - 100
        elif len(askPLs) == 0:
            bid1p = bidPLs[0]
            ask1p = bid1p + 100
        if bid1p >= ask1p:
            while bid1p >= ask1p:
                bidQty = bidDict[bid1p]
                askQty = askDict[ask1p]
                tradeQty = min(bidQty, askQty)
                tradeP = bid1p if aggSide == 2 else ask1p
                cum_volume += tradeQty
                cum_amount += tradeQty * tradeP
                close = tradeP
                bid_qty -= tradeQty
                ask_qty -= tradeQty
                bid_amount -= tradeQty * bid1p
                ask_amount -= tradeQty * ask1p

                remain = tradeQty
                if tradeQty == bidQty:
                    bidDict.pop(bid1p)
                    bidPLs = bidPLs[1:]
                    bid_price_levels -= 1
                if tradeQty == askQty:
                    askDict.pop(ask1p)
                    askPLs = askPLs[1:]
                    ask_price_levels -= 1
                if tradeQty != askQty:
                    askDict[ask1p] -= tradeQty
                if tradeQty != bidQty:
                    bidDict[bid1p] -= tradeQty

                for seqNo in bidnq1[bid1p]:
                    if remain > 0:
                        qty1 = bidnq2[seqNo]
                        if remain > qty1:
                            remain = remain - qty1
                            bidnq2.pop(seqNo)
                            b1 = list(bidnq1[bid1p])
                            b1.pop(b1.index(seqNo))
                            bidnq1[bid1p] = tuple(b1, )
                            bid_order_nums -= 1
                        elif remain == qty1:
                            bidnq2.pop(seqNo)
                            b1 = list(bidnq1[bid1p])
                            b1.pop(b1.index(seqNo))
                            bidnq1[bid1p] = tuple(b1, )
                            bid_order_nums -= 1
                            remain = 0
                        else:
                            bidnq2[seqNo] = np.int32(qty1 - remain)
                            remain = 0

                        s_um = 0
                        pop_list = []
                        for seqNo1 in asknq1[ask1p]:
                            s_um += asknq2[seqNo1]
                            trades += 1
                            if s_um > qty1:
                                asknq2[seqNo1] = np.int32(s_um - qty1)
                                break
                            elif s_um == qty1:
                                pop_list.append(seqNo1)
                                break
                            else:
                                pop_list.append(seqNo1)
                        for seqNo1 in pop_list:
                            asknq2.pop(seqNo1)
                            a1 = list(asknq1[ask1p])
                            a1.pop(a1.index(seqNo1))
                            asknq1[ask1p] = tuple(a1, )
                            ask_order_nums -= 1

                if len(bidPLs) > 0 and len(askPLs) > 0:
                    bid1p = bidPLs[0]
                    ask1p = askPLs[0]
                elif len(bidPLs) == 0:
                    ask1p = askPLs[0]
                    bid1p = ask1p - 100
                elif len(askPLs) == 0:
                    bid1p = bidPLs[0]
                    ask1p = bid1p + 100
            hasTrade = 1
        else:
            hasTrade = 0

        return bidDict, askDict, trades, cum_volume, cum_amount, close, bid1p, ask1p, bidnq1, bidnq2, asknq1, asknq2, bid_qty, ask_qty, bid_amount, ask_amount, bid_price_levels, ask_price_levels, \
               bid_order_nums, ask_order_nums, hasTrade

    def unfrozen(self, price, side, orderbookDict, frozenOrderbookDict, frozenOrderDict, frozenApplSeqNumDict,
                 bid_price_levels, bid_order_nums, bidnq1, bidnq2, bid_qty, bid_amount, ask_price_levels,
                 ask_order_nums,
                 asknq1, asknq2, ask_qty, ask_amount):

        if side == 1:
            frozenPLs = np.array(sorted(frozenOrderbookDict.keys(), reverse=True))
            frozenPLs = frozenPLs[frozenPLs <= price]
        else:
            frozenPLs = np.array(sorted(frozenOrderbookDict.keys()))
            frozenPLs = frozenPLs[frozenPLs >= price]

        for frozenP in frozenPLs:
            if side == 1:
                if frozenP in orderbookDict:
                    orderbookDict[frozenP] += frozenOrderbookDict[frozenP]
                else:
                    orderbookDict[frozenP] = frozenOrderbookDict[frozenP]
                    bid_price_levels += 1
                frozenOrderbookDict.pop(frozenP)
                frozenApplSeqNumLs = frozenApplSeqNumDict[frozenP]
                for frozenApplSeqNum in frozenApplSeqNumLs:
                    bid_order_nums += 1
                    new_qty = np.int32(frozenOrderDict[frozenApplSeqNum][1])
                    if frozenP in bidnq1:
                        bidnq1[frozenP] = bidnq1[frozenP] + (frozenApplSeqNum,)
                    else:
                        bidnq1[frozenP] = (frozenApplSeqNum,)
                    bidnq2[frozenApplSeqNum] = new_qty
                    bid_qty += new_qty
                    bid_amount += new_qty * frozenP
                    frozenOrderDict.pop(frozenApplSeqNum)
                    # Attention: insertion_bid is not used in unfrozen order cases
                    # if frozenP in self.insertion_bid:
                    #     self.insertion_bid[frozenP] += new_qty
                frozenApplSeqNumDict.pop(frozenP)

            if side == 2:
                if frozenP in orderbookDict:
                    orderbookDict[frozenP] += frozenOrderbookDict[frozenP]
                else:
                    orderbookDict[frozenP] = frozenOrderbookDict[frozenP]
                    ask_price_levels += 1
                frozenOrderbookDict.pop(frozenP)
                frozenApplSeqNumLs = frozenApplSeqNumDict[frozenP]
                for frozenApplSeqNum in frozenApplSeqNumLs:
                    ask_order_nums += 1
                    new_qty = np.int32(frozenOrderDict[frozenApplSeqNum][1])
                    if frozenP in asknq1:
                        asknq1[frozenP] = asknq1[frozenP] + (frozenApplSeqNum,)
                    else:
                        asknq1[frozenP] = (frozenApplSeqNum,)
                    asknq2[frozenApplSeqNum] = new_qty
                    ask_qty += new_qty
                    ask_amount += new_qty * frozenP
                    # if frozenP in self.insertion_ask:
                    #     self.insertion_ask[frozenP] += new_qty
                    frozenOrderDict.pop(frozenApplSeqNum)
                frozenApplSeqNumDict.pop(frozenP)

        return orderbookDict, frozenOrderbookDict, frozenOrderDict, frozenApplSeqNumDict, bid_price_levels, bid_order_nums, bidnq1, bidnq2, bid_qty, bid_amount, ask_price_levels, ask_order_nums, \
               asknq1, asknq2, ask_qty, ask_amount

    def updateMktInfo(self, caa, exchgTm, ApplSeqNum, isImprove, record):
        curBidP = sorted(self.bidDict.keys(), reverse=True)[:self.outputLv]
        curAskP = sorted(self.askDict.keys())[:self.outputLv]

        if len(self.askDict) != 0:
            self.ask1p = curAskP[0]
        else:
            self.ask1p = curBidP[0] + 100

        if len(self.bidDict) != 0:
            self.bid1p = curBidP[0]
        else:
            self.bid1p = curAskP[0] - 100

        if (self.skey >= 2300000) & (self.date >= 20200824):
            self.maxLmtBuyP = max(round(self.ask1p * 1.02 + self.EPSILON, -2), self.ask1p + 100)
            self.minLmtSellP = min(round(self.bid1p * 0.98 + self.EPSILON, -2), self.bid1p - 100)

        if record:
            self.caaLs.append(caa)
            self.exchgTmLs.append(exchgTm)
            self.ApplSeqNumLs.append(ApplSeqNum)
            self.bboImproveLs.append(isImprove)

            curBidQ = [self.bidDict[i] for i in curBidP]
            curBidN = [len(self.bidnq1[i]) for i in curBidP]
            self.bidp += [curBidP + [0] * (self.outputLv - len(curBidP))]
            self.bidq += [curBidQ + [0] * (self.outputLv - len(curBidQ))]
            self.bidn += [curBidN + [0] * (self.outputLv - len(curBidN))]

            curAskQ = [self.askDict[i] for i in curAskP]
            curAskN = [len(self.asknq1[i]) for i in curAskP]
            self.askp += [curAskP + [0] * (self.outputLv - len(curAskP))]
            self.askq += [curAskQ + [0] * (self.outputLv - len(curAskQ))]
            self.askn += [curAskN + [0] * (self.outputLv - len(curAskN))]

            self.cum_volume_ls.append(self.cum_volume)
            self.cum_amount_ls.append(self.cum_amount)
            self.cum_trades_cnt.append(self.trades)
            self.closeLs.append(self.close)

            self.hasMktLeft = 0

            self.calcVwapInfo()

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

    def getSimMktInfo(self):
        bidp = pd.DataFrame(self.bidp, columns=['bid%sp' % i for i in range(1, self.outputLv + 1)])
        bidq = pd.DataFrame(self.bidq, columns=['bid%sq' % i for i in range(1, self.outputLv + 1)])
        bidn = pd.DataFrame(self.bidn, columns=['bid%sn' % i for i in range(1, self.outputLv + 1)])

        askp = pd.DataFrame(self.askp, columns=['ask%sp' % i for i in range(1, self.outputLv + 1)])
        askq = pd.DataFrame(self.askq, columns=['ask%sq' % i for i in range(1, self.outputLv + 1)])
        askn = pd.DataFrame(self.askn, columns=['ask%sn' % i for i in range(1, self.outputLv + 1)])

        mdData = pd.DataFrame({'caa': self.caaLs, 'time': self.exchgTmLs, 'ApplSeqNum': self.ApplSeqNumLs,
                               'cum_trades_cnt': self.cum_trades_cnt,
                               'cum_volume': self.cum_volume_ls, 'cum_amount': self.cum_amount_ls,
                               'close': self.closeLs,
                               'bboImprove': self.bboImproveLs})

        aggDf = pd.DataFrame([self.total_bid_qty, self.total_ask_qty,
                              self.total_bid_vwap, self.total_ask_vwap,
                              self.total_bid_levels, self.total_ask_levels,
                              self.total_bid_orders_num, self.total_ask_orders_num]).T
        aggCols = ['total_bid_quantity', 'total_ask_quantity',
                   'total_bid_vwap', 'total_ask_vwap',
                   'total_bid_levels', 'total_ask_levels',
                   'total_bid_orders', 'total_ask_orders']
        aggDf.columns = aggCols

        lst = [mdData, bidp, bidq, bidn, askp, askq, askn, aggDf]
        mdData = pd.concat(lst, axis=1, sort=False)
        mdData['skey'] = self.skey
        mdData['date'] = self.date

        targetPLs = ['bid%sp' % i for i in range(self.outputLv, 0, -1)] + ['ask%sp' % i for i in
                                                                           range(1, self.outputLv + 1)]
        targetQLs = ['bid%sq' % i for i in range(self.outputLv, 0, -1)] + ['ask%sq' % i for i in
                                                                           range(1, self.outputLv + 1)]
        targetNLs = ['bid%sn' % i for i in range(self.outputLv, 0, -1)] + ['ask%sn' % i for i in
                                                                           range(1, self.outputLv + 1)]

        for col in ['cum_amount', 'close', 'total_bid_vwap', 'total_ask_vwap'] + targetPLs:
            mdData[col] = mdData[col] / 10000
            mdData[col] = mdData[col].fillna(0)
        for col in ['close'] + targetPLs:
            mdData[col] = mdData[col].astype(float).round(2)

        targetCols = (
                    ['skey', 'date', 'time', 'caa', 'ApplSeqNum', 'cum_volume', 'cum_amount', 'cum_trades_cnt', 'close',
                     'bboImprove'] + \
                    targetPLs + targetQLs + targetNLs + aggCols)
        mdData = mdData[targetCols].reset_index(drop=True)

        for col in (['cum_volume', 'total_bid_quantity', 'total_ask_quantity'] + targetQLs):
            mdData[col] = mdData[col].fillna(0).astype('int64')
        for col in ['skey', 'total_bid_levels', 'total_ask_levels', 'cum_trades_cnt',
                    'total_bid_orders', 'total_ask_orders', 'bboImprove'] + targetNLs:
            mdData[col] = mdData[col].astype('int32')
        for col in ['time']:
            mdData[col] = (mdData[col] * 1000).astype('int64')
        for col in ['cum_amount']:
            mdData[col] = mdData[col].astype(float).round(2)
        return mdData



