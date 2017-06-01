# -*- coding: utf-8 -*-
import __init__
from DBClient.PyMongoClient import PyMongoClient
import time
import datetime

def usetimeDistribute(num, appkey="BIQU_ANDROID", delta=120):
# def usetimeDistribute(num, appkey="biqu", delta=120):
    curday = datetime.datetime.today().strftime("%Y%m%d")
    dayStr = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400*num))
    client = PyMongoClient()
    result = {}
    # for item in client.find(appkey, "uvfile", {"tm": dayStr, "jhd_userkey": userkey}):
    for item in client.find(appkey, "uvfile", {"tm": dayStr}):
        opas = ["action", "page", "in", "end"]
        uid = item["jhd_userkey"]
        end_sum = item["item_add"].get("end", 0)
        opatms = list(set(reduce(lambda a, b: a+b, map(lambda opa: item["item_count"].get(opa, {}).get("opatm", []), opas))))
        opatms.sort()
        opsdtsmps = map(lambda opatm: int(time.mktime(time.strptime("".join([curday, opatm]), "%Y%m%d%H:%M:%S"))), opatms)
        tmp = [0, ]
        for opastamp, pos in zip(opsdtsmps, range(len(opsdtsmps)-1)):
            a = opsdtsmps[pos]
            b = opsdtsmps[pos+1]
            tmp.append(b-a)
        # print(uid, end_sum, sum([i for i in tmp if i <= delta]), tmp)
        # print(uid, end_sum, sum([i for i in tmp if i <= delta]), opatms)
        total_opatm = sum([i for i in tmp if i <= delta])
        if total_opatm != 0:
            result.setdefault(uid, total_opatm)
    # print("0 user num", i)
    # print(sum([result[uid] for uid in result])/float(len(result)))
    # print(result, len(result))
    return result








if __name__ == "__main__":
    usetimeDistribute(2)
