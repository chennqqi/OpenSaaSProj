# -*- coding: utf-8 -*-
import requests
import random

data = {"ip": "124.65.163.106", "random": random.uniform(0, 1), "coord": "gcj02"}
data = {"ip": "220.175.40.224", "random": random.uniform(0, 1)}

# https://www.opengps.cn/Data/IP/IPLocHiAcc.ashx

r = requests.post("https://www.opengps.cn/Data/IP/IPLocHiAcc.ashx", data=data)
print r.text

print "befror set cookie", r.cookies
print r.headers
print "after set cookie", r.cookies
print r.headers