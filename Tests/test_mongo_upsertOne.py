# -*- coding: utf-8 -*-
import __init__
from DBClient.PyMongoClient import PyMongoClient
from pymongo.operations import *
import time


client = PyMongoClient()


ips = [
"114.242.248.80",
"113.45.95.64",
"117.136.52.59",
"117.136.66.136",
"219.72.202.182",
"117.190.78.171",
"110.96.185.182",
"139.205.135.41"]

# op = []
# for ip in ips:
#     query_update = {"timestamp": time.time()}
#     op.append(UpdateOne({"_id": ip}, query_update, upsert=True))
#
# client.bulkWrite("jh", "UserIP", op)



