# -*- coding: utf-8 -*-
from MapData import MapData
from DBClient.PyMongoClient import PyMongoClient
import time

class MapDataMongo(MapData):

    def __init__(self):
        self.client = PyMongoClient()

    def mapCollector(self, num, event):
        dayStr = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400*num))

        query_aggregate = '''[{"$match": {"tm": "%(dayStr)s", "item_count.%(event)s": {"$exists": true}}}, \
            {"$project": {"uid": "$jhd_userkey", "maps": "$item_count.%(event)s.maps"}}]'''
