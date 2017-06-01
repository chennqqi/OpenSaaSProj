# -*- coding: utf-8 -*-
from MapDataMongo import MapDataMongo
from MapDataLog import MapDataLog

class MapDataFactory(object):

    def __init__(self):
        pass

    @staticmethod
    def create(objname):
        if objname == "mongo":
            return MapDataMongo()
        elif objname == "log":
            return MapDataLog()