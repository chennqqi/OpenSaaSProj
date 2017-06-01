# -*- coding: utf-8 -*-
from IPStorage import IPStorage
from DBClient.PyMongoClient import PyMongoClient
from pymongo.operations import *


class IPStorageMG(IPStorage):

    def __init__(self):
        self.client = PyMongoClient()
        self.conn = self.client.getConn()

    def store(self, _id, **kwargs):
        op = UpdateOne({"_id": _id}, {"$set": dict({}, **kwargs)}, False)
        self.client.bulkWrite("jh", "UserIP", [op])

    def storeItem(self, _id, key, value):
        pass

    def bulkStore(self, data):
        op = []
        for key in data:
            _id = key
            op.append(UpdateOne({"_id": _id}, {"$set": dict({}, **data[key])}, False))
        self.client.bulkWrite("jh", "UserIP", op)

if __name__ == "__main__":
    tester = IPStorageMG()
    tester.bulkStore({"8.8.8.8": dict(test="test777")})