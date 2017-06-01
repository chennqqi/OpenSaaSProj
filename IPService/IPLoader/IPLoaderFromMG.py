# -*- coding: utf-8 -*-
import __init__
import threading
import time
from IPLoader import IPLoader
from DBClient.PyMongoClient import PyMongoClient
from SaaSCommon.StackSet import StackSet

import logging

logger = logging.getLogger(__file__)


class IPLoaderFromMG(IPLoader):
    '''
    load 需要定位的IP地址 -> IPContainer(StackSet)
    '''

    def __init__(self):
        self.client = PyMongoClient()
        self.IPContainer = StackSet()

    def load(self):
        t = threading.Thread(target=self.load_thread, name="Thread_LoadIPFromMG")
        t.setDaemon(True)
        t.start()

    def load_thread(self, once_sleep = 30):
        logger.info("IP Loader Starting.......")
        _counter = 0
        while True:
            if self.IPContainer.size() >= 10000:
                time.sleep(once_sleep)
                continue
            size_before = self.IPContainer.size()
            cur = self.client.find("jh", "UserIP", {"timestamp": {"$gte": time.time()-5*60}, "city": {"$exists": False}})
            for item in cur:
                ip = item["_id"]
                self.IPContainer.push(ip)
                _counter += 1
            size_after = self.IPContainer.size()
            # print self.IPContainer.items
            logger.info("Total Read IP: %s, IPContainer has ip: %d, load ip: %d" % (_counter, size_after, size_after-size_before))
            time.sleep(once_sleep)

    def iter(self):
        while True:
            try:
                item = self.IPContainer.pop()
            except IndexError:
                import traceback
                logger.warning("IPContainer is empty!")
                time.sleep(10)
                continue
            yield item


if __name__ == "__main__":
    tester = IPLoaderFromMG()
    tester.load()
    for item in tester.iter():
        time.sleep(5)
        print item

