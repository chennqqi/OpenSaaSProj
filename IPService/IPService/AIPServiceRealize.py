# -*- coding: utf-8 -*-
import __init__
import time
import os
from os import path, sys
import logging
from IPService import IPService
from IPLoader.IPLoaderFromMG import IPLoaderFromMG
from IPtoLOC.IPtoLOCBaiduapi import IPtoLOCBaiduapi
from IPStorage.IPStorageMG import IPStorageMG

file_name = os.path.join(path.dirname(path.abspath(__file__)), "".join([__file__, ".log"]))

logger = logging.getLogger(__file__)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=file_name,
                    filemode='a')


class AIPServiceRealize(IPService):
    '''
    IPService接口的一个实现：
    IPLoader：加载待定位的IP
    IPtoLOC*api：IP转化接口
    IPStorage：保存定位的IP
    '''

    def __init__(self):
        self.loader = IPLoaderFromMG()
        # 启动 load 线程
        self.loader.load()
        self.ip_to_loc = IPtoLOCBaiduapi()
        self.ip_storage = IPStorageMG()

    def start(self):
        for item in self.loader.iter():
            ip = item
            try:
                loc_dict = self.ip_to_loc.loc(ip)
            except:
                logger.error(sys.exc_info())
                # import traceback
                # print traceback.print_exc()
                continue
            self.ip_storage.store(ip, **loc_dict)

if __name__ == "__main__":
    tester = AIPServiceRealize()
    tester.start()


