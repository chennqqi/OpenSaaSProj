# -*- coding: utf-8 -*-
from collections import OrderedDict
from Tongji.StoreCommon.StoreBasicCommon import StoreBasicCommon


class StoreBasicCustom(StoreBasicCommon):

    pass

if __name__ == "__main__":
    tester = StoreBasicCustom()
    print(tester.tablename("datatype", "plat", "mainname"))