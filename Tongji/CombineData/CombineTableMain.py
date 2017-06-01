# -*- coding: utf-8 -*-
import __init__
from tables import biqu as biqutables
from CombineTable import CombineTable
import sys

if __name__ == "__main__":

    if "biqu" in sys.argv:
        if len(sys.argv) == 3:
            num = int(sys.argv[2])
        else:
            num = 1
        combiner = CombineTable()
        dbname = biqutables["dbname"]
        appkey_plat_pairs = (("biqu", "ios"), ("biqu_android", "android"))
        combineappkey = "biqu_all"
        for tablename in biqutables["tables"]:
            combiner.combinetable_fromappkey(num, dbname, appkey_plat_pairs, tablename, combineappkey)

    if "store" in sys.argv:
        if len(sys.argv) == 3:
            num = int(sys.argv[2])
        else:
            num = 1
        days = [i for i in range(1, num+1)]
        days.reverse()
        for i in days:
            combiner = CombineTable()
            dbname = biqutables["dbname"]
            appkey_plat_pairs = (("biqu", "ios"), ("biqu_android", "android"))
            combineappkey = "biqu_all"
            for tablename in biqutables["tables"]:
                combiner.combinetable_fromappkey(i, dbname, appkey_plat_pairs, tablename, combineappkey)