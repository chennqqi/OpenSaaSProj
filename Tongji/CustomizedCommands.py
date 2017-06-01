# -*- coding: utf-8 -*-
import __init__
import importlib
import sys

import Tongji.CustomizeCaiyu.Store.StoreCustomized

from DBClient.MysqlData import MysqlData
from SaaSConfig.config import set_uvfile_path_format
from SaaSConfig.config import set_log_path_format_full
from SaaSConfig.config import set_log_path_format

log_path_format = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/????.log.gz"
log_path_format_full = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz"
uvfile_path_format = "/data1/logs/uvfilesaas/%(datatype)s/uvfile_%(yyyymmdd)s.log"

def params(datatype):
    mysqldata = MysqlData()
    dbname, datatype, plat = mysqldata.getCustomParms(datatype)
    return dbname, datatype, plat

config = {
    "caiyu_ad": {
        "storer" : lambda freq: (importlib.import_module('Tongji.CustomizeCaiyu.Store.StoreCustomized').StoreCustomized(store_freq = freq)),
        "config_path": lambda (log_path_format, log_path_format_full, uvfile_path_format): (
            set_log_path_format(log_path_format),
            set_log_path_format_full(log_path_format_full),
            set_uvfile_path_format(uvfile_path_format),
        ),
    "iszip_uvfile": True
    },
    "caiyu_ios_free": {
        "storer": lambda freq: importlib.import_module('Tongji.CustomizeCaiyu.Store.StoreCustomized').StoreCustomized(store_freq = freq),
        "config_path": lambda (log_path_format, log_path_format_full, uvfile_path_format): (
            set_log_path_format(log_path_format),
            set_log_path_format_full(log_path_format_full),
            set_uvfile_path_format(uvfile_path_format),
        ),
        "iszip_uvfile": True
    },
    "ncf_360": {
        "storer": lambda freq: importlib.import_module('Tongji.CustomizeNcf.Store.StoreCustomized').StoreCustomized(store_freq = freq),
        "config_path": lambda (log_path_format, log_path_format_full, uvfile_path_format): (
            set_log_path_format("/noexists.file"),
            set_log_path_format_full("/noexists.file"),
            set_uvfile_path_format("/data1/logs/uvfilesaas/ncf_ws/uvfile_zc2/%(yyyymmdd)s.log"),
        ),
        "iszip_uvfile": False
    },
}


if __name__ == "__main__":

    # format: python CustomizedCommands.py daily 1 可选参数：[caiyu_ad/caiyu_ios_free]
    if "daily" in sys.argv[1]:
        if len(sys.argv) >= 3:
            argv_3 = sys.argv[2]
            if "-" in argv_3:
                start, end = argv_3.split("-")
                days = [i for i in range(int(start), int(end))]
                days.sort(reverse=True)
            elif "~" in argv_3:
                start, end = argv_3.split("~")
                days = [i for i in range(int(start), int(end))]
                days.sort(reverse=True)
            elif "," in argv_3:
                days = map(int, argv_3.split(","))
                days.sort(reverse=True)
            else:
                days = [int(argv_3), ]
        else:
            num = 1

        filter_datatype = []
        if len(sys.argv) >= 4:
            argv_4 = sys.argv[3]
            filter_datatype = argv_4.split(",")

        for datatype in config:
            if len(filter_datatype) > 0 and datatype not in filter_datatype:
                continue
            config[datatype]["config_path"]((log_path_format, log_path_format_full, uvfile_path_format))
            for i in days:
                try:
                    print "\t".join(map(str, [i, datatype]))
                    dbname, datatype, plat = params(datatype)
                    storer = config[datatype]["storer"]("daily")
                    storer.storeByDaily(i, dbname, datatype, plat, ifdel=True, iszip=config[datatype]["iszip_uvfile"])
                except:
                    import traceback
                    print traceback.print_exc()

    if "week" in sys.argv[1]:
        if len(sys.argv) >= 3:
            argv_3 = sys.argv[2]
            if "-" in argv_3:
                start, end = argv_3.split("-")
                days = [i for i in range(int(start), int(end))]
                days.sort(reverse=True)
            elif "~" in argv_3:
                start, end = argv_3.split("~")
                days = [i for i in range(int(start), int(end))]
                days.sort(reverse=True)
            elif "," in argv_3:
                days = map(int, argv_3.split(","))
                days.sort(reverse=True)
            else:
                days = [int(argv_3), ]
        else:
            num = 1

        filter_datatype = []
        if len(sys.argv) >= 4:
            argv_4 = sys.argv[3]
            filter_datatype = argv_4.split(",")

        for datatype in config:
            if len(filter_datatype) > 0 and datatype not in filter_datatype:
                continue
            config[datatype]["config_path"]((log_path_format, log_path_format_full, uvfile_path_format))
            for i in days:
                try:
                    print "\t".join(map(str, [i, datatype]))
                    dbname, datatype, plat = params(datatype)
                    storer = config[datatype]["storer"]("week")
                    storer.storeByWeekThrough(i, dbname, datatype, plat, ifdel=True)
                except:
                    import traceback
                    print traceback.print_exc()



    if "appoint" in sys.argv[1]:
        if len(sys.argv) >= 3:
            argv_3 = sys.argv[2]
            if "-" in argv_3:
                start, end = argv_3.split("-")
                days = [i for i in range(int(start), int(end))]
                days.sort(reverse=True)
            elif "~" in argv_3:
                start, end = argv_3.split("~")
                days = [i for i in range(int(start), int(end))]
                days.sort(reverse=True)
            elif "," in argv_3:
                days = map(int, argv_3.split(","))
                days.sort(reverse=True)
            else:
                days = [int(argv_3), ]
        else:
            num = 1

        filter_datatype = []
        if len(sys.argv) >= 4:
            argv_4 = sys.argv[3]
            filter_datatype = argv_4.split(",")

        argv_5 = sys.argv[4].strip()

        for datatype in config:
            if len(filter_datatype) > 0 and datatype not in filter_datatype:
                continue
            config[datatype]["config_path"]((log_path_format, log_path_format_full, uvfile_path_format))
            for i in days:
                try:
                    print "\t".join(map(str, [i, datatype]))
                    dbname, datatype, plat = params(datatype)
                    storer = config[datatype]["storer"]("daily")
                    storer.storeappointbydaily(i, dbname, datatype, plat, appoint=argv_5, ifdel=True)
                except:
                    import traceback
                    print traceback.print_exc()