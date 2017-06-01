# -*- coding: utf-8 -*-
import time
# proj_dir = "/data/py/test/SaaSProj"
from os import sys, path
import os
sys.path.append(path.dirname(path.dirname(path.abspath(__file__)))) # 获取祖父路径（../SaaSProj）, 把项目根目录加入环境变量
base_path = path.dirname(path.dirname(path.abspath(__file__)))
proj_dir = base_path
# print(proj_dir) # C:\PycharmProjects\SaaSProj
# print("-"*20)
# print(os.getcwd() # C:\PycharmProjects\SaaSProj\SaaSConfig)
# print(os.path.basename(__file__)) # config.py
# print(os.path.abspath(__file__)) # C:\PycharmProjects\SaaSProj\SaaSConfig\config.py
# print(os.path.dirname(__file__)) # C:/PycharmProjects/SaaSProj/SaaSConfig


global log_path_format, log_path_format_full, uvfile_path_format, mongo_ids, node_name
# global err_log_path_format_full

mongo_ids = [1, 2]
node_name = "node_1"

log_path_format = "/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/????.log"
log_path_format_full = "/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"

# err_log_path_format_full = "/data1/logs/transform/errlog/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"

uvfile_path_format = "/data1/logs/uvfile/%(datatype)s/uvfile_%(yyyymmdd)s.log"

# ipdata_path = "/".join([proj_dir, "SaaSTools/IPtoLoc/"])

def set_log_path_format(path_format):
    global log_path_format
    log_path_format = path_format



def set_log_path_format_full(path_format):
    global log_path_format_full
    log_path_format_full = path_format
    LogPathConfig["default"] = path_format


def set_uvfile_path_format(path_format):
    global uvfile_path_format
    uvfile_path_format = path_format
    UVFilePathConfig["default"] = path_format


LogPathConfig = {
    "default": log_path_format_full,
    "datatype_demo": "/test/path/"
}


UVFilePathConfig = {
    "default": uvfile_path_format,
    "datatype_demo": "/test/path/"
}


def get_log_path(num=1, datatype = 'feeling'):
    # type: (object) -> object
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-num*86400))
    return log_path_format % {'yyyymmdd': yyyymmdd, 'datatype': datatype}


def get_file_path(**args):
    # type: (object) -> object
    assert "yyyymmdd" in args, "lack 'yyyymmdd'"
    assert "hhmm" in args, "lack 'hhmm'"
    assert "last" in args, "lack 'last'"
    assert "datatype" in args, "lack 'datatype'"
    yyyymmdd = args["yyyymmdd"]
    hhmm = args["hhmm"]
    tm_stamp = time.mktime(time.strptime("+".join([yyyymmdd, hhmm]), '%Y%m%d+%H%M'))
    result = []
    path_format = LogPathConfig.get(args.get("datatype", "default"), LogPathConfig["default"])
    for i in range(0, args["last"]):
        tm_stamp_delta = tm_stamp - i*60
        hhmm = time.strftime("%H%M", time.localtime(tm_stamp_delta))
        result.append(path_format % {"datatype": args["datatype"], "yyyymmdd": yyyymmdd, "hhmm": hhmm})
        result = list(set(result))
        result.sort()
    return result


def get_uvfile_path(num, datatype, iszip = False):
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-86400*num))
    path_format = UVFilePathConfig["default" if datatype not in UVFilePathConfig else datatype]
    path = path_format %  {"datatype": datatype, "yyyymmdd": yyyymmdd}
    if iszip and not path.endswith("gz"):
        path = "".join([path, ".gz"])
    return path



# ### mongodb
# mongo_ip = "101.201.145.120"
# mongo_port = 27017
# ### mysql
# mysql_host = "injhkj01.mysql.rds.aliyuncs.com"
# mysql_port = 3306
# mysql_user = "jhkj"
# mysql_passwd = "jhkj_jhkj"

if __name__ == "__main__":
    print(get_uvfile_path(1, "guagua"))
    # for path in get_file_path(datatype='feeling', yyyymmdd='20160709', hhmm='2359', last = 1500):
    #     print(path)
