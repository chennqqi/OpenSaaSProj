# -*- coding: utf-8 -*-
import  time
import os
import sys
from Config import remote_host_list
from LogStore import LogStore


def collectFiles(timestamp = time.time()-10*60, tm = {}, remote_dir_format="", local_dir_part_format="", local_dir_format="", datatypeList=[], is_store = False):
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(timestamp)) if tm.get('yyyymmdd', 0) == 0 else tm.get('yyyymmdd', 0)
    hhmm = time.strftime("%H%M", time.localtime(timestamp)) if tm.get('hhmm', 0) == 0 else tm.get('hhmm', 0)
    log_path = "/data1/logs/collector/collectorinfo.%(yyyymmdd)s" % {"yyyymmdd": yyyymmdd}
    for datatype in datatypeList:
        remote_dir = remote_dir_format % {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype}
        local_dir = local_dir_format % {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype}
        if not os.path.exists(local_dir):
            os.system("mkdir -p %s" % "/".join(local_dir.split("/")[:-1]))
        souce_files = []
        a = time.time()
        for (remote_host, part) in zip(remote_host_list, range(0, len(remote_host_list))):
            b = time.time()
            try:
                local_dir_part = local_dir_part_format % \
                                 {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype, 'part': part}
                cmd = "scp root@%(remote_host)s:%(remote_dir)s %(local_dir_part)s" % \
                          {'remote_host': remote_host, 'remote_dir': remote_dir, 'local_dir_part': local_dir_part}
                print cmd
                result = os.system(cmd)
                # print cmd
                if is_store == False:
                    if result != 0:
                        time.sleep(2)
                        result = os.system(cmd)
                        if result != 0:
                            LogStore(log_path, "\t".join([time.strftime("%Y-%m-%d+%H:%M:%S", time.localtime(time.time())), "faild_0", "#", datatype, cmd]), mode="a+")
                if os.path.exists(local_dir_part):
                    souce_files.append(local_dir_part)
            except:
                import traceback
                print traceback.print_exc()
            print "remote_host: ", remote_host, " %s " % datatype, " cost ", time.time() - b
        souce_files_str = " ".join(souce_files)
        souce_files_str = souce_files_str.strip()
        if os.path.exists(local_dir):
            os.system("rm -f %(local_dir)s" % {"local_dir": local_dir})
            print("remove file", local_dir)
        if souce_files:
            os.system("cat %(souce_files)s >> %(local_dir)s && rm -rf %(souce_files)s" % \
                      {'souce_files': souce_files_str, 'local_dir': local_dir}
                      )
        print datatype, " cost ", time.time() - a
    LogStore.finished(iszip=True)


if __name__ == "__main__":
    if 'collector' in sys.argv:
        # /data1/logs/transformNew/feeling/
        datatype_list = ['guaeng', 'guagua', 'feeling']
        collectFiles(remote_dir_format="/data1/logs/transformNew/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log",
                     local_dir_part_format="/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log",
                     local_dir_format="/data1/logs/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log",
                     datatypeList=datatype_list)

    if "test" in sys.argv:
        print datatype_list

    if "collectorsaash5" in sys.argv:
        timestamp = time.time() - 10 * 60
        def getAppkey():
            datatype_list = []
            try:
                from MysqlClient import MysqlClient
                client = MysqlClient("saas_meta")
                # result = client.getAppkey()
                # datatype_list = [item[1] for item in result if item[2] == "h5"]
                result = client.getAppkey_H5()
                datatype_list = [item["appkey"] for item in result]
                client.closeMysql()
            except:
                import traceback
                print traceback.print_exc()
            return datatype_list

        datatype_list = getAppkey()
        print(datatype_list)
        collectFiles(remote_dir_format="/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                     local_dir_part_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
                     local_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                     datatypeList=datatype_list,
                     timestamp=timestamp)
        # 从整体中，筛选符合条件的日志
        try:
            from ChooseLog.Choose import choose
            choose(timestamp)
        except:
            import traceback
            print traceback.print_exc()

    if "collectorsaas" in sys.argv:
        def getAppkey():
            datatype_list = []
            try:
                from MysqlClient import MysqlClient
                client = MysqlClient("saas_meta")
                result = client.getAppkey_app()
                datatype_list = [item["appkey"] for item in result]
                client.closeMysql()
            except:
                import traceback
                print traceback.print_exc()
            datatype_list.append("hbtv") if "hbtv" not in datatype_list else None
            return datatype_list

        datatype_list = getAppkey()
        print datatype_list
        collectFiles(remote_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                     local_dir_part_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
                     local_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                     datatypeList=datatype_list)

    if "collectorh5" in sys.argv:
        datatypeList = ["feeling_H5"]
        collectFiles(\
remote_dir_format="/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz", \
local_dir_part_format="/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
local_dir_format = "/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz", \
datatypeList=datatypeList\
)
    if "saas_store" in sys.argv:
        def getAppkey():
            datatype_list = []
            try:
                from MysqlClient import MysqlClient
                client = MysqlClient("saas_meta")
                result = client.getAppkey_app()
                datatype_list = [item["appkey"] for item in result]
                client.closeMysql()
            except:
                import traceback
                print traceback.print_exc()
            datatype_list.append("hbtv") if "hbtv" not in datatype_list else None
            return datatype_list
        datatype_list = ["huiyue_ad", "huiyue_ios"]
        startstamp = time.mktime(time.strptime('20170320+000000', '%Y%m%d+%H%M%S'))
        endstamp = time.mktime(time.strptime('20170324+110600', '%Y%m%d+%H%M%S'))
        while startstamp <= endstamp:
            try:
                collectFiles(timestamp=endstamp, remote_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                             local_dir_part_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
                             local_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                             datatypeList=datatype_list, is_store=True)
            except:
                import traceback
                print traceback.print_exc()
            endstamp -= 60


    if 'store' in sys.argv:
        def getAppkey():
            datatype_list = []
            try:
                from MysqlClient import MysqlClient
                client = MysqlClient("saas_meta")
                result = client.getAppkey_H5()
                datatype_list = [item["appkey"] for item in result]
                client.closeMysql()
            except:
                import traceback
                print traceback.print_exc()
            datatype_list.append("hbtv") if "hbtv" not in datatype_list else None
            return datatype_list
        datatypeList = getAppkey()
        startstamp = time.mktime(time.strptime('20170317+000000', '%Y%m%d+%H%M%S'))
        endstamp = time.mktime(time.strptime('20170317+235900', '%Y%m%d+%H%M%S'))
        while startstamp <= endstamp:
            try:
                collectFiles(\
timestamp=endstamp, \
remote_dir_format="/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz", \
local_dir_part_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
local_dir_format = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz", \
datatypeList=datatypeList\
)
            except:
                import traceback
                print traceback.print_exc()
            endstamp -= 60

    if 'test' in sys.argv:
        datatypeList = ["hbtv"]
        tm = {"yyyymmdd": "20160812", "hhmm": "1028"}
        collectFiles(tm=tm, remote_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz", datatypeList=datatypeList)
