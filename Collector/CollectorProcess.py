# -*- coding: utf-8 -*-
import threading
import time
import os
import subprocess
import logging

from Config import remote_host_list
from JHWrite import JHWrite

logger = logging.getLogger(__file__)

def collector(datatype, remote_dir_format, local_dir_part_format, local_dir_format, timestamp = time.time()-10*60, **default):
    '''
    :param remote_dir_format: /data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz
    :param local_dir_part_format: /data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz
    :param local_dir_format: /data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz
    :param timestamp: 1489980591.763
    :param default: {"yyyymmdd": "20160812", "hhmm": "1028"}
    :return:
    '''
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(timestamp)) if default.get('yyyymmdd', None) else default['yyyymmdd']
    hhmm = time.strftime("%H%M", time.localtime(timestamp)) if default.get('hhmm', None) else default.get['hhmm']
    # 格式化日志路径
    remote_dir = remote_dir_format % {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype}
    local_dir = local_dir_format % {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype}
    log_path = "/data1/logs/collector/collectorinfo.%(yyyymmdd)s" % {"yyyymmdd": yyyymmdd}

    base_path, file_name = local_dir.rsplit("/", 1)
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    souce_files = []
    for (remote_host, part) in zip(remote_host_list, range(0, len(remote_host_list))):
        try:
            local_dir_part = local_dir_part_format % \
                             {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype, 'part': part}
            cmd = "scp root@%(remote_host)s:%(remote_dir)s %(local_dir_part)s" % \
                  {'remote_host': remote_host, 'remote_dir': remote_dir, 'local_dir_part': local_dir_part}
            retcode = subprocess.call(cmd)
            if retcode == 0:
                logger.info("SCP was terminated by signal %d, CMD: %s" % (retcode, cmd))
            else:
                logger.warning("SCP was terminated by signal %d, CMD: %s" % (retcode, cmd))
                continue
            souce_files.append(local_dir_part)
        except OSError as e:
            logger.error("Execution failed: %s" % repr(e))
    JHWrite.combinefiles(souce_files, local_dir)


class CollectorProcess(object):

    def __init__(self):
        pass

    def get_appkeys(self, ty):
        datatype_list = []
        # collectorsaas
        if ty == "transformsaaslogs":
            try:
                from MysqlClient import MysqlClient
                client = MysqlClient("saas_meta")
                result = client.getAppkey()
                datatype_list = [item[1] for item in result if item[2] != "h5"]
                client.closeMysql()
            except:
                import traceback
                print traceback.print_exc()
            datatype_list.append("hbtv") if "hbtv" not in datatype_list else None
        # collectorsaash5
        elif ty == "transformh5":
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
        # collectorh5
        elif ty == "feeling_H5":
            datatype_list.append("feeling_H5")
        elif ty == "transformNew":
            datatype_list += ['guaeng', 'guagua', 'feeling']
        return datatype_list

