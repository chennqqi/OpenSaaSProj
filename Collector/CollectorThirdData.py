# --coding=utf8--
import time
import os
import sys
from Config import remote_host_list
from LogStore import LogStore


def collect(delay=time.time()-10*60, remote_dir_format="", local_dir_part_format="", local_dir_format="", datatypes=[], remote_ips=[]):
    '''
    :param delay: 拉取延迟多少时间的文件，默认10分钟
    :param tm:
    :param remote_dir_format: remote服务器目录格式
    :param local_dir_part_format: 本地存储目录格式
    :param local_dir_format:
    :param datatypeList:
    :param remote_ips:
    :return:
    '''
    # 获取日期
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(delay))
    hhmm = time.strftime("%H%M", time.localtime(delay))
    log_path = "/data1/logs/collector/collectorinfo.%(yyyymmdd)s" % {"yyyymmdd": yyyymmdd}
    # 遍历所有的datatype，迭代获取相应的文件
    for datatype in datatypes:
        # remote目录
        remote_dir = remote_dir_format % {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': datatype}
        # 本地临时存储文件
        if datatype.find("_log") != -1:
            local_datatype = datatype[datatype.find("/")+1:]
        else:
            local_datatype = datatype
        local_dir = local_dir_format % {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': local_datatype}
        # 如果本地文件不存在，则新建
        if not os.path.exists(local_dir):
            os.system("mkdir -p %s" % "/".join(local_dir.split("/")[:-1]))

        #临时文件存储
        souce_files = []
        # 获取远程ip和ip index
        for (remote_host, part) in zip(remote_ips, range(0, len(remote_host_list))):
            
            local_dir_part = local_dir_part_format % \
                             {'yyyymmdd': yyyymmdd, 'hhmm': hhmm, 'datatype': local_datatype, 'part': part}
            #scp 远程文件
            cmd = "scp root@%(remote_host)s:%(remote_dir)s %(local_dir_part)s" % \
                      {'remote_host': remote_host, 'remote_dir': remote_dir, 'local_dir_part': local_dir_part}
            # 失败重试
            result = os.system(cmd)
            if result != 0:
                #time.sleep(2)
                result = os.system(cmd)
                if result != 0:
                    LogStore(log_path, "\t".join([time.strftime("%Y-%m-%d+%H:%M:%S", time.localtime(time.time())), "faild_0", "#", datatype, cmd]), mode="a+")
            if os.path.exists(local_dir_part):
                souce_files.append(local_dir_part)
        souce_files_str = " ".join(souce_files)
        souce_files_str = souce_files_str.strip()
        if os.path.exists(local_dir):
            os.system("rm -f %(local_dir)s" % {"local_dir": local_dir})
            print("remove file", local_dir)
        if souce_files:
            os.system("cat %(souce_files)s >> %(local_dir)s && rm -f %(souce_files)s" % \
                      {'souce_files': souce_files_str, 'local_dir': local_dir}
                      )
    LogStore.finished()


if __name__ == "__main__":
    if 'biqu' in sys.argv:
        # /data1/logs/transformNew/feeling/
        remote_host_list=['103.37.158.249']
        datatype_list = ['android_log/BIQU_ANDROID', "ios_log/biqu", "h5_log/BQ_H5"]
        collect(delay=time.time() - 15 * 60,
		        remote_dir_format="/data/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                local_dir_part_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
                local_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                datatypes=datatype_list,
                remote_ips=remote_host_list)

    if 'biqu_daily' in sys.argv:
    # /data1/logs/transformNew/feeling/
       remote_host_list = ['103.37.158.249']
       # datatype_list = ['android_log/BIQU_ANDROID', "ios_log/biqu"]
       datatype_list = ["ios_log/biqu"]
       datatype_list = ['android_log/BIQU_ANDROID', "h5_log/BQ_H5"]
       d_date = '20161009'
       d_time = ''
       for h in range(0, 24):
           for m in range(0, 60):
               d_time = "%.2d%.2d" % (h, m)
               d_datetime = d_date + d_time
               delay = time.mktime(time.strptime(d_datetime, "%Y%m%d%H%M"))
               collect(delay=delay,
                       remote_dir_format="/data/transform/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                       local_dir_part_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s_%(part)d.log.gz",
                       local_dir_format="/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log.gz",
                       datatypes=datatype_list,
                       remote_ips=remote_host_list)
