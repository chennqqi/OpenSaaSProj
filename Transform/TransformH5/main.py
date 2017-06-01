# -*- coding: utf-8 -*-
import sys
import time
import gzip
from Transform import Transform
from LogStore import LogStore
import json
import re

ip_pattern = re.compile(r'''"((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))"''')


def main(timestamp, src_logpath_format="/data1/nginxlogs/jhsaaslogs_h5/access_jhlogs.%(yyyymmddhhmm)s",
         errlog_path_format="/data1/logs/transformh5/err/%(yyyymmdd)s/%(hhmm)s.err",
         filename_format="/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"):
    yyyymmddhhmm = time.strftime('%Y%m%d%H%M', time.localtime(timestamp))
    yyyymmdd = time.strftime('%Y%m%d', time.localtime(timestamp))
    hhmm = time.strftime('%H%M', time.localtime(timestamp))
    src_logpath = src_logpath_format % {"yyyymmddhhmm": yyyymmddhhmm, "yyyymmdd": yyyymmdd}
    errlog_path = errlog_path_format % {"yyyymmddhhmm": yyyymmddhhmm, "yyyymmdd": yyyymmdd, "hhmm": hhmm}
    print src_logpath
    if src_logpath.endswith(".gz"):
        src_logpath_file = gzip.open(src_logpath)
    else:
        src_logpath_file = open(src_logpath)
    try:
        transform = Transform(timestamp=timestamp)
    except:
        transform = Transform()
    for line in src_logpath_file:
        try:
            # ip = line.split(",")[0].strip()
            # # 如果为内网ip，做单独处理
            # try:
            #     if ip.startswith("127"):
            #         ip = ip_pattern.search(line).group(1)
            # except:
            #     import traceback
            #     print(traceback.print_exc())
            data = transform.transform(line)
            # data = json.loads(lod_line)
            # data["ip"] = ip
            if not data:
                continue
            datatype = data["appkey"]
            filename = filename_format % {"datatype": datatype, "yyyymmdd": yyyymmdd, "hhmm": hhmm}
            LogStore(filename, json.dumps(data))
        except Exception, e:
            LogStore(errlog_path, "%s, %s"%(e, line.strip()))

    if not src_logpath.endswith(".gz"):
        src_logpath_file.close()
    LogStore.finished(iszip=True)

if __name__ == "__main__":
    if 'transform_h5' in sys.argv:
        src_logpath_format = "/data1/nginxlogs/jhsaaslogs_h5/access_jhlogs.%(yyyymmddhhmm)s"
        errlog_path_format = "/data1/logs/transformh5/err/%(yyyymmdd)s/%(hhmm)s.err"
        filename_format = "/data1/logs/transformh5/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
        timestamp = int(time.time()-60*5)
        main(timestamp, src_logpath_format=src_logpath_format,
             errlog_path_format=errlog_path_format,
             filename_format=filename_format)

    if 'store' in sys.argv:
        # startstamp = time.mktime(time.strptime('20160501+000100', '%Y%m%d+%H%M%S'))
        # endstamp = time.mktime(time.strptime('20160602+000000', '%Y%m%d+%H%M%S'))
        startstamp = time.mktime(time.strptime('20161124+000000', '%Y%m%d+%H%M%S'))
        # startstamp = time.mktime(time.strptime('20160808+000000', '%Y%m%d+%H%M%S'))
        endstamp = time.mktime(time.strptime('20161206+235900', '%Y%m%d+%H%M%S'))
        while startstamp <= endstamp:
            today = time.strftime("%Y%m%d", time.localtime(time.time()))
            today_begin_stamp = time.mktime(time.strptime("".join([today, "000000"]), "%Y%m%d%H%M%S"))
            # main(endstamp, src_logpath_format = "/data1/nginxlogs/jhlogs/%(yyyymmdd)s/access_jhlogs.%(yyyymmddhhmm)s.gz")
            try:
                if endstamp >= today_begin_stamp:
                    main(endstamp,
                         src_logpath_format = "/data1/nginxlogs/jhsaaslogs_h5/access_jhlogs.%(yyyymmddhhmm)s")
                else:
                    main(endstamp,
                         src_logpath_format="/data1/nginxlogs/jhsaaslogs_h5/%(yyyymmdd)s/access_jhlogs.%(yyyymmddhhmm)s.gz")
                # main(endstamp, src_logpath_format = "/data1/nginxlogs/jhlogs/access_jhlogs.%(yyyymmddhhmm)s")
            except:
                import traceback
                print traceback.print_exc()
            endstamp -= 60
