import sys
import time
import gzip
from Transform import Transform
from LogStore import LogStore
import json

src_logpath_format = "/data1/nginxlogs/jhlogs/%(yyyymmdd)s/access_jhlogs.%(yyyymmddhhmm)s.gz"
src_logpath_format = "/data1/nginxlogs/jhlogs/access_jhlogs.%(yyyymmddhhmm)s"
errlog_path_format = "/data1/logs/transform/err/%(yyyymmdd)s/%(hhmm)s.err"
filename_format = "/data1/logs/transformNew/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"


def main(timestamp, src_logpath_format="/data1/nginxlogs/jhlogs/access_jhlogs.%(yyyymmddhhmm)s"):
    yyyymmddhhmm = time.strftime('%Y%m%d%H%M', time.localtime(timestamp))
    yyyymmdd = time.strftime('%Y%m%d', time.localtime(timestamp))
    hhmm = time.strftime('%H%M', time.localtime(timestamp))
    src_logpath = src_logpath_format % {"yyyymmddhhmm": yyyymmddhhmm, "yyyymmdd": yyyymmdd}
    errlog_path = errlog_path_format % {"yyyymmddhhmm": yyyymmddhhmm, "yyyymmdd": yyyymmdd, "hhmm": hhmm}
    transform = Transform()
    if src_logpath.endswith(".gz"):
        src_logpath_file = gzip.open(src_logpath)
    else:
        src_logpath_file = open(src_logpath)
    for line in src_logpath_file:
        try:
            for log in transform.transform(line):
                datatype = log["jhd_datatype"]
                # if datatype == "guaeng":
                #     print datatype, src_logpath_format
                #     print log
                filename = filename_format % {"yyyymmdd": yyyymmdd, "hhmm": hhmm, "datatype": datatype}
                # print filename
                log_line = json.dumps(log)
                LogStore(filename, log_line)
        except Exception, e:
            # import traceback
            # print traceback.print_exc()
            LogStore(errlog_path, "%s, %s"%(e, line))

    if not src_logpath.endswith(".gz"):
        src_logpath_file.close()
    LogStore.finished(iszip=False)

if __name__ == "__main__":
    if 'normal' in sys.argv:
        timestamp = int(time.time()-60*5)
        main(timestamp)

    if 'guagua' in sys.argv:
        timestamp = int(time.time()-60*5)
        main(timestamp, src_logpath_format = "/data1/nginxlogs/guagua/access_guagua.%(yyyymmddhhmm)s")

    if 'saas' in sys.argv:
        timestamp = int(time.time()-60*5)
        main(timestamp, src_logpath_format = "/data1/nginxlogs/jhsaaslogs/access_jhlogs.%(yyyymmddhhmm)s")

    if 'store' in sys.argv:
        # startstamp = time.mktime(time.strptime('20160501+000100', '%Y%m%d+%H%M%S'))
        # endstamp = time.mktime(time.strptime('20160602+000000', '%Y%m%d+%H%M%S'))
        startstamp = time.mktime(time.strptime('20160812+000000', '%Y%m%d+%H%M%S'))
        # startstamp = time.mktime(time.strptime('20160808+000000', '%Y%m%d+%H%M%S'))
        endstamp = time.mktime(time.strptime('20160812+103000', '%Y%m%d+%H%M%S'))
        while startstamp <= endstamp:
            # main(endstamp, src_logpath_format = "/data1/nginxlogs/jhlogs/%(yyyymmdd)s/access_jhlogs.%(yyyymmddhhmm)s.gz")
            try:
                # main(endstamp, src_logpath_format = "/data1/nginxlogs/guagua/%(yyyymmdd)s/access_guagua.%(yyyymmddhhmm)s.gz")
                main(endstamp, src_logpath_format = "/data1/nginxlogs/jhsaaslogs/access_jhlogs.%(yyyymmddhhmm)s")
            except:
                import traceback
                print traceback.print_exc()
            endstamp -= 60
