# coding: utf-8
from Transform import Transform
import time
from LogStore import LogStore
import json
import sys
reload(sys)
sys.setdefaultencoding("utf8")
from JHOpen import JHOpen


def main(timestamp,
         src_file_format,
         errlog_path_format,
         filename_format,
         devuser_path_format = "/data1/logs/transformsaaslogs/devuserlog/%(yyyymmdd)s/%(hhmm)s.log"):
    transform = Transform(timestamp=timestamp*1000) # 毫秒
    yyyymmdd = time.strftime('%Y%m%d', time.localtime(timestamp))
    yyyymmddhhmm = time.strftime('%Y%m%d%H%M', time.localtime(timestamp))
    hhmm = time.strftime('%H%M', time.localtime(timestamp))
    src_file = src_file_format % {"yyyymmdd": yyyymmdd, "yyyymmddhhmm": yyyymmddhhmm}
    errlog_path = errlog_path_format % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
    devuser_path = devuser_path_format % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
    print src_file
    # with open(src_file) as f:
    errlognum = 0
    for line in JHOpen().readLines(src_file):
        if not line:
            continue
        try:
            logs = transform.transform(line)
            for item in logs:
                datatype = item['jhd_datatype']
                item['jhd_userkey'] = item['jhd_userkey'].strip()
                filename = filename_format % {"yyyymmdd": yyyymmdd, "hhmm": hhmm, 'datatype': datatype}
                # unicode 转码中文
                try:
                    line_out = json.dumps(item, ensure_ascii=False).encode('utf-8')
                except:
                    line_out = json.dumps(item, ensure_ascii=False)
                if item.get("isdevuser", False) == False:
                    LogStore(filename, line_out)
                else:
                    LogStore(devuser_path, line_out)
        except:
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            errinfo.append(line)
            LogStore(errlog_path, json.dumps(map(lambda item: item.strip(), errinfo)))
            errlognum += 1
    if errlognum:
        print("\t".join(["@" + yyyymmddhhmm, "errlognum: %d" % errlognum, "err info: %s" % errlog_path]))
        print("".join(["endline", '-' * 10]))
    LogStore.finished(iszip=True)

if __name__ == "__main__":

    if 'jhsaaslogs_android' in sys.argv:
        src_file_format = "/data1/nginxlogs/jhsaaslogs/access_jhlogs.%(yyyymmddhhmm)s"
        errlog_path_format = "/data1/logs/transformsaaslogs/err/%(yyyymmdd)s/%(hhmm)s.err"
        filename_format = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
        timestamp = int(time.time()-60*5)
        main(timestamp, src_file_format=src_file_format, errlog_path_format=errlog_path_format, filename_format=filename_format)

    if 'jhsaaslogs_ios' in sys.argv:
        src_file_format = "/data1/nginxlogs/jhsaaslogs_ios/access_jhlogs.%(yyyymmddhhmm)s"
        errlog_path_format = "/data1/logs/transformsaaslogs/err/%(yyyymmdd)s/%(hhmm)s.err"
        filename_format = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
        timestamp = int(time.time()-60*5)
        main(timestamp, src_file_format=src_file_format, errlog_path_format=errlog_path_format, filename_format=filename_format)

    if 'store_ios' in sys.argv:
        # startstamp = time.mktime(time.strptime('20160501+000100', '%Y%m%d+%H%M%S'))
        # endstamp = time.mktime(time.strptime('20160602+000000', '%Y%m%d+%H%M%S'))
        startstamp = time.mktime(time.strptime('20161028+000000', '%Y%m%d+%H%M%S'))
        # startstamp = time.mktime(time.strptime('20160808+000000', '%Y%m%d+%H%M%S'))
        endstamp = time.mktime(time.strptime('20161028+170000', '%Y%m%d+%H%M%S'))

        while startstamp <= endstamp:
            today = time.strftime("%Y%m%d", time.localtime(time.time()))
            today_begin_stamp = time.mktime(time.strptime("".join([today, "000000"]), "%Y%m%d%H%M%S"))
            try:
                if endstamp >= today_begin_stamp:
                    src_file_format = "/data1/nginxlogs/jhsaaslogs_ios/access_jhlogs.%(yyyymmddhhmm)s"
                    errlog_path_format = "/data1/transformsaaslogs/err/err/%(yyyymmdd)s/%(hhmm)s.err"
                    filename_format = "/data1/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
                    main(endstamp,
                         src_file_format=src_file_format,
                         errlog_path_format=errlog_path_format,
                         filename_format=filename_format)
                else:
                    src_file_format = "/data1/nginxlogs/jhsaaslogs_ios/%(yyyymmdd)s/access_jhlogs.%(yyyymmddhhmm)s.gz"
                    errlog_path_format = "/data1/transformsaaslogs/err/%(yyyymmdd)s/%(hhmm)s.err"
                    filename_format = "/data1/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
                    main(endstamp,
                         src_file_format=src_file_format,
                         errlog_path_format=errlog_path_format,
                         filename_format=filename_format)
            except:
                import traceback
                print(traceback.print_exc())
            endstamp -= 60

    if 'store_android' in sys.argv:
        # startstamp = time.mktime(time.strptime('20160501+000100', '%Y%m%d+%H%M%S'))
        # endstamp = time.mktime(time.strptime('20160602+000000', '%Y%m%d+%H%M%S'))
        startstamp = time.mktime(time.strptime('20161029+000000', '%Y%m%d+%H%M%S'))
        # startstamp = time.mktime(time.strptime('20160808+000000', '%Y%m%d+%H%M%S'))
        # endstamp = time.mktime(time.strptime('20161030+142200', '%Y%m%d+%H%M%S'))
        endstamp = time.mktime(time.strptime('20161029+235900', '%Y%m%d+%H%M%S'))

        while startstamp <= endstamp:
            today = time.strftime("%Y%m%d", time.localtime(time.time()))
            today_begin_stamp = time.mktime(time.strptime("".join([today, "000000"]), "%Y%m%d%H%M%S"))
            try:
                if endstamp >= today_begin_stamp:
                    src_file_format = "/data1/nginxlogs/jhsaaslogs/access_jhlogs.%(yyyymmddhhmm)s"
                    errlog_path_format = "/data1/logs/transformsaaslogs/err/%(yyyymmdd)s/%(hhmm)s.err"
                    filename_format = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
                    main(endstamp,
                         src_file_format=src_file_format,
                         errlog_path_format=errlog_path_format,
                         filename_format=filename_format)
                else:
                    src_file_format = "/data1/nginxlogs/jhsaaslogs/%(yyyymmdd)s/access_jhlogs.%(yyyymmddhhmm)s.gz"
                    errlog_path_format = "/data1/logs/transformsaaslogs/err/%(yyyymmdd)s/%(hhmm)s.err"
                    filename_format = "/data1/logs/transformsaaslogs/%(datatype)s/%(yyyymmdd)s/%(hhmm)s.log"
                    main(endstamp,
                         src_file_format=src_file_format,
                         errlog_path_format=errlog_path_format,
                         filename_format=filename_format)
            except:
                import traceback
                print(traceback.print_exc())
            endstamp -= 60