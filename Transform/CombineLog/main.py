# -*- coding: utf-8 -*-
import __init__
from Transform.Common.JHWrite import JHWrite
import sys
import time


def get_path(path_format, ts):
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(ts))
    hhmm = time.strftime("%H%M", time.localtime(ts))
    combine_path = path_format % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
    return combine_path

if __name__ == "__main__":

    if "combinebiqu" in sys.argv:
        delay = 20
        ts = time.time() - delay*60

        path1 = "/data1/logs/transformsaaslogs/biqu/%(yyyymmdd)s/%(hhmm)s.log.gz"
        path2 = "/data1/logs/transformsaaslogs/BIQU_ANDROID/%(yyyymmdd)s/%(hhmm)s.log.gz"
        dest_path = "/data1/logs/transformsaaslogs/biqu_all/%(yyyymmdd)s/%(hhmm)s.log.gz"
        path1 = get_path(path1, ts)
        path2 = get_path(path2, ts)
        dest_path = get_path(dest_path, ts)
        JHWrite.combinefiles([path1, path2], dest_path)

    if "store" in sys.argv:
        start_tm = time.mktime(time.strptime('2016-10-23+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-10-23+23:59:00', '%Y-%m-%d+%H:%M:%S'))
        while start_tm <= end_tm:
            ts = start_tm
            path1 = "/data1/logs/transformsaaslogs/biqu/%(yyyymmdd)s/%(hhmm)s.log.gz"
            path2 = "/data1/logs/transformsaaslogs/BIQU_ANDROID/%(yyyymmdd)s/%(hhmm)s.log.gz"
            dest_path = "/data1/logs/transformsaaslogs/biqu_all/%(yyyymmdd)s/%(hhmm)s.log.gz"
            path1 = get_path(path1, ts)
            path2 = get_path(path2, ts)
            dest_path = get_path(dest_path, ts)

            JHWrite.combinefiles([path1, path2], dest_path)

            start_tm += 60