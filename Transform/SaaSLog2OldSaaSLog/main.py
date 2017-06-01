# -*- coding: utf-8 -*-
import __init__
from Transform.Common.JHOpen import JHOpen
from Transform.Common.JHWrite import JHWrite
from SaaSLog2OldSaaSLog import SaaSLog2OldSaaSLog
from Transform.Common.JHDecorator import fn_timer
import time
import sys
import json
import os


@fn_timer
def logappend(paths, combine_path, mode="a"):
    oldsaasfile = paths[0]
    newsaasfile = paths[1]
    JHWrite.combinefiles([oldsaasfile, newsaasfile], combine_path)
    transformer = SaaSLog2OldSaaSLog()
    for line in JHOpen.readLines(combine_path):
        if not line:
            continue
        try:
            data = json.loads(line)
            result = transformer.deformation(data)
            if not result:
                continue
            if "jhd_session" in result:
                JHWrite.write(filename=oldsaasfile, line=json.dumps(result), mode=mode)
            else:
                JHWrite.write(filename=newsaasfile, line=json.dumps(result), mode=mode)
        except:
            import traceback
            print(traceback.print_exc())
    JHWrite.finished()

@fn_timer
def logcombine(paths, combine_path):
    oldsaasfile = paths[0]
    newsaasfile = paths[1]
    if os.path.exists(combine_path):
        return
    else:
        JHWrite.combinefiles([oldsaasfile, newsaasfile], combine_path)

def get_paths(ts):
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(ts))
    hhmm = time.strftime("%H%M", time.localtime(ts))
    data_format = {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
    paths = [path_format % data_format for path_format in paths_format]
    return paths

def get_combine_path(ts):
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(ts))
    hhmm = time.strftime("%H%M", time.localtime(ts))
    combine_path = combine_path_format % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
    return combine_path

if __name__ == "__main__":
    paths_format = [
        "/data1/logs/feeling/%(yyyymmdd)s/%(hhmm)s.log",
        "/data1/logs/transformsaaslogs/feeling/%(yyyymmdd)s/%(hhmm)s.log.gz"
    ]
    combine_path_format = "/data1/logs/feeling_combine/%(yyyymmdd)s/%(hhmm)s.cmb.log.gz"

    if "logcombine" in sys.argv:
        delay = 20
        ts = time.time() - delay*60
        paths = get_paths(ts)
        combine_path = get_combine_path(ts)
        logcombine(paths, combine_path)

    if "logappend" in sys.argv:
        delay = 25
        ts = time.time() - delay*60
        paths = get_paths(ts)
        combine_path = get_combine_path(ts)
        logappend(paths, combine_path)


    if "store" in sys.argv:
        start_tm = time.mktime(time.strptime('2016-10-24+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2016-10-24+18:10:00', '%Y-%m-%d+%H:%M:%S'))
        while start_tm <= end_tm:
            paths = get_paths(start_tm)
            combine_path = get_combine_path(start_tm)
            logappend(paths, combine_path)
            start_tm += 60