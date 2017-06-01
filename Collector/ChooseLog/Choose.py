# -*- coding: utf-8 -*-
import __init__
import time
import gzip
import json
import logging
import sys

from Config import ncf_360
from JHWrite import JHWrite
from JHOpen import JHOpen

logger = logging.getLogger(__file__)

items = [ncf_360, ]


def choose(timestamp = time.time()-10*60):
    yyyymmdd = time.strftime("%Y%m%d", time.localtime(timestamp))
    hhmm = time.strftime("%H%M", time.localtime(timestamp))

    for item in items:
        input_path = item["input_path_format"] % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
        output_path = item["out_path_format"] % {"yyyymmdd": yyyymmdd, "hhmm": hhmm}
        filter_chain = item["filter_chain"]
        f = JHOpen.readLines(input_path)
        for line in f:
            try:
                data = json.loads(line)
                ua = data["ua"].strip()
                for filter_container in filter_chain:
                    filter_worker = filter_container["filter"]
                    if filter_worker(ua):
                        JHWrite.write(output_path, line)
                        break
            except:
                err_info = sys.exc_info()
                logger.error(repr(err_info))
        JHWrite.finished(iszip=True)


if __name__ == "__main__":
    if "store" in sys.argv:
        start_tm = time.mktime(time.strptime('2017-03-18+00:00:00', '%Y-%m-%d+%H:%M:%S'))
        end_tm = time.mktime(time.strptime('2017-03-18+23:59:00', '%Y-%m-%d+%H:%M:%S'))
        while start_tm <= end_tm:
            a = time.time()
            try:
                choose(start_tm)
            except:
                import traceback
                print traceback.print_exc()
            print "@%s, cost: %.4f" % (time.strftime("%Y-%m-%d+%H:%M", time.localtime(start_tm)), time.time()-a)
            start_tm += 60