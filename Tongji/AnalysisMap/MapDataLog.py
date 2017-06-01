# -*- coding: utf-8 -*-
from MapData import MapData
from LogLoder import LogLoder
from SaaSConfig.config import get_file_path
import json
import time

class MapDataLog(MapData, LogLoder):

    def __init__(self):
        pass

    def pipline(self, path):
        for line in super(MapDataLog, self).pipline(path):
            if not line:
                continue
            data = json.loads(line[0])
            yield data

    def mapCollector(self, datatype, num, ruleFunction):
        pass
        # dayStr = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400 * num))
        yyyymmddhhmm = time.strftime("%Y%m%d%H%M", time.localtime(time.time() - 86400 * num))
        paths = get_file_path(datatype=datatype, yyyymmdd=yyyymmddhhmm[:8], hhmm="2359", last=1440)
        result = {}
        for path in paths:
            for data in self.pipline(path):
                try:
                    ruleFunction.rules(result, data)
                except:
                    import traceback
                    print(traceback.print_exc())
        return result

