# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod
from SaaSCommon.JHOpen import JHOpen
from SaaSConfig.config import get_file_path
from SaaSConfig.config import get_uvfile_path


class PathProperty(object):

    def __init__(self, _path = None, pathtype = None):
        self._path = _path
        self._pathtype = pathtype

    @property
    def pathfull(self):
        return self._path

    @pathfull.setter
    def pathfull(self, _path):
        self._path = _path

    @property
    def pathtype(self):
        return self._pathtype


class LogLoder(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def pipline(self, path):
        for line in JHOpen().readLines(path):
            if not line:
                continue
            yield [line]

    def paths(self, **kwargs):
        logtype = kwargs["logtype"]
        if logtype == "logfile":
            _paths = get_file_path(**kwargs)
            return [PathProperty(_path = _path, pathtype = "logfile") for _path in _paths]
        elif logtype == "uvfile":
            num = kwargs["num"]
            datatype = kwargs["datatype"]
            iszip = kwargs.get("iszip", True)
            _paths = [get_uvfile_path(num, datatype, iszip=iszip)]
            return [PathProperty(_path=_path, pathtype="uvfile") for _path in _paths]

        # 所有类型日志路径
        if isinstance(logtype, list):
            paths = []
            for _logtype in logtype:
                kwargs = dict(kwargs, **{"logtype": _logtype})
                paths += self.paths(**kwargs)
            return paths

class LogLoderTester(LogLoder):
    def pipline(self, path):
        super(LogLoderTester, self).pipline(path)

if __name__ == "__main__":
    parms = dict(
    num = 1,
    yyyymmdd = "20161031",
    hhmm = "2140",
    last = 1440,
    datatype = "feeling",
    logtype = ["logfile", "uvfile"])
    tester = LogLoderTester()
    result = tester.paths(**parms)
    print(result)
    print([item.pathfull for item in result])
    print([item.pathtype for item in result])