# coding: utf-8
import os
import traceback

class LogStore(object):
    out_file = {}

    def __init__(self, filename, line, mode="w"):
        self._Logging(filename, line, mode)

    @staticmethod
    def _Logging(filename, line, mode="w"):
        path = "/".join(filename.split("/")[:-1])
        if not os.path.exists(path):
            os.system("mkdir -p %s" % path)
        out = LogStore.out_file[filename] if LogStore.out_file.get(filename, None) else open(filename, mode)
        LogStore.out_file.setdefault(filename, out)
        print >> out, line

    @staticmethod
    def finished(iszip=False):
        for filename in LogStore.out_file.keys():
            try:
                LogStore.out_file[filename].close()
                if iszip:
                    os.system("gzip -f %s" % filename)
            except:
                print(traceback.print_exc())
            try:
                del LogStore.out_file[filename]
            except:
                print(traceback.print_exc())
