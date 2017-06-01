# -*- coding: utf-8 -*-
import gzip
import os


class JHOpen(object):
    def __init__(self):
        pass

    @staticmethod
    def readLines(path):
        if path.endswith(".gz"):
            if os.path.exists(path):
                try:
                    out = gzip.open(path)
                    for line in out:
                        yield line.strip()
                except:
                    with open(path) as f:
                        for line in f:
                            yield line.strip()
            else:
                print("Path is not exists: %s" % path)
                yield
        else:
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        for line in f:
                            yield line.strip()
                except:
                    out = gzip.open(path)
                    for line in out:
                        yield line.strip()
            else:
                print("Path is not exists: %s" % path)
                yield




if __name__ == "__main__":
    for line in JHOpen().readLines("C:/1652.log.gz"):
        print(line.strip())