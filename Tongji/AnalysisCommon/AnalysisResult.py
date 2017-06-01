# -*- coding: utf-8 -*-
# from abc import ABCMeta
# from abc import abstractmethod
import __init__
from SaaSCommon.DiskDict import DiskDict


class AnalysisResult(object):

    def __init__(self, dict_type = "native"):
        if dict_type == "native":
            self.__result = {}
        else:
            self.__result = DiskDict()

    # @abstractmethod
    @property
    def result(self):
        return self.__result

    @result.setter
    def result(self, data):
        self.__result = data

    @property
    def transformresult(self):
        return self.__transformresult

    @transformresult.setter
    def transformresult(self, data):
        self.__transformresult = data

    def cleardata(self):
        self.__result.clear()

if __name__ == "__main__":

    tester = AnalysisResult()
    print(tester.result)
    tester.result = {1: 1}
    print(tester.result)