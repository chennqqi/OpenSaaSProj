# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class LogControler:

    __metaclass__ = ABCMeta

    @abstractmethod
    def pipeline(self, path):
        pass

    @abstractmethod
    def parse(self, dataDict, modedict, mode):
        pass

    @abstractmethod
    def dataCollect(self, *args, **kwargs):
        pass