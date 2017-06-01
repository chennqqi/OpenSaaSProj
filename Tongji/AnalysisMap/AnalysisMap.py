# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod

class AnalysisMap(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.finished = False

    def reset(self):
        self.finished = False

    @abstractmethod
    def rules(self, analysisresult, data, num, *args, **kwargs):
        pass

    @abstractmethod
    def transformresult(self, analysisresult, *args, **kwargs):
        pass