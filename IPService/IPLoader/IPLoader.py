# -*- coding: utf-8 -*-
from abc import abstractmethod
from abc import ABCMeta


class IPLoader(object):

    __metaclass__ = ABCMeta

    @staticmethod
    def load(self):
        pass

    @staticmethod
    def iter(self):
        pass



