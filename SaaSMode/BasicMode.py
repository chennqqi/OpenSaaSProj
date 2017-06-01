# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class BasicMode(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def builder(self):
        pass

    @abstractmethod
    def merge(self, _old, _new = None):
        pass