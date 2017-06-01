# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod

class MapData(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def mapCollector(self, *args, **kwargs):
        pass