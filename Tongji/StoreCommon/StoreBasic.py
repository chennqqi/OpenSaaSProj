# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod



class StoreBasic(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def store(self, *args, **kwargs):
        pass