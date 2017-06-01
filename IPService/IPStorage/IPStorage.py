# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class IPStorage(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def store(self, ip, **kwargs):
        pass

    @abstractmethod
    def storeItem(self, ip, key, value):
        pass