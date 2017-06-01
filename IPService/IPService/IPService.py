# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class IPService(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        pass