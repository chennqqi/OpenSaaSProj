# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod


class Transform(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def transform(self, *args, **kwargs):
        pass

    @abstractmethod
    def retransform(self, *args, **kwargs):
        pass

    @abstractmethod
    def deformation(self, *args, **kwargs):
        pass