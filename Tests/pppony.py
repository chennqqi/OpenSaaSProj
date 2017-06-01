# -*- coding: utf-8 -*-
import pony
from abc import ABCMeta


class UpperAttrMetaclass(type):
    def __new__(cls, name, bases, dct):
        print type(cls), cls, type(cls.__class__), cls.__class__.__name__, cls.__name__
        print type(cls.__class__)
        attrs = ((name, value) for name, value in dct.items() if not name.startswith('__'))
        uppercase_attr  = dict((name.upper(), value) for name, value in attrs)
        return type.__new__(cls, name, bases, uppercase_attr)



UpperAttrMetaclass('666', (), {})