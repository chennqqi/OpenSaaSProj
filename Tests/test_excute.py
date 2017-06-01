# -*- coding: utf-8 -*-
import sys


def excute(*args, **kwargs):
    print "args", args
    print "kwargs", kwargs


if __name__ == "__main__":
    excute()