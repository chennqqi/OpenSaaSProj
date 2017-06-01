# -*- coding: utf-8 -*-
from print_file import print_file
import inspect

print_file()


def test():
    print("inspectsa", inspect.stack()[0][3])
    print("inspectsa", inspect.stack())
    print(__name__)
print(__name__)

test()