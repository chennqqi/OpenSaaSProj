# -*- coding: utf-8 -*-
import inspect

def get_current_function_name():
    return inspect.stack()[1][3]


def test():
    print get_current_function_name()

class Test(object):

    def __init__(self):
        pass

    def test666(self):
        print get_current_function_name()

if __name__ == "__main__":
    # test()
    tester = Test()
    tester.test666()