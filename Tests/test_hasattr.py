# -*- coding: utf-8 -*-


class A(object):

    def __init__(self):
        self.var_1 = None

    def _hasattr(self):
        print(hasattr(self, "var_1"))


if __name__ == "__main__":

    tester = A()
    tester._hasattr()