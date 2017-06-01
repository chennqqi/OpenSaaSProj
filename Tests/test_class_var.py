# -*- coding: utf-8 -*-

class test:



    def __init__(self):
        var = 1

    def set_var(self, var):
        self.__init__.var = var

    def _print(self):
        print("var is", self.__init__.var)


a = test()
a._print()
print(a.var)
a.set_var(6)

a._print()
print(a.var)

b = test()
a._print()
print(b.var)

if __name__ == "__main__":
    pass