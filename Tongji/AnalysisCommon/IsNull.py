# -*- coding: utf-8 -*-

class IsNull(object):

    @staticmethod
    def judge(obj):
        if not obj:
            return True
        elif isinstance(obj, unicode):
            if obj.lower() == "null":
                return True
        elif isinstance(obj, str):
            if obj.lower() == "null":
                return True
        return False

if __name__ == "__main__":
    print(IsNull.judge(unicode("Null")))
    print(IsNull.judge(u"Null"))
    print(IsNull.judge("Null"))
    print(IsNull.judge(None))
    print(IsNull.judge(""))


