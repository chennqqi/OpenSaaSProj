# -*- coding: utf-8 -*-
import time

def while_yeild():
    while True:
        yield "6666"
        time.sleep(1)


if __name__ == "__main__":
    for item in while_yeild():
        print item