# -*- coding: utf-8 -*-
import time
import threading


class WorkerLimit(object):
    '''
    控制任务执行频率
    EG:
        limiter = WorkerLimit(60, 180) # 每分钟最多执行180次
        limiter.inc() # 执行次数加1
        limiter.remainCount() # 剩余执行次数
    '''

    def __init__(self, sec_delta, max_limit, begin_timestamp = None):
        self.total = 0
        self.counter = 0
        self.max_limit = max_limit
        self.sec_delta = sec_delta
        if begin_timestamp is None:
            self.begin_timestamp = time.time()
        t = threading.Thread(name="Thread_Monitor(sec_delta=%d,max_limit=%d)"%(sec_delta, max_limit), target=self.monitor)
        t.start()

    def monitor(self):
        while True:
            if time.time() - self.begin_timestamp >= self.sec_delta:
                self.total += self.counter
                self.counter = 0
                self.begin_timestamp = time.time()
            else:
                time.sleep(1)

    def getTotalInc(self):
        if self.total == 0:
            return self.counter
        else:
            return self.total + self.counter

    def remainCount(self):
        return self.max_limit - self.counter

    def inc(self, value = 1):
        self.counter += value

    def speed(self):
        return self.counter/((time.time()-self.begin_timestamp)+1)


if __name__ == "__main__":
    tester = WorkerLimit(10, 30)
    while True:
        if tester.remainCount() <= 0:
            print("speed: ", tester.speed())
            print("remainCount: ", tester.remainCount())
            print("sleep 3")
            time.sleep(3)
        else:
            print("speed: ", tester.speed())
            print("remainCount: ", tester.remainCount())
            print("inc")
            tester.inc()