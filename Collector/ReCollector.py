# coding: utf-8
import mmap
import os
import sys
import time


# 失败数据重传，默认重试次数为2次，
# 日志格式(\t分隔)：日期（%Y-%m-%d+%H:%M:%S）、faild_0(重试失败次数)、datatype、CMD
def reCollect(path):
    if not os.path.exists(path):
        return
    f = os.open(path, os.O_RDWR)
    mm = mmap.mmap(f, 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ|mmap.PROT_WRITE)
    pre = mm.tell()
    while mm.readline():
        cur = mm.tell()
        try:
            mm.seek(pre)
            line = mm.readline()
            print line
            items = line.split("\t")
            num = int(items[1].split("_")[1])
            if num >= 2:
                mm.seek(cur)
                pre = cur
                continue
            items[1] = "_".join([items[1].split("_")[0], str(num+1)])
            cmd = items[4]
            result = os.system(cmd)
            if result == 0:
                items[2] = "A"
                mm[pre:cur] = "\t".join(items)
                mm.flush()
            else:
                mm[pre:cur] = "\t".join(items)
                mm.flush()
            mm.seek(cur)
            pre = cur
        except:
            import traceback
            print(traceback.print_exc())
            mm.seek(cur)
            pre = cur
    mm.close()
    os.close(f)


if __name__ == "__main__":
    if "test" in sys.argv:
        reCollect("/data1/logs/collector/collectorinfo.20160824")

    if "routine" in sys.argv:
        yyyymmdd = time.strftime("%Y%m%d", time.localtime(time.time()-61*60))
        reCollect("/data1/logs/collector/collectorinfo.%(yyyymmdd)s" % {"yyyymmdd": yyyymmdd})
