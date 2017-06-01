# -*- coding: utf-8 -*-
import datetime
import json
import glob
import urlparse
import gzip
import numpy as np

from data import define_path

def load_sample_data():
    result = {}
    with open('/data1/logs/clickhouse_dumps/tmp/user_level_all.log') as f:
        for line in f:
            items = line.split("\t")
            # appkey = items[0].strip()
            pushid = items[1].strip()
            first_login_time = items[2].strip()
            charge_day = items[3].strip()
            level = int(items[4].strip())
            result.setdefault(pushid, [first_login_time, set(), set()])
            result[pushid][1].add(charge_day)
            result[pushid][2].add(level)

    return result

users_login = load_sample_data()


def main(num):
    yyyymmdd = (datetime.datetime.now() - datetime.timedelta(days=num)).date().strftime("%Y%m%d")
    path_raw = "/data1/logs/transformsaaslogs/ncf_ws/%(yyyymmdd)s/????.log.gz" % {"yyyymmdd": yyyymmdd}
    paths = glob.iglob(path_raw)
    vector_length = len(define_path)
    result = {}
    for path in paths:
        with gzip.open(path) as f:
            for line in f:
                try:
                    data = json.loads(line)
                    pushid = data["pushid"]
                    optype = data["type"].strip()
                    if optype != "page":
                        continue
                    if pushid not in users_login:
                        continue
                    uri = data["uri"].strip()

                    uri_path = urlparse.urlparse(uri).path.rstrip("/")
                    if uri_path in define_path:
                        user_level = max(users_login[pushid][2])
                        result.setdefault(user_level, {}).setdefault(pushid, [0] * vector_length)
                        index = define_path[uri_path][1]
                        result[user_level][pushid][index] += 1
                except:
                    import traceback
                    print traceback.print_exc()
                    print line
    # tmp = {}
    for level in result:
        users_vector = result[level]
        # user_num = len(result[level])
        # users_matrix = np.array([item for pushid in users_vector for item in users_vector[pushid]])
        users_matrix = np.array([users_vector[pushid] for pushid in users_vector])
        user_avg = []
        print users_matrix.shape
        row_n, col_n = users_matrix.shape
        print row_n, col_n
        for i in range(col_n):
            user_avg.append(sum(users_matrix[:,i])/float(row_n))
        print level, user_avg

if __name__ == "__main__":
    main(1)





