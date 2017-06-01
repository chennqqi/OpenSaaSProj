# -*- coding: utf-8 -*-
import time
# from SaaSConfig.config import log_path_format_full


def find_value_fromdict_listkey(dict_a, find_key, result):
    assert isinstance(dict_a, dict), "dict_a is not type(dict)"
    assert isinstance(find_key, list), "find_key is not type(list)"
    if isinstance(find_key, list):
        for x in range(len(dict_a)):
            temp_key = dict_a.keys()[x]
            temp_value = dict_a[temp_key]
            if temp_key in find_key:
                result[temp_key] = temp_value
            elif isinstance(temp_value, dict):
                find_value_fromdict_listkey(temp_value, find_key, result)
            else:
                continue
        values = []
        [values.append(result.get(key, None)) for key in find_key]
        return values


def find_value_fromdict_singlekey(dict_a, find_key):
    assert isinstance(dict_a, dict), "dict_a is not type(dict)"
    if find_key in dict_a:
        return dict_a[find_key]
    for x in range(len(dict_a)):
        temp_key = dict_a.keys()[x]
        temp_value = dict_a[temp_key]
        # if temp_key == find_key:
        #     return temp_value
        if isinstance(temp_value, dict):
            result = find_value_fromdict_singlekey(temp_value, find_key)
            if result != None:
                return result
        else:
            continue


def getDay(dateStr, format, interval):
    dateTime = time.mktime(time.strptime(dateStr, format))
    return time.strftime(format, time.localtime(dateTime + 86400 * interval))


def getDayDelta(dateStr1, dateStr2, dateformat="%Y%m%d"):
    from datetime import datetime
    dayDelta = (datetime.strptime(dateStr1, dateformat) - datetime.strptime(dateStr2, dateformat)).days
    return dayDelta


def getWeekDays(last_week, startDay=time.strftime("%Y-%m-%d", time.localtime(time.time())), dateformat="%Y%m%d"):
    from datetime import datetime
    from datetime import timedelta
    last_week_end_day = (datetime.fromtimestamp(time.mktime(time.strptime(startDay.replace("-", ""), "%Y%m%d"))) -
                         timedelta(weeks=last_week)).isoweekday()
    result = {}
    for i in range(last_week_end_day+(last_week-1)*7, last_week_end_day+(last_week-1)*7+7):
        day = (datetime.fromtimestamp(time.mktime(time.strptime(startDay.replace("-", ""), "%Y%m%d"))) -
        timedelta(days=i)).strftime(dateformat)
        result.setdefault(day, i)
    return result


def getMonthDays(last_month, relativeYearMonth=time.strftime("%Y-%m", time.localtime(time.time())), dateformat="%Y%m%d"):
    pass

def getMonthFirstDay(last_month, relativeYearMonth = time.strftime("%Y-%m", time.localtime(time.time())), dateformat = "%Y%m%d"):
    from datetime import datetime
    from datetime import timedelta
    startDay = (datetime.fromtimestamp(time.mktime(time.strptime(relativeYearMonth.replace("-", "")+"01", "%Y%m%d"))) -
                         timedelta(days=1 if last_month != 0 else 0)).strftime("%Y%m") + "01"
    monthFirstDay = datetime.fromtimestamp(time.mktime(time.strptime(startDay, "%Y%m%d"))).strftime(dateformat)
    relativeMonth = monthFirstDay[:6]
    if last_month != 0:
        last_month = last_month - 1 if last_month > 0 else last_month + 1
    if last_month != 0:
        return getMonthFirstDay(last_month, relativeMonth, dateformat)
    else:
        return monthFirstDay

def getMonthEndDay(last_month, relativeMonth = time.strftime("%Y-%m", time.localtime(time.time())), dateformat = "%Y%m%d"):
    year, month = int(relativeMonth.split("-")[0]), int(relativeMonth.split("-")[1])
    delta_year = int((month - (last_month - 1)) / 12)
    target_month = (month - (last_month - 1)) % 12


def getWeekFirstDay(last_week, startDay = time.strftime("%Y-%m-%d", time.localtime(time.time())), dateformat = "%Y%m%d"):
    from datetime import datetime
    from datetime import timedelta
    last_week_end_day = (datetime.fromtimestamp(time.mktime(time.strptime(startDay.replace("-", ""), "%Y%m%d"))) -
                         timedelta(weeks=last_week)).isoweekday()
    result = {}
    day = (datetime.fromtimestamp(time.mktime(time.strptime(startDay.replace("-", ""), "%Y%m%d"))) -
    timedelta(days=last_week_end_day+(last_week-1)*7+7-1)).strftime(dateformat)
    result.setdefault(day, last_week_end_day+(last_week-1)*7+7-1)
    return result

if __name__ == "__main__":
    import time
    a = time.time()
    # for i in range(100000):
    print(getWeekDays(1))
    # print(getDayDelta("20160805", "20160506"))
    print(time.time() - a)