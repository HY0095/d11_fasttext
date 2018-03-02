# -*- coding:utf-8 -*-
import time
import datetime


def timestamp_datetime(value):
    value = time.localtime(int(value))
    dt = time.strftime('%Y-%m-%d %H:%M:%S', value)
	return dt


def datetime_timestamp(dt):
    time.strptime(dt, '%Y-%m-%d %H:%M:%S')
    stamp = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
    return int(stamp)


def get_day(t1, t2):
    # Both t1 and t2 format as "2012-03-27 06:53:21"
    t1 = time.strptime(t1, "%Y-%m-%d %H:%M:%S")
    t2 = time.strptime(t2, "%Y-%m-%d %H:%M:%S")
    y, m, d, H, M, S = t1[0:6]
    datetime_1 = datetime.datetime(y, m, d, H, M, S)
    y, m, d, H, M, S = t2[0:6]
    datetime_2 = datetime.datetime(y, m, d, H, M, S)
    day_diff = (datetime_1-datetime_2).days
    return int(day_diff)

