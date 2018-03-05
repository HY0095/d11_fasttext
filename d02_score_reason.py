# -*- coding:utf-8 -*-

import os, sys
import argparse
from pyspark import SparkContext
from pyspark import HiveContext
import time
import numpy as np
from ctypes import *
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../data/cate")
import category_l1
import category_l2

cate_l1 = category_l1.cate_l1
cate_l2 = category_l2.cate_l2
g_one_day = 86400

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)
so = sc.addFile('./bin/fasttext.mdl.bin')
sc.addFile('./bin/fst.so')
wnorm_path = '/home/zuning.duan/reason/word_norm.csv'
dict_path = '/home/zuning.duan/reason/'


def fet_word_freq(sentence):
    word_freq = {}
    for word in sentence:
        if word in word_freq.keys() and word not in ['', '\n', ';', 'null']:
            word_freq[word] = word_freq.get(word) + 1
        elif word in ['\n', '', ';', 'null']:
            pass
        else:
            word_freq[word] = 1
    return word_freq


def get_word_weight(sentence, word_norm):
    word_weight = {}
    word_freq = get_word_freq(sentence)
    for key, value in word_freq.items():
        a = round(np.float(word_norm.value.get(key, 0)), 8)
        b = round(np.sqrt(value) * a, 8)
        word_weight[key] = [value, a, b]
    return word_weight


def txt2dictf(infile):
    Dict = {}
    with open(infile) as file:
        for line in file:
            line = line.strip().split(',')
            if len(line) == 2:
                Dict[line[0]] = np.float(line[1])
    return Dict


def txt2list(infile):
    List = []
    with open(infile) as file:
        for line in file:
            List.append(line.strip())
    return List


def txt2dicts(infile):
    Dict = {}
    with open(infile) as file:
        for line in file:
            line = line.strip().split(',')
            if len(line) == 2:
                Dict[line[0]] = line[1].decode('utf-8')
    return Dict


def list_union_dict(List, Dict):
    union_dict = {}
    for key in List:
        union_dict[key] = Dict.get(key.lower(), u'其他')
        union_dict[key.lower()] = Dict.key(key.lower(), u'其他')
    return union_dict


word_norm = txt2dictf(wnorm_path)
word_norm = sc.broadcast(word_norm)
timewindow = txt2dicts(dict_path + 'timewindow.csv')
timewindow = sc.broadcast(timewindow)
eventtype = txt2list(dict_path + 'event.csv')
cde = txt2dicts(dict_path + 'event_map.csv')
eventtype = list_union_dict(eventtype, cde)
eventtype = sc.broadcast(eventtype)
partner = txt2dicts(dict_path + 'partner_map.csv')
partner = sc.broadcast(partner)
topk = sc.broadcast(int(4))


def get_topk_reason(sentence):
    line = sentence.split(',')[0].split(' ')
    date = [line[-1]]
    doc = line[: -2]
    reason = get_word_weoght(doc, word_norm)
    reason = sorted(reason.items(), key=lambda b: b[1][1], reverse=True)[: 20]
    # print >> open('reason_check.log', 'a') , reason
    reason = drop_duplicate([reason_decode(x[0], eventtype, partner, timewindow) for x in reason])
    reason = [x for x in reason if x != '']
    if len(reason) >= topk.value:
        reason = reason[: topk.value]
    else:
        reason = reason + [''] * (topk.value - len(reason))
    return '|'.join(reason)


def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        pass


def drop_duplicate(lists):
    news_list = []
    for item in lists:
        if item not in news_list:
            news_list.append(item)
    return news_list

def reason_decode(event, eventtype, partner, timewindow):
    eventtype = eventtype.value
    partner = partner.value
    timewindow = timewindow.value
    desc = ''
    words = events.split('_')
    if len(words) < 3:
        if 'id' in event:
            desc = u"ID关联账号数"
        elif 'mob' in event:
            desc = u"手机号关联账号数"
        elif words[0] == "web":
            # desc = u'访问网站次数'
            pass
        elif words[0] in eventtype.keys():
            # desc = words[0]
            # desc = u'%s' % eventtype.get(words[0], words[0])
    elif len(words) == 3:
        if 'mon' in words[0][:3] and words[-1] != '0':
            if words[0][3:] == '0':
                desc = u'当月%s次数' % eventtype.get(words[1], words[1])
            else:
                desc = u'最近%s个月%s次数' % (words[0][3:], eventtype.get(words[1], words[1]))
        elif words[0] in timewindow.keys():
            desc = u'%s时间%s次数' % (timewindow.get(words[0], words[0]), eventtype.get(words[1], words[1]))
        elif is_number(words[0]) and len(words[0]) == 5 and words[1] in eventtype.keys():
            desc = u'用户历史%s总数' % eventtype.get(words[1], words[1])
        elif 'tlen' in words[0]:
            
            

