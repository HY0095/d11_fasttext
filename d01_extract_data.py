# -*- coding:utf-8 -*-

import os, sys
import argparse
from pyspark import SparkContext
from pyspark import HiveContext
import time
import datetime
import utils

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)


def udf_entity_extract(id, id_value, apply_dt, e_type='accountmobile'):
    es = {}
    if id_value is None or id is None: return '%s:' % id
    if len(apply_dt) == 10: apply_dt = '%s 00:00:00' % apply_dt
    for x in id_value:
        event_time = utils.timestamp_datetime(x.get('eventoccurtime'))
        date_diff = utils.get_day(apply_dt, event_time)
        if (event_time > apply_dt) and (date_diff <= 720):
            continue
        contact_mob = set([str(a) for a in eval(x.get('contact_mobile', '[]')) if len(a) > 0 and a.isdigit()])
        for e in contact_mob:
            es[e] = es.get(e, 0) + 1
        if e_type not in x:
            continue
        e = x[e_type]
        es[e] = es.get(e, 0) + 1
    if len(es.keys()) > 0:
        return ";".join(["%s:%s" % (id, e) for e, _ in sorted(es.items(), key=lambda x: x[1], reverse=True)[0:50]])
    return "%s:" % id


def udf_action_key(x):
    return 'pc:%s,et:%s,mob:%s,id:%s,did:%s,type:%s' % (
            x.get('partnercode', '')
           ,str(int(x.get('eventoccurtime', '0'))/86400)
           ,x.get('accountmobile', '')
           ,x.get('idnumber', '')
           ,'' # x.get('deviceid', '')
           ,x.get('eventtype', '')
            )


def udf_distinct_v(v):
    new_v = []
    log_key = {}
    try:
        for x in sorted(v, key=lambda x: x['eventoccurtime'], reverse=False):
            a = x['eventoccurtime'][0:10]
            if len(a) != 10 or a.find('.') >= 0:
                continue
            try:
                b = int(a)
            except:
                continue
            x['eventoccurtime'] = a
            k = udf_action_key(x)
            minitue = int(x['eventoccurtime'][0:10])
            if minitue - log_key.get(k, 0) > 5*60*60:
                new_v.append(x)
            log_key[k] = minitue
    except:
        return []
    return new_v


def udf_entity_hash(id, id_value, apply_dt, dbg=False):
    if id_value is None or id is None: return 'null'
    id_value = udf_distinct_v(id_value)
    if len(apply_dt) == 10: apply_dt = '%s 00:00:00' % apply_dt
    ft_v = []
    for x in id_value:
        event_time = utils.timestamp_datetime(x.get('eventoccurtime'))
        date_diff = utils.get_day(apply_dt, event_time)
        if (event_time < apply_dt) and (date_diff <= 720):
            ft_v.append(x)
    if len(ft_v) == 0: return 'null'
    e_cnts = []
    for e in ['accountmobile', 'idnumber', 'deviceid', 'accountemail', 'qqnumber']:
        cnt = len(set([x[e] for x in ft_v if x.get(e,'') != '' ]))
        e_cnts.append(min(cnt, 9))
    return '%s_%s_%s' % (id, apply_dt, ''.join([str(x) for x in e_cnts]))


def group_hash(args, hc):
    # group id_hash of mobile
    ids_hash_sql = '''
        select
            distinct D.mobile, collect_list(D.id_hash) ids_hash
        from (
                select
                    distinct A.mobile, B.id_hash
                from tmp_id_mob_tb A
                left outer join (
                        select
                            distinct id, id_hash
                        from id_hash_tb
                    ) B on A.id = B.id
                ) D
        group by D.mobile
    '''
    print ids_hash_sql
    hc.sql(ids_hash_sql).registerTempTable('ids_hash')

    # group by mob_hash od id
    mobs_hash_sql = '''
        select
            distinct D.id, collect_list(D.mob_hash) mobs_hash
        from (
                select
                    distinct A.id, B.mob_hash
                from tmp_id_mob_tb A
                left outer join (
                        select
                            distinct mobile, mob_hash
                        from mob_hash_tb
                    )B on A.mobile = B.mobile
                ) D
        group by D.id
    '''
    print mobs_hash_sql
    hc.sql(mobs_hash_sql).registerTempTable('mobs_hash')


def main_calc_id_mob_hash(args, hc):
    # calc id_hash
    calc_id_hash_sql = '''
        select
            A.value id, A.apply_dt, udf_entity_hash(A.value, B.id_value, A.apply_dt) id_hash
        from %s_%s A
        left outer join tmp_id_tb B on A.value = B.id
    ''' % (args.target_tb_prefix, args.source)
    hc.sql(calc_id_hash).registerTempTable('id_hash_tb')

    # calc mob_hash
    calc_mob_hash_sql = '''
        select
            A.mobile, A.apply_dt, udf_entity_hash(A.mobile, B.mobile_value, A.apply_dt) mob_hash
        from %s_%s A
        left outer join tmp_mob_tb B on A.mobile = B.mobile
    ''' % (args.target_tb_prefix, args.source)
    hc.sql(calc_mob_hash_sql).registerTempTable('mob_hash_tb')


def main_offline_extract_data(args, hc, id_event_table, mob_event_table):
    sql = '''
        select
            distinct
            split(x, ':')[0] as id
           ,split(x, ':')[1] as mobile
        from (
                select 
                    explode(split(id_mob, ':')) x
                from (
                    select 
                        udf_entity_extract(B.id, B.id_value, A.apply_dt, 'accountmobile') id_mob
                    from %s_%s A
                    left outer join %s B on A.value = B.id
                    )C
                )D
        union all
            select 
                value id
               ,mobile
            from %s_%s

    ''' % ('fl_target', args.source, id_event_table, 'fl_target', args.source)
    print sql
    hc.sql(sql).registerTempTable('tmp_id_mob_tb')
    print hc.sql('select count(*) from tmp_id_mob_tb').take(1)
    
    sql = '''
        select 
            B.*
        from (
                select
                    distinct id
                from fl_id_mob_%s
                ) A
        left outer join %s B on A.id = B.id
    ''' % (args.source, id_event_table)
    print sql
    hc.sql(sql).registerTempTable('tmp_id_tb')
    print hc.sql('select * from tmp_id_tb limit 10').show(5)

    sql = '''
        select 
            B.*
        from (
                select
                    distinct mobile
                from fl_id_mob_%s
                ) A
        left outer join %s B on A.mobile = B.mobile
    ''' % (args.source, mobile_event_table)
    print sql
    hc.sql(sql).registerTempTable('tmp_mob_tb')
    print hc.sql('select * from tmp_mob_tb limit 10').show(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-task', default='Test_id', help='project name')
    parser.add_argument('-flag_4_test', default=1, help='project name')
    parser.add_argument('-target_tb_prefix', default='fl_traget_test', help='project name')
    parser.add_argument('-test_tb', default='creditmodel.fasttext_score_dzn')
    args = parser.parse_args()
    args.source = 'test' if args.task == 'Test_id' else 'td'
    sql = '''
        select value, mobile, test_date as apply_dt from %s
    ''' % args.test_tb
    hc.sql(sql).registerTempTable('fl_target_test')
    hc.registerFunction('udf_entity_extract', udf_entity_extract)
    id_event_table = 'bigdata.idnumber_aggregation_events'
    mob_event_table = 'bigdata.mobile_aggregation_events'
    if args.flag_4_test == 1:
        main_offline_extract_data(args, hc, id_event_table, mob_event_table)
    else:
        args.source = 'online'
        main_4_online(args, hc)



    
