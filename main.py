import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
import json
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch

engine = create_engine('postgresql://postgres:nrz1371@localhost/samak')
es = Elasticsearch(hosts="http://norozzadeh:Kibana@110$%^@192.168.143.34:9200")

#______Read event  & epg file_______

s_point='2021-08-31T23:29'
d_point='2021-08-31T23:59'
# time.sleep(39600)

for day in range(0,365):
    t1=time.perf_counter()

    start=datetime.strptime(s_point,'%Y-%m-%dT%H:%M')
    start=start+timedelta(days=day)
    end=start+timedelta(minutes=30)
    _start = start - timedelta(days=1)

    day=datetime.strftime(start,'%Y-%m-%dT%H:%M')
    end_day =datetime.strftime(end,'%Y-%m-%dT%H:%M')
    lable=datetime.strftime(start,'%d_%m_%Y')


    # print(read_file)

    try:

        _end = datetime.strftime(end, '%Y-%m-%d')
        _start_ = datetime.strftime(_start, '%Y-%m-%d')

        body = {'query': {'bool': {'must': [{'match_all': {}},
                                            {'range': {'@timestamp': {'gte': '{}T19:31:00.000Z'.format(_start_),
                                                                      'lt': '{}T19:30:00.000Z'.format(_end)}}}]}}}

        def process_hits(hits):
            _df1 = pd.DataFrame()
            for item in hits:
                _res = json.dumps(item, indent=2)
                _df0 = pd.DataFrame(item['_source'], columns=item['_source'].keys(), index=[0])
                # print(_df0)
                _df1 = _df1.append(_df0, ignore_index=True)
            return _df1


        data = es.search(
            index='live-action',
            scroll='1m',
            size=10000,
            body=body
        )

        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        sum = 0
        read_file = pd.DataFrame()
        while scroll_size > 0:

            _a = process_hits(data['hits']['hits'])
            read_file = read_file.append(_a, ignore_index=True)

            data = es.scroll(scroll_id=sid, scroll='2m')


            sid = data['_scroll_id']

            scroll_size = len(data['hits']['hits'])

            read_file = read_file[['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
                       'content_name', 'channel_name', 'content_id', 'content_type_id', 'action_id']]


            read_file = read_file.astype(str)

            sum = scroll_size + sum
            print(sum)
        read_file["time_stamp"] = pd.to_datetime(read_file["time_stamp"])
        # read_file['time_stamp2'] = read_file['time_stamp'] + pd.Timedelta(hours=4, minutes=30)
        read_file['time_stamp'] = read_file['time_stamp'] + pd.Timedelta(hours=4, minutes=30)
        read_file['time_stamp'] = read_file['time_stamp'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
        # read_file['time_stamp2'] = read_file['time_stamp2'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
        read_file.to_csv(r'F:\clean_epg\epg\____epg____{}.csv'.format(lable), index=False)
        # read_file=pd.read_csv(r'F:\clean_epg\epg\____epg____{}.csv', index_col=False)

        exist=1
    except:
        # print('nok1')
        exist=0
    if exist!=0:
        try:

            epg = pd.read_excel(r'F:\clean_epg\epg\epg_{}.xlsx'.format(lable), index_col=False)
            epg['channel'] = epg['channel'].fillna('_بدون عنوان')
            # epg.to_excel(r'F:\clean_epg\epg\eeeeeeeepg_{}.xlsx')
            print('ok2')

            # v_q = datetime.strftime(end, "%Y-%m-%d")
            # my_str = "\'" + v_q + "\'"
            # epg = pd.read_sql_query('SELECT * FROM epg_get where "e_dete" = date {}'.format(my_str),con=engine.connect())
            # print(epg['s_time'])
            # epg["s_time"]=epg["s_time"].astype(str)
            # epg["e_time"] = epg["e_time"].astype(str)

            exist=1
        except:

            exist=0
        if exist!=0:
            # ______Epg Correction_________
            epg['visit'] = 0
            epg['u_visit'] = 0
            epg['dur'] = 0
            epg["s1_time"] = pd.to_datetime(epg["s_time"])
            epg["e1_time"] = pd.to_datetime(epg["e_time"])
            _column = epg["e1_time"] - epg["s1_time"]
            epg['dif'] = _column.dt.total_seconds() / 60

            epg['s_time'] = [x[:5] for x in epg['s_time']]
            epg['s_time'] = epg['s_time'].astype(str) + ':00'
            epg['e_time'] = [x[:5] for x in epg['e_time']]
            epg['e_time'] = epg['e_time'].astype(str) + ':00'



            # ________event file Correction_______

            df = read_file.copy()
            df['action_id'] = df['action_id'].astype(str)

            # ______Create dfn DataFrame______

            dfn = df.copy()
            dfn['tag'] = ''
            d = ''
            dfx = dfn.copy()
            dfn = dfx.copy()
            epgt = epg.copy()
            epg['tag'] = ''

            for i in range(0, len(dfn)):

                print(i)
                a = dfn.loc[i, 'session_id']
                # print(a)

                tag = dfn.loc[i, 'tag']
                chan = dfn.loc[i, 'channel_name']

                if a != '' and tag == '':