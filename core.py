import datetime
import time
import pandas_gbq
import pandas as pd
import requests
import locals as LC

DLIST=[] # list of date for load
f = lambda x:'secondtime_Mon_depth' if x.strftime("%a") == 'Mon' else 'secondtime_depth'

def load_from_appm(xtable, xfields, xdate_since, xdate_until, xname):
    PARAMS = {'application_id': LC.APPLICATION_ID ,
            'date_since': xdate_since ,
            'date_until': xdate_until ,
            'date_dimension': 'default',
            'use_utf8_bom': 'true',
            'fields': xfields}
    headers = {"Authorization": "OAuth "+LC.APPMETRICA_YAPASPORT_KEY}
    print("GET requests from appmetrica table: ", xtable,xdate_since,xdate_until)
    URL = 'https://api.appmetrica.yandex.ru/logs/v1/export/' + xtable + '.json?'
    r = requests.get(URL, params=PARAMS, headers=headers)
    timer=0
    print(PARAMS)
    if r.status_code != 200:
        while r.status_code != 200:
            if r.status_code in [400,500,403]:
                print('Bad Code=', r.status_code, ' response text=', r.text, 'at=', datetime.datetime.now())
                quit()
            time.sleep(10) #waiting for server response
            timer+=10
            print('awaiting ',timer,' seconds, resp.status_code', r.status_code)
            r = requests.get(URL, params=PARAMS, headers=headers)
    df = pd.read_json(bytes(r.text, 'utf-8'), orient='split')  # ['data']
    del r
    if not df.empty:
        print('data loaded from appmetrica')
        pandas_gbq.to_gbq( df, LC.GBQ_DATASET_NAME + '.' + xname, if_exists='replace', credentials=LC.CREDENTIALS )
        del df
        return True
    else:
        print('no data for loading to Bigquery')
        return False


def table_lister():
    # здесь создание массива дат
    for table in LC.APPMETRICA_FIELDS:
        print("1",table['table'])
        for x in DLIST:
           load_from_appm(table['table'],','.join(table['fields']), x["start"], x["finish"], table['table'] + x["nm"])  # загрузка таблиц

def date_coll(first_time=True):
    NOW_DATE = datetime.datetime.today()
    if first_time:
        CURSOR_DATE=datetime.datetime.today()-datetime.timedelta(days=LC.DATES['firsttime_depth'])
        DLIST.append( {'start': LC.DATES['first_date']+ ' 00:00:00', 'finish': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 23:59:59', 'nm': '_old'})
    else:
        CURSOR_DATE = NOW_DATE - datetime.timedelta(days=LC.DATES[f(NOW_DATE)])
        DLIST.append( {'start': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 00:00:00', 'finish': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 23:59:59', 'nm': '_' + CURSOR_DATE.strftime("%Y%m%d")})

    while CURSOR_DATE < NOW_DATE:
        CURSOR_DATE += datetime.timedelta(days=1)
        fn = CURSOR_DATE.strftime("%Y%m%d")
        DLIST.append({'start': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 00:00:00', 'finish': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 23:59:59',
                          'nm': '_' + fn})

    return

if __name__ == "__main__":
    print('started at', datetime.datetime.now())
    date_coll(LC.FIRST_TIME_LOAD)
    table_lister()
    print('finished at', datetime.datetime.now())

