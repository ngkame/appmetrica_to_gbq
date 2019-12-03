import datetime
import os
import time
import xml.etree.ElementTree as ET
import pandas_gbq
import pandas as pd
import requests
import locals as LC
from google.oauth2 import service_account
DLIST=[] # list of date for load
## CREDENTIALS = service_account.Credentials.from_service_account_file(LC.TOKEN_AUTH)
f = lambda x:'secondtime_Mon_depth' if x.strftime("%a") == 'Mon' else 'secondtime_depth'

def to_bigq(r, ds, tbl):
    global CREDENTIALS
    pandas_gbq.to_gbq(
        r, ds + '.' + tbl, if_exists='replace', credentials=CREDENTIALS
    )
    print("Job finished.")

    return

def load_from_appm(xtable, xfields, xdate_since, xdate_until, xname):
    PARAMS = {'application_id': LC.APPLICATION_ID ,
            'date_since': xdate_since ,
            'date_until': xdate_until ,
            'date_dimension': 'default',
            'use_utf8_bom': 'true',
            'fields': xfields}


    URL = 'https://api.appmetrica.yandex.ru/logs/v1/export/' + xtable + '.json?'
    # Authorization: OAuth
    headers = {"Authorization": ' OAuth '+LC.APPMETRICA_YAPASPORT_KEY}
    print("GET requests from appmetrica table: ", xtable,xdate_since,xdate_until)
    r = requests.get(URL, params=PARAMS, headers=headers)
    k = 0
    if r.status_code != 200:
        while r.status_code != 200:
            if r.status_code == 400 or r.status_code == 500:
                print('Bad Code=', r.status_code, ' text=', r.text, 'at=', datetime.datetime.now())
            time.sleep(10)
            k += 10

            r = requests.get(URL, params=PARAMS, headers=headers)

    print('request waiting for seconds=', k)

    df = pd.read_json(bytes(r.text, 'utf-8'), orient='split')  # ['data']
    if not df.empty:
        print('data loaded from appm')
        to_bigq(df, 'appmetrica', xname)
        # print('quiting') #comment this for normal
        # quit() #comment this for normal
        return True
    else:
        print('no data for loading to Bigquery')
        return False


def table_lister(date_list):
    # здесь создание массива дат

    fold = os.path.dirname(__file__)
    tree = ET.parse(fold + '/fields_of_table.xml')
    for table in tree.findall('./table'):
        print("1", table.get('name'))
        xtra = ''
        xfields = ''
        xtable = table.get('name')
        for ch_child in list(table):
            xfields = xfields + xtra + ch_child.attrib['name']
            xtra = ','
        for x in date_list:
            print("loading table...", xtable + x["name"])
            load_from_appm(xtable, xfields, x["start"], x["finish"], xtable + x["name"])  # загрузка таблиц


def date_coll(first_time=True):
    NOW_DATE = datetime.datetime.today()
    if first_time:
        CURSOR_DATE=datetime.datetime.today()-datetime.timedelta(days=LC.DATES['firsttime_depth'])
        DLIST.append( [{'start': LC.DATES['first_date']+ ' 00:00:00', 'finish': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 23:59:59', 'name': '_old'}])
    else:
        CURSOR_DATE = NOW_DATE - datetime.timedelta(days=LC.DATES[f(NOW_DATE)])


        DLIST.append(
            [{'start': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 00:00:00', 'finish': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 23:59:59', 'name': '_' + CURSOR_DATE.strftime("%Y%m%d")}])
    while CURSOR_DATE < NOW_DATE:
        CURSOR_DATE += datetime.timedelta(days=1)
        fn = CURSOR_DATE.strftime("%Y%m%d")
        DLIST.append({'start': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 00:00:00', 'finish': CURSOR_DATE.strftime("%Y-%m-%d")+ ' 23:59:59',
                          'name': '_' + fn})

    return


if __name__ == "__main__":
    print('started at', datetime.datetime.now())

    date_coll(True)
    print(DLIST)
    #table_lister(date_coll(first_time=True))
    #to_bigq(etl_sessions(), 'appmetrica', 'sessions_last_etl')
    print('finished at', datetime.datetime.now())

