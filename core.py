import datetime
import os
import time
import xml.etree.ElementTree as ET
import pandas_gbq
import pandas as pd
import requests
import locals as LC
from google.oauth2 import service_account

## CREDENTIALS = service_account.Credentials.from_service_account_file(LC.TOKEN_AUTH)

def to_bigq(r, ds, tbl):
    global CREDENTIALS
    pandas_gbq.to_gbq(
        r, ds + '.' + tbl, if_exists='replace', credentials=CREDENTIALS
    )
    print("Job finished.")

    return

def load_from_appm(xtable, xfields, xdate_since, xdate_until, xname):
    parA = {'application_id': LC.APPLICATION_ID ,
            'date_since': xdate_since + ' 00:00:00',
            'date_until': xdate_until + ' 23:59:59',
            'date_dimension': 'default'}
    parB = {'use_utf8_bom': 'true',
            'fields': xfields}
    PARAMS = dict(parA)
    PARAMS.update(parB)
    key = "OAuth AgAEA7qi2BGOAAW45YmXjzra-k8LgcDHAG5CSrE"
    URL = 'https://api.appmetrica.yandex.ru/logs/v1/export/' + xtable + '.json?'
    # Authorization: OAuth
    headers = {"Authorization": key}
    print("GET requests from appmetrica table: ", xtable,xdate_since,xdate_until)  # ," fields-",xfields)
    r = requests.get(URL, params=PARAMS, headers=headers)
    k = 0
    if r.status_code != 200:
        while r.status_code != 200:
            if r.status_code == 400 or r.status_code == 500:
                print('Bad Code=', r.status_code, ' text=', r.text, 'at=', datetime.datetime.now())
                # quit()
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
    if first_time:
        FIRST_DATE = '2018-05-01'
        nd = datetime.datetime.now()
        NOW_DATE = datetime.date(nd.year, nd.month, nd.day)
        print(NOW_DATE)
        LAST_DATE = NOW_DATE
        LAST_DATE -= datetime.timedelta(days=150)
        print(LAST_DATE)
        # LAST_DATE=datetime.datetime.now()-datetime.timedelta(month=1)

        dlist = [{'start': FIRST_DATE, 'finish': LAST_DATE.strftime("%Y-%m-%d"), 'name': '_old'}]
        CURSOR_DATE = LAST_DATE
        while CURSOR_DATE < NOW_DATE:
            CURSOR_DATE += datetime.timedelta(days=1)
            fn = CURSOR_DATE.strftime("%Y%m%d")
            # print(fn)
            dlist.append({'start': CURSOR_DATE.strftime("%Y-%m-%d"), 'finish': CURSOR_DATE.strftime("%Y-%m-%d"),
                          'name': '_' + fn})

            # print(CURSOR_DATE)
    else:
        nd = datetime.datetime.now()
        NOW_DATE = datetime.date(nd.year, nd.month, nd.day)
        if NOW_DATE.strftime("%a") == 'Mon':
            cnt = 30
        else:
            cnt = 7
        CURSOR_DATE = NOW_DATE - datetime.timedelta(days=cnt)
        fn = CURSOR_DATE.strftime("%Y%m%d")
        dlist = (
            [{'start': CURSOR_DATE.strftime("%Y-%m-%d"), 'finish': CURSOR_DATE.strftime("%Y-%m-%d"), 'name': '_' + fn}])
        while CURSOR_DATE < NOW_DATE:
            CURSOR_DATE += datetime.timedelta(days=1)
            fn = CURSOR_DATE.strftime("%Y%m%d")
            dlist.append({'start': CURSOR_DATE.strftime("%Y-%m-%d"), 'finish': CURSOR_DATE.strftime("%Y-%m-%d"),
                          'name': '_' + fn})

    return dlist


if __name__ == "__main__":
    print('started at', datetime.datetime.now())
    #table_lister(date_coll(first_time=True))
    to_bigq(etl_sessions(), 'appmetrica', 'sessions_last_etl')
    print('finished at', datetime.datetime.now())
import datetime
import os
import time
import xml.etree.ElementTree as ET
import pandas_gbq
import pandas as pd
import requests
from google.oauth2 import service_account

DIR = os.path.dirname(os.path.realpath(__file__))
TOKEN_AUTH =DIR+ '/token_gbq.json'
CREDENTIALS = service_account.Credentials.from_service_account_file(TOKEN_AUTH)
# import timedelta
def to_bigq(r, ds, tbl):
    global CREDENTIALS
    pandas_gbq.to_gbq(
        r, ds + '.' + tbl, if_exists='replace', credentials=CREDENTIALS
    )
    print("Job finished.")

    return


def load_from_appm(xtable, xfields, xdate_since, xdate_until, xname):
    parA = {'application_id': 1750004,
            'date_since': xdate_since + ' 00:00:00',
            'date_until': xdate_until + ' 23:59:59',
            'date_dimension': 'default'}
    parB = {'use_utf8_bom': 'true',
            'fields': xfields}
    PARAMS = dict(parA)
    PARAMS.update(parB)
    key = "OAuth AgAEA7qi2BGOAAW45YmXjzra-k8LgcDHAG5CSrE"
    URL = 'https://api.appmetrica.yandex.ru/logs/v1/export/' + xtable + '.json?'
    # Authorization: OAuth
    headers = {"Authorization": key}
    print("GET requests from appmetrica table: ", xtable,xdate_since,xdate_until)  # ," fields-",xfields)
    r = requests.get(URL, params=PARAMS, headers=headers)
    k = 0
    if r.status_code != 200:
        while r.status_code != 200:
            if r.status_code == 400 or r.status_code == 500:
                print('Bad Code=', r.status_code, ' text=', r.text, 'at=', datetime.datetime.now())
                # quit()
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
    if first_time:
        FIRST_DATE = '2018-05-01'
        nd = datetime.datetime.now()
        NOW_DATE = datetime.date(nd.year, nd.month, nd.day)
        print(NOW_DATE)
        LAST_DATE = NOW_DATE
        LAST_DATE -= datetime.timedelta(days=150)
        print(LAST_DATE)
        # LAST_DATE=datetime.datetime.now()-datetime.timedelta(month=1)

        dlist = [{'start': FIRST_DATE, 'finish': LAST_DATE.strftime("%Y-%m-%d"), 'name': '_old'}]
        CURSOR_DATE = LAST_DATE
        while CURSOR_DATE < NOW_DATE:
            CURSOR_DATE += datetime.timedelta(days=1)
            fn = CURSOR_DATE.strftime("%Y%m%d")
            # print(fn)
            dlist.append({'start': CURSOR_DATE.strftime("%Y-%m-%d"), 'finish': CURSOR_DATE.strftime("%Y-%m-%d"),
                          'name': '_' + fn})

            # print(CURSOR_DATE)
    else:
        nd = datetime.datetime.now()
        NOW_DATE = datetime.date(nd.year, nd.month, nd.day)
        if NOW_DATE.strftime("%a") == 'Mon':
            cnt = LC.DATES['secondtime_mon_depth']
        else:
            cnt = LC.DATES['secondtime_depth']
        CURSOR_DATE = NOW_DATE - datetime.timedelta(days=cnt)
        fn = CURSOR_DATE.strftime("%Y%m%d")
        dlist = (
            [{'start': CURSOR_DATE.strftime("%Y-%m-%d"), 'finish': CURSOR_DATE.strftime("%Y-%m-%d"), 'name': '_' + fn}])
        while CURSOR_DATE < NOW_DATE:
            CURSOR_DATE += datetime.timedelta(days=1)
            fn = CURSOR_DATE.strftime("%Y%m%d")
            dlist.append({'start': CURSOR_DATE.strftime("%Y-%m-%d"), 'finish': CURSOR_DATE.strftime("%Y-%m-%d"),
                          'name': '_' + fn})

    return dlist


if __name__ == "__main__":
    print('started at', datetime.datetime.now())
    print(LC.DATES['secondtime_mon_depth'])
    #table_lister(date_coll(first_time=LC.FIRST_TIME_LOAD))
    #to_bigq(etl_sessions(), 'appmetrica', 'sessions_last_etl')
    print('finished at', datetime.datetime.now())
