# Appmetrica_to_gbq loader
Load data from yandex appmetrica Logs API to Google big query
Get api-key for yandex appmetrica here https://passport.yandex.ru/registration?mode=register

Create Google Big Query dataset.
Get json service account for Google big query https://cloud.google.com/bigquery/docs/authentication/service-account-file and
replace token.json in app folder.

Update locals. For first run edit locals.py

#Locals updates

<i>GBQ_DATASET_NAME </i>= name of Google Big Query dataset which you create before <br>
<i>FIRST_TIME_LOAD </i>=if you load dataset first time set 'True'. If you dataset allready loaded set 'False' <br/>

<i>DATES</i><li>if FIRST_TIME_LOAD=True <br>
'first_date': 'if   then date since you load data from appmetrica in first date load' <br>
'firsttime_depth': 'days depth from today backwards in day count. today- firsttime_depth must be greater then first_date' <br>
<li>if FIRST_TIME_LOAD=False <br>
'secondtime_depth': 'days depth from today backwards in day count run at everyday' <br>
'secondtime_Mon_depth': 'days depth from today backwards in day count run at Monday' 

# run program
```bash
python3 appmetica_to_gbq/
```