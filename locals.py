import os
from google.oauth2 import service_account

DIR = os.path.dirname(os.path.realpath(__file__))
TOKEN_AUTH =DIR+ '\token.json' #Google Big Query Token file. https://cloud.google.com/bigquery/docs/authentication/service-account-file
CREDENTIALS = service_account.Credentials.from_service_account_file(TOKEN_AUTH)
GBQ_DATASET_NAME='' # create dataset before first time loading
FIRST_TIME_LOAD=True # change on false after firtst time loading

DATES= {'first_date':'2018-05-01',
        'firsttime_depth':150,
        'secondtime_depth':7,
        'secondtime_Mon_depth':30}
APPMETRICA_YAPASPORT_KEY='' # get key here https://passport.yandex.ru/registration?mode=register

APPLICATION_ID='' # appmetrica application id like #####

#add or delete tables/fields for export
APPMETRICA_FIELDS=[{'table':'clicks',
                    'fields':['application_id','click_id','click_ipv6','click_datetime','click_timestamp','click_url_parameters','click_user_agent','publisher_id',
                              'publisher_name','tracker_name','tracking_id','city','country_iso_code','device_type','device_model','device_manufacturer',
                              'os_version','os_name','windows_aid','google_aid','ios_ifv','ios_ifa']},
                   {'table':'postbacks',
                    'fields':['application_id','click_datetime','click_id','click_ipv6','click_timestamp','click_url_parameters','click_user_agent','publisher_id',
                              'publisher_name','tracker_name','tracking_id','install_datetime','install_ipv6','install_timestamp','match_type','appmetrica_device_id',
                              'device_locale','device_manufacturer','device_model','device_type','google_aid','ios_ifa','ios_ifv','os_name','os_version','windows_aid'
                              'app_package_name','app_version_name','conversion_datetime','conversion_timestamp','event_name','attempt_datetime','attempt_timestamp',
                              'cost_model','notifying_status','postback_url','postback_url_parameters','response_body','response_code']},
                   {'table':'installations',
                    'fields':['application_id','click_datetime','click_id','click_ipv6','click_timestamp','click_url_parameters','click_user_agent','profile_id',
                              'publisher_id','publisher_name','tracker_name','tracking_id','install_datetime','install_ipv6','install_receive_datetime',
                              'install_receive_timestamp','install_timestamp','is_reattribution','is_reinstallation','match_type','appmetrica_device_id','city',
                              'connection_type','country_iso_code','device_locale','device_manufacturer','device_model','device_type','google_aid','ios_ifa',
                              'ios_ifv','mcc','mnc','operator_name','os_name','os_version','windows_aid','app_package_name','app_version_name']},
                   {'table': 'events',
                    'fields': ['event_datetime','event_json','event_name','event_receive_datetime','event_receive_timestamp','event_timestamp','session_id',
                               'installation_id','appmetrica_device_id','city','connection_type','country_iso_code','device_ipv6','device_locale','device_manufacturer',
                               'device_modeldevice_model','device_type','google_aid','ios_ifa','ios_ifv','mcc','mnc','operator_name','original_device_model','os_name',
                               'os_version','profile_id','windows_aid','app_build_number','app_package_name','app_version_name','application_id']},
                   {'table':'profiles',
                    'fields':['profile_id','appmetrica_gender','appmetrica_birth_date','appmetrica_notifications_enabled','appmetrica_name','appmetrica_crashes',
                              'appmetrica_errors','appmetrica_first_session_date','appmetrica_last_start_date','appmetrica_push_opens','appmetrica_push_send_count',
                              'appmetrica_sdk_version','appmetrica_sessions','android_id','appmetrica_device_id','city','connection_type','country_iso_code',
                              'device_manufacturer','device_model','device_type','google_aid','ios_ifa','ios_ifv','mcc','mnc','operator_name','os_name','os_version',
                              'windows_aid','app_build_number','app_framework','app_package_name','app_version_name']},
                   {'table':'deeplinks',
                    'fields':['deeplink_url_parameters','deeplink_url_path','deeplink_url_scheme','event_datetime','event_receive_datetime','event_receive_timestamp',
                              'event_timestamp','is_reengagement','profile_id','publisher_id','publisher_name','session_id','tracker_name','tracking_id','android_id',
                              'appmetrica_device_id','appmetrica_sdk_version','city','connection_type','country_iso_code','device_ipv6','device_locale','device_manufacturer',
                              'device_model','device_type','google_aid','ios_ifa','ios_ifv','mcc','mnc','original_device_model','os_version','windows_aid','app_build_number',
                              'app_package_name','app_version_name']},
                    {'table':'crashes',
                    'fields':['crash','crash_datetime','crash_group_id','crash_id','crash_name','crash_receive_datetime','crash_receive_timestamp','crash_timestamp',
                              'appmetrica_device_id','city','connection_type','country_iso_code','device_ipv6','device_locale','device_manufacturer','device_model',
                              'device_type','google_aid','ios_ifa','ios_ifv','mcc','mnc','operator_name','os_name','os_version','profile_id','windows_aid','app_package_name',
                              'app_version_name','application_id']},
                    {'table':'errors',
                    'fields':['error','error_datetime','error_id','error_name','error_receive_datetime','error_receive_timestamp','error_timestamp','appmetrica_device_id',
                              'city','connection_type','country_iso_code','device_ipv6','device_locale','device_manufacturer','device_model','device_type','google_aid','ios_ifa',
                              'ios_ifv','mcc','mnc','operator_name','os_name','os_version','profile_id','windows_aid','app_package_name','app_version_name','application_id']},
                    {'table':'push_tokens',
                    'fields':['token','token_datetime','token_receive_datetime','token_receive_timestamp','token_timestamp','appmetrica_device_id','city','connection_type',
                              'country_iso_code','device_ipv6','device_locale','device_manufacturer','device_model','device_type','google_aid','ios_ifa','ios_ifv','mcc','mnc',
                              'operator_name','os_name','os_version','profile_id','windows_aid','app_package_name','app_version_name','application_id']},
                   {'table':'sessions_starts',
                    'fields':['session_id','session_start_datetime','session_start_receive_datetime','session_start_receive_timestamp','session_start_timestamp','appmetrica_device_id',
                              'city','connection_type','country_iso_code','device_ipv6','device_locale','device_manufacturer','device_model','device_type','google_aid','ios_ifa',
                              'ios_ifv','mcc','mnc','operator_name','original_device_model','os_name','os_version','profile_id','windows_aid','app_build_number','app_package_name',
                              'app_version_name','application_id']}
                    ]

