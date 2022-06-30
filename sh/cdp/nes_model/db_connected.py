# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 12:07:02 2021

@author: user001
"""

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import numpy as np
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
# %%


def date_transform_to_timestamp(string_date):  # ex "2021-3-1"
    return int(datetime.strptime(string_date, '%Y-%m-%d').timestamp())


def timestamp_transform_to_date(int_timestamp):
    return datetime.fromtimestamp(int_timestamp).date()
import json,requests
def send_message_To_my_telegram(text, chat_id):
    final_text = "You said: " + text
    TELE_TOKEN='5019057258:AAHefWcICzDB8Su6DyxI9BcAY6vPbRVtm_0'
    URL = "https://api.telegram.org/bot{}/".format(TELE_TOKEN)
    url = URL + "sendMessage?text={}&chat_id={}".format(final_text, chat_id)
    jj=requests.post(url)


def connect_to_CDP_mysql_and_return_df(query):
    host = 'product-accucdp-rds-cdp-master-ro-prd.cfilkryz7gav.ap-northeast-1.rds.amazonaws.com'
    user = 'etl-reader'
    passwd = '6EsuJizi5u'

    config = {
        'host': host,
        'user': user,
        'password': passwd,

    }
    try:
        conn = mysql.connector.connect(**config)
#        conn.execute('set max_allowed_packet=67108864')

        print("Connection established")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif err.errno == 1142:
            print("no db")
        else:
            print(err)
    else:
        cursor = conn.cursor()
    try:
        cursor.execute(query)
        query_data = pd.DataFrame(cursor.fetchall(),  columns=[
                                  desc[0] for desc in cursor.description])
    except:
        return pd.DataFrame()
    # Cleanup
    cursor.close()
    conn.close()
    print("Done for connect and close clearly.")
    return query_data
# %%


def upload_data_to_Product(query):

    host = 'accu-data-science-rds-etl-datapool-prd-01.cfilkryz7gav.ap-northeast-1.rds.amazonaws.com'
    user = '3dm_worker'
    passwd = 'BvM4se3BXP'
    config = {
        'host': host,
        'user': user,
        'password': passwd,
    }

    config = {
        'host': host,
        'user': user,
        'password': passwd,
        'database': 'nes_model'
    }

    try:
        conn = mysql.connector.connect(**config)
        #conn.execute('set max_allowed_packet=67108864')
        print("Connection established")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif err.errno == 1142:
            print("no db")
        else:
            print(err)

    # 一個add table
    # 一個upload
    else:
        cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Done for connect and close clearly.")
# %%

def judge_level(rt_ratio) :
    if(rt_ratio<2):
        return "E0"
    elif(rt_ratio>=2 and rt_ratio<2.5):
        return "S1"
    elif(rt_ratio>=2.5 and rt_ratio<3):
        return "S2"
    elif(rt_ratio>=3):
        return "S3"
    else:
        return "例外處理"
#%%
def feature_engerring_for_nes_new(final_new_ssn) :


    final_new_ssn['rt_ratio'] = final_new_ssn['rt_ratio'].fillna(0)
# print(final_new_ssn['rt_ratio'])
# 檢查中位數是否正常
    cycle_median = final_new_ssn['cycle_time'].median()
    print("檢查中位數是否為nan")
    print(final_new_ssn['cycle_time'].median())
    #雖然不太可能發生 但還是防待一下cycle_median==nan的情況

    if(pd.isna(final_new_ssn['cycle_time'].median())):
        cycle_median = 1
    final_new_ssn["Kind_of_person"] = "skr"
    final_new_ssn.loc[final_new_ssn.rt_ratio == 0,'rt_ratio' ] = (final_new_ssn['Recency']/3)
    final_new_ssn['Kind_of_person'] =  final_new_ssn['rt_ratio'].apply(judge_level)
    final_new_ssn.loc[(final_new_ssn.now_create_interval <=7 ,'Kind_of_person')] = 'NEW'

#     # 對新客戶進行處理
#     final_new_ssn["Kind_of_person"] = "skr"
#     for i in range(len(final_new_ssn)):
#         final_new_ssn['Kind_of_person'][i] =  judge_level(final_new_ssn['rt_ratio'][i])
#         if(final_new_ssn['now_create_interval'][i]<=7):
#             final_new_ssn['Kind_of_person'][i] = "NEW"

    table3 = {
     "start_date" : "" ,
     "End_date" : "" ,
     "E0_sum": len(final_new_ssn['Kind_of_person'][final_new_ssn['Kind_of_person']=="E0"]),
     "S1_sum": len(final_new_ssn['Kind_of_person'][final_new_ssn['Kind_of_person']=="S1"]),
     "S2_sum": len(final_new_ssn['Kind_of_person'][final_new_ssn['Kind_of_person']=="S2"]),
     "S3_sum": len(final_new_ssn['Kind_of_person'][final_new_ssn['Kind_of_person']=="S3"]),
     "New_sum" : len(final_new_ssn['Kind_of_person'][final_new_ssn['Kind_of_person']=="NEW"])
    }
    return { "table2" : final_new_ssn ,"table3" : table3}
#%%

#這個func的功能有 1. 回傳select * from 轉換成df 2. 得到table 3. 上傳table
def connect_to_3DM_lot_of_methods_mysql_and_return_df(query , method ,value):

    host = 'accu-data-science-rds-etl-datapool-prd-01.cfilkryz7gav.ap-northeast-1.rds.amazonaws.com'
    user = 'jay'
    passwd = 'O6WNC#pRi$z'
    config = {
      'host':host,
      'user':user,
      'password':passwd,
    }

    try:
        conn = mysql.connector.connect(**config)
#         conn.execute('set max_allowed_packet=67108864')
        print("Connection established")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif err.errno == 1142:
            print("no db")
        else:
            print(err)

    else:
      cursor = conn.cursor()
    try :
        if(method=="table"):
            cursor.execute(query)
            query_data = cursor.fetchall()
            cursor.close()
            conn.close()
            return query_data
    except :
        return pd.DataFrame()
    # Cleanup

    print("Done for connect and close clearly.")
    if(method=="get"):
        cursor.execute(query)
        query_data =  pd.DataFrame(cursor.fetchall() ,  columns = [desc[0] for desc in cursor.description])
        cursor.close()
        conn.close()
        return  query_data

    if(method=="add_a_lot"):
        cursor.executemany(query, value)
        conn.commit()
        cursor.close()
        conn.close()

    if(method=="normal"):
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
#%%
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)
#%%
#這個func的功能有 1. 回傳select * from 轉換成df 2. 得到table 3. 上傳table
def upload_to_3DM_Proudcut(query , method ,value):

    host = 'accu-data-science-rds-main-datapool-prd-01.cfilkryz7gav.ap-northeast-1.rds.amazonaws.com'
    user = '3dm_worker'
    passwd = 'BvM4se3BXP'
    config = {
      'host':host,
      'user':user,
      'password':passwd,
    }

    try:
        conn = mysql.connector.connect(**config)
#         conn.execute('set max_allowed_packet=67108864')
        print("Connection established")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif err.errno == 1142:
            print("no db")
        else:
            print(err)

    else:
      cursor = conn.cursor()
    try :
        if(method=="table"):
            cursor.execute(query)
            query_data = cursor.fetchall()
            cursor.close()
            conn.close()
            return query_data
    except :
        return pd.DataFrame()
    # Cleanup

    print("Done for connect and close clearly.")
    if(method=="get"):
        cursor.execute(query)
        query_data =  pd.DataFrame(cursor.fetchall() ,  columns = [desc[0] for desc in cursor.description])
        cursor.close()
        conn.close()
        return  query_data

    if(method=="add_a_lot"):
        cursor.executemany(query, value)
        conn.commit()
        cursor.close()
        conn.close()

    if(method=="normal"):
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

def tranform_df_to_tuple_for_mysql(df):
    result = [tuple(data) for data in df.values.tolist()]
    return result

def create_3table_in_nes_mondel(org_id):
    upload_to_3DM_Proudcut("create table web.nes_"+str(org_id)+" like web.nes_1","normal","")
    upload_to_3DM_Proudcut("create table web.nes_"+str(org_id)+"_etl like web.nes_1_etl","normal","")
    upload_to_3DM_Proudcut("create table web.nes_"+str(org_id)+"_src like web.nes_1_src","normal","")
