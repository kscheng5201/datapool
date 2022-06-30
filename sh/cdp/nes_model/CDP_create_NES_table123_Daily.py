# -*- coding: utf-8 -*-
"""
Created on Tue Jan 25 09:00:07 2022
@author: user001
"""
# -*- coding: utf-8 -*-
'''
daily 和 first time最大的差別就只是datestart的開始不一樣 就這樣
'''
from db_connected import *
#from jandi_sent import *
start = time.time()
tagDate = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')  #想要觀察的日期 可抓系統時間 美國跟我們差8小時 所以要+8
now = (datetime.now()+timedelta(hours=8)) +timedelta(days=-1)    #請調整這個當排成失敗
dateStart = int((dt.datetime.strptime(tagDate, '%Y-%m-%d')-relativedelta(days=1)).timestamp()) #新增過去到今天的資料
deldate = datetime.strptime(tagDate, '%Y-%m-%d')-relativedelta(days=180) #刪除半年以前的資料
dateEnd = int((dt.datetime.strptime(tagDate, '%Y-%m-%d')).timestamp()) #觀察日期轉換成秒數
print(timestamp_transform_to_date(dateStart),tagDate,now)

#%%
def get_data_from_cdp_for_NES(company_Number) :
    Data=pd.DataFrame()
    print("SQL語法撈取囉"+str(company_Number))
    for i in company_Number:
# Step1 : Extract Data
#這邊要取得indentity以及使用者的加入日期
        query1 = '''
        select fpc , identity ,first_at from(
                SELECT fpc_unique_id as fpc_id, identity, fpc_unique_created_at as first_at FROM cdp_web_'''+str(i)+'''.fpc_unique_data
        )as aa
        left join(
                SELECT fpc, id as fpc_id FROM  cdp_web_'''+str(i)+'''.fpc_unique
        )as bb
        on aa.fpc_id=bb.fpc_id
        '''
    #這邊要取得的是用戶所有的點擊時間

        query2='''
        SELECT fpc, created_at as timestamp
        FROM cdp_web_'''+str(i)+'''.fpc_raw_data
        WHERE created_at  between unix_timestamp(date(now() -interval 1 day)) and unix_timestamp(date(now()))
        '''

        data1 = connect_to_CDP_mysql_and_return_df(query1)
        print(len(data1))
        print(len(data1))
        data2 = connect_to_CDP_mysql_and_return_df(query2)
        print(query2)
        if(len(data1.columns)!=2 and len(data2.columns)!=2):
            return "無此db"
        elif(data1.empty or data2.empty):
            return "無資料"


        print(len(data1.merge(data2, on='fpc', how='inner')))
        Data=Data.append(data1.merge(data2, on='fpc', how='inner'))

    # Cleanup
        print("Done for connect and close clearly.")
        print("Extract 完成嚕 開始 Transfrom")
        #Get UUID
    #這裡的目的是為了產生uuid ，uuid是跨渠道的primary key 這樣才有辦法去groupby 時間戳記
        ssn = Data.copy()
        ssn['identity'] = ssn['identity'].fillna('0') #先補0
        ssn['identity'] = ssn['identity'].astype(str) #其實下面改成%10應行阿
        ssn['linkage'] = ssn['identity'].apply(lambda x: int(x[-1])) #是否有被歸戶 判斷最後一個字是否為1 為1代表有被歸戶
        ssn['uuid'] = ssn.apply(lambda x: x['fpc'] if x['linkage']==0 else x['identity'], axis=1) #primay key
        ssn['timestamp'] = ssn['timestamp'] # 變成台灣時間
        ssn['first_at'] = ssn['first_at']   # 變成台灣時間
        ssn['timestamp']  = ssn.timestamp.apply(lambda x: datetime.fromtimestamp(x).date())
        ssn['first_at']  = ssn.first_at.apply(lambda x: datetime.fromtimestamp(x).date())
        # print(ssn)

    # 接著因為ssn就代表有很多重複的東西 不同的客戶在不同渠道但在同一天的行為我們只算一次 所以這邊drop_duplicate
        new_ssn = ssn[['uuid','first_at','timestamp']].drop_duplicates().reset_index(drop=True)
        return new_ssn

#%%
'''
一次撈玩CDP所有的資料
'''
company_query ='''
select nickname,db_id,org_id
from cdp_organization.organization_domain
where domain_type = "web"
order by org_id
'''
#用number一次爬完所有cdp的資料
company_org_id_data = connect_to_CDP_mysql_and_return_df(company_query)
data = pd.DataFrame([{ "org_id": id, "company_name" : company_org_id_data[company_org_id_data.org_id==id].nickname.drop_duplicates().values.tolist()  , "db_id" : company_org_id_data[company_org_id_data.org_id==id].db_id.values.tolist() } for id in company_org_id_data.org_id.unique().tolist()])
final_data = [{ "org_id": i[0], "company_name" : i[1]  , "db_id" : i[2] ,'data' : get_data_from_cdp_for_NES(i[2])} for i in data.values]


#%%
#upload to all_company_data_to_table1
table = upload_to_3DM_Proudcut('''show tables  from web where Tables_in_web like "%src%" and Tables_in_web like "%nes%"''',"get","")
the_table_num_you_should_create =  [i[0].replace("nes_","").replace("_src","") for i in table.values]

'''
怕被找麻煩 我先寫好 2021/10/8
若未來某年某月某一天這邊有資料了 再回來改ㄅ

#CREATE TABLE new_tbl LIKE orig_tbl;
for i in range(len(final_data)):
    if(type(final_data[i]['data']) != str ):
        if(str(final_data[i]['org_id']) in the_table_num_you_should_create):
            print("讚喔 已create" ,final_data[i]['org_id'] )
        else:
            print("建置db",final_data[i]['org_id'])
            create_3table_in_nes_mondel(org_id)
'''

'''
檢查DB用 2022/2/23
已排查原因，原因如下

過去邏輯 : 必須要有事件或pv才創建
現有邏輯 : 只要出現在org_domain我才建

此邏輯將於2022/2/23開始運行
'''
for i in range(len(final_data)): #1~14間公司 0->1 1->2 依此類推
    org_id = final_data[i]['org_id']
    print(org_id)

    if(str(final_data[i]['org_id']) in the_table_num_you_should_create):
        print("讚喔 已create" ,final_data[i]['org_id'] )
    else:
        print("建置db",final_data[i]['org_id'])
        send_message_To_my_telegram("CDP NES Product"+"建置db"+str(final_data[i]['org_id']),"1888352017")
        create_3table_in_nes_mondel(org_id)

for i in range(len(final_data)): #1~14間公司 0->1 1->2 依此類推
    space=[]
    if(type(final_data[i]['data'])==str):
        print("跳過公司"+str(final_data[i]['company_name']))
    else :
        org_id = final_data[i]['org_id']

        if(type(final_data[i]['data']) != str ):
            if(str(final_data[i]['org_id']) in the_table_num_you_should_create):
                print("讚喔 已create" ,final_data[i]['org_id'] )
            else:
                print("建置db",final_data[i]['org_id'])
                send_message_To_my_telegram("CDP NES Product"+"建置db"+str(final_data[i]['org_id']),"1888352017")

                create_3table_in_nes_mondel(org_id)
        print(org_id)

        for j in range(len(final_data[i]['data'])):
            data = final_data[i]['data'].loc[j]

            add_values = "('"+str(data[0])+"', '"+str(data[1])+"','"+str(data[2])+"')"
            # 判斷是否為最後一個

            if( j != (len(final_data[i]['data'])-1) ):
                add_values = add_values + ","
            else :
                add_values = add_values
            space.append(add_values)

        print(len(final_data[i]['data']))
        print(len(space))
        all_value  = "".join(space)
        '''
        若未來某年某月某一天這邊有資料了 再回來改ㄅ
        '''
        add_dquery = "INSERT INTO web.nes_"+str(org_id)+"_src(uuid,first_at,timestamp) VALUES" +all_value
        del_query = "DELETE FROM web.nes_"+str(org_id)+"_src where timestamp <='"+str(deldate.date())+"';"

        print(del_query)
        upload_to_3DM_Proudcut(add_dquery,"normal","")
        upload_to_3DM_Proudcut(del_query,"normal","")
#%% 直接接table23 牛步牛
# =============================================================================
# =============================================================================
# # 來嚕
# =============================================================================
# =============================================================================

'''
如果未來某年某一天這個壞掉惹
就調整一下就好了唷
'''

from datetime import date
table = upload_to_3DM_Proudcut('''show tables  from web where Tables_in_web like "%nes%" and Tables_in_web like "%src%" ''',"get","")


from datetime import datetime, timedelta,timezone
tz = timezone(timedelta(hours=+8))

for num in table['Tables_in_web']:
    print(num)
    try :
        print(num.replace("nes_","").replace("_src",""))
        num = num.replace("nes_","").replace("_src","")
    
        etl = '''web.nes_'''+str(num)+'''_etl'''
        history = '''web.nes_'''+str(num)+''' '''
    
        today = (now+timedelta(hours=8)).date() #台灣時間
        start = now.date()+timedelta(days=-0)
        end = now.date()+timedelta(days=-91)
        print(num ,start , end)
# =============================================================================
#         原本的QUERY 是全部寫在一起的 但S3M
#         query = '''
#         SELECT  uuid,first_at,count(timestamp) as sum ,min(timestamp) as 90_days_Old_Date ,
#                 max(timestamp) as 90_days_new_Date 
#         from (
#             SELECT distinct uuid,first_at,timestamp from web.nes_'''+str(num)+'''_src
#             where timestamp between (date(now()) - interval 91 day ) and (date(now()) - interval 1 day)
#         )as a
#         group by uuid,first_at
#         '''
#         
# =============================================================================
        #得到所有人的query
        query='''
           SELECT distinct uuid,first_at,timestamp from web.nes_'''+str(num)+'''_src
            where timestamp between (date(now()) - interval 91 day ) and (date(now()) - interval 1 day)
        '''
        ori_data =upload_to_3DM_Proudcut(query,"get","") 
        ori_data = ori_data[['uuid','first_at','timestamp']].groupby(['uuid','first_at']).count().reset_index().rename(columns={"timestamp": "sum"})
    
        # max min 相關資訊
        query2='''
        SELECT  uuid,min(timestamp) as 90_days_Old_Date, max(timestamp) as 90_days_new_Date
        from web.nes_'''+str(num)+'''_src
        where timestamp between (date(now()) - interval 91 day ) and (date(now()) - interval 1 day)
        group by uuid,first_at
        '''
        max_min_info = upload_to_3DM_Proudcut(query2,"get","")
        ori_data['uuid'] = ori_data['uuid'].astype('str')
        max_min_info['uuid'] = max_min_info['uuid'].astype('str')
        data = ori_data.merge(max_min_info, on='uuid',how='left')

        

        if(data.empty):
            data = pd.DataFrame(columns=[ 'uuid', 'first_at', 'sum_count', 'observation_olddate',
           'observation_newdate', 'observation_interval', 'now_create_interval',
           'cycle_time', 'Recency', 'rt_ratio'])
            
        else:   
            data['now_create_interval']  = data.apply(lambda x : (datetime.now(tz).date() -x['first_at']).days ,axis=1)
            data['cycle_time']=data.apply(lambda x : (x['90_days_new_Date'] -x['90_days_Old_Date']).days / (x['sum']-1) if(x['sum']!=1) else np.nan ,axis=1)
            data['Recency']  = data.apply(lambda x : (datetime.now(tz).date() -x['90_days_new_Date']).days ,axis=1)
            data['rt_ratio'] = data['Recency'] /data['cycle_time']
    
        pre_data = feature_engerring_for_nes_new(data)
        
        print(len(pre_data['table2']))
        print(len(pre_data['table3']))
    
       #for_table2.append(pre_data['table2'])
       #for_table3.append(pre_data['table3'])
        pre_data['table3']['start_date'] = str(end)
        pre_data['table3']['End_date'] = str(start)
    
    # #那就上傳吧
    #先上傳到table2
        # upload_to_3DM_Proudcut("truncate table "+etl ,"normal" ,  "" )
        try :
            table2_query = "INSERT INTO "+etl+"(uuid,first_at,sum_count,observation_olddate,observation_newdate,observation_interval,create_interval,cycle_time,Recency,rt_ratio,kind_of_person) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            table2_data = [tuple(data) for data in pre_data['table2'].values.tolist()]
            print(table2_query)
            # upload_to_3DM_Proudcut(table2_query ,"add_a_lot" ,  table2_data )
        # =============================================================================
        #
        # #接著上傳到table3
            table3_query = "INSERT INTO "+history+"(start_date ,End_date ,E0_sum,S1_sum,S2_sum,S3_sum, N_sum) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            table3_data = [tuple(data for data in list(pre_data['table3'].values()))]
            send_message_To_my_telegram("CDP NES Product"+str(datetime.now().date())+history+"上傳成功","1888352017")
            
        # =============================================================================
            upload_to_3DM_Proudcut(table3_query ,"add_a_lot" , table3_data)
            updateprop_sql = "update "+history+"\
            SET E0_prop = (E0_sum /(E0_sum + S1_sum + S2_sum + S3_sum + N_sum))*100,\
                S1_prop = (S1_sum /(E0_sum + S1_sum + S2_sum + S3_sum + N_sum))*100,\
                S2_prop = (S2_sum /(E0_sum + S1_sum + S2_sum + S3_sum + N_sum))*100,\
                S3_prop = (S3_sum /(E0_sum + S1_sum + S2_sum + S3_sum + N_sum))*100,\
                N_prop = (N_sum /(E0_sum + S1_sum + S2_sum + S3_sum + N_sum))*100 \
            WHERE End_date >=date(now() - interval 3 day)\
            "
            upload_to_3DM_Proudcut(updateprop_sql ,"normal", "")
    
        except Exception as e :
            from jandi_sent import *
            Repo = LogRepo()
            Repo.Jandi_send(message_type='error'
                            ,brief="執行錯誤-- CDP/互動行為分析/CDP NES"
                            ,detail=str(e)
                            ,jandi_url="https://wh.jandi.com/connect-api/webhook/24388692/37194735b24a02d9c23c33342fc78a55")
            
            # print(e, file=f)
            # os.system("/opt/conda/bin/python /home/jovyan/IG_Token_Product/send_email.py")
    except Exception as e :
        send_message_To_my_telegram("CDP NES Product"+str(datetime.now().date())+history+"上傳失敗","1888352017")
        send_message_To_my_telegram(str(e),"1888352017")
            
        
#
# =============================================================================
#
#    //
# =============================================================================
print("END")
#Repo = LogRepo()
#Repo.Jandi_send(message_type='info', # 有上傳成功、程式錯誤 我的情境應該這兩個就夠了
#                brief="執行完畢— CDP/ 互動行為分析/ CDP NES"
#                ,jandi_url="https://wh.jandi.com/connect-api/webhook/24388692/37194735b24a02d9c23c33342fc78a55")
