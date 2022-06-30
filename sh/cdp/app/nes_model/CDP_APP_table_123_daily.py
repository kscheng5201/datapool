# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 15:45:39 2021

@author: user001
"""


from db_connected import *
from datetime import date
import re
f = open("/root/datapool/sh/cdp/app/nes_model/app_nes.txt", "a")
#%%


'''
一次撈玩CDP所有的資料
'''
company_query ='''
select nickname,db_id,org_id
from cdp_organization.organization_domain
where domain_type = "app"
order by org_id
'''
#用number一次爬完所有cdp的資料
company_org_id_data = connect_to_CDP_mysql_and_return_df(company_query)
data = pd.DataFrame([{ "org_id": id, "company_name" : company_org_id_data[company_org_id_data.org_id==id].nickname.drop_duplicates().values.tolist()  , "db_id" : company_org_id_data[company_org_id_data.org_id==id].db_id.values.tolist() } for id in company_org_id_data.org_id.unique().tolist()])  
data=data[(data.org_id != 1) & (data.org_id != 8)] #這個的目的是 假裝沒有pchome 和 愛酷 愛酷很扯連DB都沒 哈
#%%
start_time = str(datetime.now().date() + timedelta(days=-2)) 
end_time = str(datetime.now().date() + timedelta(days=-1))

print(start_time+" , "+end_time ,file=f)
# daily　新增資料
def add_app_nes_daily_data(db_id_list,org_id):
    data = pd.DataFrame()
    # 撈出來一天的web event 有舊新增 沒有我也沒辦法
    for db_id in db_id_list:
        query='''
        select b.app,a.identity,date(FROM_UNIXTIME(b.created_at)) as first_at,date(FROM_UNIXTIME(a.created_at)) as stat_date
        	from cdp_app_'''+str(db_id)+'''.app_event_raw_data a, 
        	cdp_app_'''+str(db_id)+'''.app_unique b
        	where a.app_unique_id = b.id and 
        	a.created_at >=unix_timestamp("'''+start_time+'''") and a.created_at<=unix_timestamp("'''+end_time+'''")   
        '''
        # print(query)
        app_ = connect_to_CDP_mysql_and_return_df(query)
        # print(app_)
        data = data.append(app_)
        if(len(app_.columns) ==0):
            print("沒此DB欸喵喵囉")
            create_3table_in_nes_mondel(num)
            print("     新增db"+str(num)+"再看看有無成功", file=f)
            
    print(data)    
    if(data.empty):
        print("     "+"公司編號"+str(org_id)+"沒資料唷 記得寫這一句到LOG", file=f)
    else : 
        data['uuid']=data[['app','identity']].apply(lambda x: x['identity'] if(x['identity']%10==1) else x['app'] ,axis=1)
        data = data.drop(["app","identity"],axis=1)
        data = data.rename(columns={"stat_date":"timestamp"})
        table_name = "app.nes_"+str(org_id)+"_src"
        df_trasform_query_and_upload(data,table_name)
        print("     "+"公司編號"+str(org_id)+"上傳資料成功囉", file=f)
        
# ㄟㄟ 這要怎寫成一行阿 ==
for db_id,org_id in zip(data.db_id,data.org_id):
    add_app_nes_daily_data(db_id,org_id)
    
      
#%%
for org_id in data.org_id:
    print(org_id)
    
    end = datetime.now().date() + timedelta(days=-1)
    begin = datetime.now().date() +timedelta(days=-91)
    today = datetime.now().date() 
    print(begin,end,today)
    
    query = '''
       SELECT uuid ,first_at , count(timestamp) as sum_count ,            
            min(timestamp) as observation_olddate,            
            max(timestamp) as observation_newdate,          
            datediff(max(timestamp) , min(first_at)) as observation_interval,     
            datediff("'''+str(today)+'''", first_at )as create_interval,           
            datediff(max(timestamp) , min(timestamp)) / (count(timestamp) - 1) as cycle_time,          
            datediff("'''+str(end)+'''",max(timestamp)) as Recency,
            datediff("'''+str(end)+'''",max(timestamp)) / datediff(max(timestamp) , min(timestamp)) / (count(timestamp) - 1) as rt_ratio      
        FROM app.nes_'''+str(org_id)+'''_src       
        where timestamp >="'''+str(begin)+'''" and timestamp<="'''+str(end)+'''"
        group by uuid , first_at
    '''

    etl_data = connect_to_3DM_lot_of_methods_mysql_and_return_df(query,"get","")
    pre_data = feature_engerring_for_nes_new(etl_data)
    pre_data['table3']['start_date'] = str(begin)
    pre_data['table3']['End_date'] = str(end)
    
    df_trasform_query_and_upload(pre_data['table2'],"app.nes_"+str(org_id)+"_etl")
    df_trasform_query_and_upload(pd.DataFrame(pre_data['table3'],index=[0]),"app.nes_"+str(org_id))
    
print("     執行結束 - 程式有跑道最後一行唷", file=f)

