# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 16:31:47 2021

@author: user001
"""


from db_connected import *
import re

tables =connect_to_3DM_lot_of_methods_mysql_and_return_df('''show tables  from bots_lifespan  where Tables_in_bots_lifespan like "%interaction%" ''',"get","")
def judge_history_table_name(text):   
    if("fb" in text):
        number = re.findall(r'[0-9]+',text )
        company_name = ["nix_fb_"+number[0]+"_etl","nix_fb_"+number[0]+"_history"]
        
        return company_name
    elif("line" in text):
        number = re.findall(r'[0-9]+',text)
        company_name = ["nix_line_"+number[0]+"_etl","nix_line_"+number[0]+"_history"]    
        return company_name
#%%




all_table3=[]

now=datetime.now()
for table in tables.values:
    etl , history = judge_history_table_name(table[0])  #這樣就不會新增錯的表惹
    begin = now+timedelta(days=-91)
    end =  now+timedelta(days=-1)
    print(begin,end)
    
# #檢查是否超過今天
# 判定方式應該為fbmessengerbot_20_interaction.stat_date.min() <= 距離今天180天 所以沒東西 應該是這樣
#     if(end>=now):
#         break
# #檢查資料是否滿足90天        
# #         check_statisfy_90_ =''' select min(stat_date) from bots_lifespan.'''+table[0]
# #         if(connect_to_mysql_and_return_df(check_statisfy_90_,"get").values[0] <= start_date):
# #             print("過去歷史資料90天")
# #         else :
# #             print("不滿足")
# #             break

    query = '''
       SELECT user_id ,first_date , count(stat_date) as sum_count ,            
            min(stat_date) as observation_olddate,            
            max(stat_date) as observation_newdate,          
            datediff(max(stat_date) , min(stat_date)) as observation_interval,     
            datediff("'''+str(end+timedelta(days=1))+'''", first_date )as create_interval,           
            datediff(max(stat_date) , min(stat_date)) / (count(stat_date) - 1) as cycle_time,          
            datediff("'''+str(end+timedelta(days=1))+'''",max(stat_date)) as Recency,
            datediff("'''+str(end+timedelta(days=1))+'''",max(stat_date)) / datediff(max(stat_date) , min(stat_date)) / (count(stat_date) - 1) as rt_ratio      
        FROM bots_lifespan.'''+table[0]+'''       
        where stat_date >="'''+str(begin)+'''" and stat_date<="'''+str(end)+'''"
        group by user_id , first_date
    '''
    print(begin,end)

    data = connect_to_3DM_lot_of_methods_mysql_and_return_df(query,"get","")
    pre_data = feature_engerring_for_nes_new(data)
    pre_data['table3']['start_date'] = str(begin)
    pre_data['table3']['End_date'] = str(end)

# 空的部分再看看要如何解決 - 改成這樣好像不用特表鳥ㄟ 因為會直接+00000然後table2直接沒東西 好ㄟ 2ㄏ


# =============================================================================
# # #那就上傳吧         
# #先上傳到table2
#     connect_to_mysql_and_return_df("truncate table nes_model."+etl ,"add_a_lot" ,  "" )
#     
#     table2_query = "INSERT INTO nes_model."+etl+"(user_id,first_at,sum_count,observation_olddate,observation_newdate,observation_interval,create_interval,cycle_time,Recency,rt_ratio,kind_of_person) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#     table2_data = [tuple(data) for data in pre_data['table2'].values.tolist()]
#     
#     connect_to_mysql_and_return_df(table2_query ,"add_a_lot" ,  table2_data )
# 
# =============================================================================

#接著上傳到table3
    table3_query = "INSERT INTO summary_nix."+history+"(start_date ,End_date ,E0_sum,S1_sum,S2_sum,S3_sum, N_sum) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    table3_data = [tuple(data for data in list(pre_data['table3'].values()))]    
    upload_to_product_db(table3_query ,"add_a_lot" , table3_data)
    
