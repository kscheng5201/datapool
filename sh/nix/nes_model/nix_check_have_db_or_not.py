# -*- coding: utf-8 -*-

from db_connected import *
tables =connect_to_3DM_lot_of_methods_mysql_and_return_df('''show tables  from bots_lifespan  where Tables_in_bots_lifespan like "%interaction%" ''',"get","")


#%%

import re
def judge_history_table_name(text):   
    if("fb" in text):
        number = re.findall(r'[0-9]+',text )
        company_name = ["nix_fb_"+number[0]+"_etl","nix_fb_"+number[0]+"_history"]
        
        return company_name
    elif("line" in text):
        number = re.findall(r'[0-9]+',text)
        company_name = ["nix_line_"+number[0]+"_etl","nix_line_"+number[0]+"_history"]    
        return company_name
    
final_table_name_history = [judge_history_table_name(i[0]) for i in tables.values]
#%%
import time 
time.sleep(1)
    
#%%
for etl,history in final_table_name_history:
    table3_query = '''
    CREATE TABLE IF NOT EXISTS summary_nix.'''+history+'''(	
        serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number', 
        created_at timestamp not null default current_timestamp comment '創建時間', 
        updated_at timestamp not null default current_timestamp on update current_timestamp comment '更新時間',
        unique idx_SerialNumber (serial),
        `start_date` date not null COMMENT '計算起始日期', 
        `End_date` date not null COMMENT  '計算終止日期', 
        `E0_sum` int(10)  NOT NULL COMMENT '被分群成E0的總人數',
        `S1_sum` int(10)  NOT NULL COMMENT '被分群成S1的總人數',
        `S2_sum` int(10)  NOT NULL COMMENT '被分群成S2的總人數',
        `S3_sum` int(10)  NOT NULL COMMENT '被分群成S3的總人數',
        `N_sum` int(10)   NOT NULL COMMENT '被分群成New的總人數' 
    )ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='nix_line或fb用戶歷史分群個數';    
    '''
    
    upload_to_product_db(table3_query,"normal","")
    time.sleep(1) # cursor一直打怕把他打壞掉 
    print(history)





