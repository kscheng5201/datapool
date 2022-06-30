

-- fpc_event_raw_data COPY
select a.id, fpc, fpc_unique_id, domain, kind, type, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, identity, a.created_at
from cdp_web_42.fpc_event_raw_data a, 
    cdp_web_42.fpc_unique b
where a.created_at < UNIX_TIMESTAMP('20220222' + interval 1 day)
    and a.created_at >= UNIX_TIMESTAMP('20220222') 
    and a.fpc_unique_id = b.id


-- fpc_unique_data COPY
select a.id, fpc, fpc_unique_id, audience_data_id, member_id, name, mobile, email, birth, address, registered_at, pageviews, events, sessions, durations, durations_avg, location, utm_source, referrer, referrer_parameter, device, os, browser, ip, page_url, page_parameter, web_notify_endpoint, identity, fpc_unique_created_at, a.updated_at
from cdp_web_42.fpc_unique_data a, 
    cdp_web_42.fpc_unique b
where (a.fpc_unique_created_at < UNIX_TIMESTAMP('20220222' + interval 1 day)
    and a.fpc_unique_created_at >= UNIX_TIMESTAMP('20220222'))
    and a.fpc_unique_id = b.id



CREATE TABLE IF NOT EXISTS marvin_test.fpc_unique_data (             
   id int unsigned NOT NULL AUTO_INCREMENT, 
   fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
   fpc_unique_id int unsigned NOT NULL DEFAULT '0' COMMENT 'fpc_unique 的 id',      
   audience_data_id int unsigned NOT NULL DEFAULT '0' COMMENT 'audience_data的id',  
   member_id varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',   
   name varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '姓名',           
   mobile varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '手機(10碼數字)',
   email varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'email',        
   birth date NOT NULL COMMENT '生日(ex:2020-01-01)',           
   address varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '地址',       
   registered_at int unsigned NOT NULL DEFAULT '0' COMMENT '會員註冊時間',           
   pageviews smallint unsigned NOT NULL DEFAULT '0' COMMENT '總瀏覽總量',            
   events smallint unsigned NOT NULL DEFAULT '0' COMMENT '事件累積數',               
   sessions smallint unsigned NOT NULL DEFAULT '0' COMMENT '有停留時間的瀏覽總量',    
   durations mediumint unsigned NOT NULL DEFAULT '0' COMMENT '總停留時間',           
   durations_avg float(8,1) unsigned NOT NULL DEFAULT '0.0' COMMENT '平均停留時間( durations / sessions)',                   
   location smallint unsigned NOT NULL DEFAULT '0' COMMENT '最後瀏覽位置(縣市)',      
   utm_source varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm來源', 
   referrer varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '上層來源URL',
   referrer_parameter varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '上層來源URL的參數',     
   device tinyint unsigned NOT NULL DEFAULT '0' COMMENT '最後瀏覽裝置',              
   os tinyint unsigned NOT NULL DEFAULT '0' COMMENT '最後瀏覽的OS',                  
   browser tinyint unsigned NOT NULL DEFAULT '0' COMMENT '最後瀏覽器種類',            
   ip varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '0' COMMENT '最後瀏覽IP',                
   page_url varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '最後瀏覽頁面URL',                
   page_parameter varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '最後瀏覽頁面URL的參數',     
   web_notify_endpoint text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Web Notify 推播 Endpoint',   
   identity int NOT NULL DEFAULT '0' COMMENT '識別是否同一人(還沒綁定fpc_unique_id尾數+0，綁定audience_match尾數+1)',            
   fpc_unique_created_at int unsigned NOT NULL DEFAULT '0' COMMENT 'fpc_unique 的 created_at',          
   updated_at int unsigned NOT NULL DEFAULT '0' COMMENT '資料時間',                  
   PRIMARY KEY (id,updated_at),           
   UNIQUE KEY fuid_unique (fpc_unique_id),  
   KEY fpc (fpc),
   KEY created_at (updated_at),           
   KEY fu_created_at (fpc_unique_created_at),                 
   KEY fuid (fpc_unique_id),              
   KEY identity (identity),               
   KEY audience_data_id (audience_data_id)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每個fpc最後新值'              
;

CREATE TABLE IF NOT EXISTS marvin_test.fpc_event_raw_data (               
   id bigint unsigned NOT NULL AUTO_INCREMENT,  
   fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
   fpc_unique_id int unsigned NOT NULL DEFAULT '0' COMMENT 'fpc_unique 的 id',           
   domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',             
   kind tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件代碼',     
   type tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
   col1 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col2 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col3 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col4 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col5 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col6 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col7 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col8 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col9 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,          
   col10 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,         
   col11 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,         
   col12 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,         
   col13 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,         
   col14 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,         
   identity int NOT NULL DEFAULT '0' COMMENT '識別是否同一人(還沒綁定fpc_unique_id尾數+0，綁定audience_match尾數+1)',                 
   created_at int unsigned NOT NULL DEFAULT '0', 
   PRIMARY KEY (id),              
   KEY fpc (fpc), 
   KEY created_at (created_at),                
   KEY kind_type (kind,type),                
   KEY fpc_unique_id (fpc_unique_id),          
   KEY identity (identity) 
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='fpc事件'     
