echo `date`
#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type="session"
export table_name="both" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### Get DateName (Mon to Sun)
if [ -n "$1" ]; 
then
    vDateName=`date -d $1 '+%a'`
else
    vDateName=`date -d "1 day ago" '+%a'`
fi

#### Get the First and Last Date of Month ####
if [ -n "$1" ]; 
then 
    vMonthFirst=`date -d $1 +"%Y-%m-01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y-%m-01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi



while read org_id; 
do 
    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
            and domain NOT REGEXP 'deprecated'
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt



    while read db_id; 
    do
        export sql_2="    
            CREATE TABLE IF NOT EXISTS marvin_test.fpc_unique_data_${db_id} (             
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
            CREATE TABLE IF NOT EXISTS marvin_test.fpc_event_raw_data_${db_id} (               
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
            ;" 
        echo ''
        echo [create table if not exists ${project_name}.${type}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error

    
        export sql_5="
            select a.id, fpc, fpc_unique_id, domain, kind, type, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, identity, a.created_at
            from cdp_web_${db_id}.fpc_event_raw_data a, 
                cdp_web_${db_id}.fpc_unique b
            where a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                and a.fpc_unique_id = b.id
            ;"
        echo $sql_5
        #### Export Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        echo [exporting data to ${project_name}.fpc_event_raw_data.txt]
        mysql --login-path=${src_login_path} -e "$sql_5" > $export_dir/$src_login_path/$project_name/fpc_event_raw_data_${db_id}.txt 2>>$error_dir/$src_login_path/$project_name/fpc_event_raw_data_${db_id}.error
    
        #### Import Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/fpc_event_raw_data_${db_id}.txt' INTO TABLE marvin_test.fpc_event_raw_data_${db_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/fpc_event_raw_data_${db_id}.error 
    
    
        export sql_4="
            select a.id, fpc, fpc_unique_id, audience_data_id, member_id, name, mobile, email, birth, address, registered_at, pageviews, events, sessions, durations, durations_avg, location, utm_source, referrer, referrer_parameter, device, os, browser, ip, page_url, page_parameter, web_notify_endpoint, identity, fpc_unique_created_at, a.updated_at
            from cdp_web_${db_id}.fpc_unique_data a, 
                cdp_web_${db_id}.fpc_unique b
            where a.fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                and a.fpc_unique_id = b.id
            ;"
        echo $sql_4
        #### Export Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        echo [exporting data to ${project_name}.fpc_unique_data.txt]
        mysql --login-path=${src_login_path} -e "$sql_4" > $export_dir/$src_login_path/$project_name/fpc_unique_data_${db_id}.txt 2>>$error_dir/$src_login_path/$project_name/fpc_unique_data_${db_id}.error
    
        #### Import Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/fpc_unique_data_${db_id}.txt' INTO TABLE marvin_test.fpc_unique_data_${db_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/fpc_unique_data_${db_id}.error 
    
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt


    export sql_6="
        CREATE TABLE IF NOT EXISTS marvin_test.accu_mapping_${org_id} (      
           serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number', 
           accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',      
           channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)',  
           org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',       
           db_id tinyint unsigned NOT NULL DEFAULT '0' COMMENT '資料來自哪個DB',    
           channel_id int unsigned NOT NULL DEFAULT '0' COMMENT 'unique_id/channel_id',  
           id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',       
           id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明', 
           browser_fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',  
           member_id varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID', 
           first_at datetime DEFAULT NULL COMMENT '此 fpc 首次出現時間',  
           registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)', 
           created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間', 
           updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
           PRIMARY KEY (serial),
           KEY idx_id (id),
           KEY idx_id_type (id_type),      
           KEY idx_org_id (org_id),        
           KEY idx_channel (channel),      
           KEY idx_channel_id (channel_id),
           KEY idx_db_id (db_id),
           KEY idx_member_id (member_id),  
           KEY idx_browser_fpc (browser_fpc),         
           KEY idx_id_id_type (id,id_type),         
           KEY idx_updated_at (updated_at) 
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
        ;"
    echo ''
    echo $sql_6
    mysql --login-path=${dest_login_path} -e "$sql_6"


    export sql_7="
        select *
        from uuid.accu_mapping_${org_id}
        ;"
    echo $sql_7
    #### Export Data ####
    echo ''
    echo [start: `date` on ${vDate}]
    echo [exporting data to ${project_name}.fpc_event_raw_data.txt]
    mysql --login-path=${dest_login_path}_prod -e "$sql_7" > $export_dir/$src_login_path/$project_name/accu_mapping_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/accu_mapping_${org_id}.error

    #### Import Data ####
    echo ''
    echo [start: `date` on ${vDate}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/accu_mapping_${org_id}.txt' INTO TABLE marvin_test.accu_mapping_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/accu_mapping_${org_id}.error 

    


done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo `date`
