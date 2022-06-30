#!/usr/bin/bash
####################################################
# Project: 標籤系統
# Branch: mining_label 上游
# Author: Benson Cheng
# Created_at: 2022-04-20
# Updated_at: 2022-04-20
# Note: 
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="mining_label"
export table_name="fpc_raw_data" 
export src_login_path="cdp"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


while read org_id; 
do 
    export sql_0="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id} (
            id bigint NOT NULL COMMENT '原始 id',   
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'domain fpc',
            db_id tinyint unsigned NOT NULL DEFAULT '0' COMMENT '資料庫編號',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',
            created_at date DEFAULT NULL COMMENT '資料原始時間（只保留日期）',   
            weekday varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '星期一到日',
            engage_time_type varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '凌晨(2am–6am)、早上(6am–10am)、中午(10am–2pm)、下午(2pm–6pm)、晚上(6pm–10pm)、深夜(10pm-2am)等6個時段',
            device varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'mobile/tablet/pc',
            updated_at int unsigned NOT NULL DEFAULT '0',   
            PRIMARY KEY (id, domain) USING BTREE,  
            KEY idx_created_at (created_at) USING BTREE,
            KEY idx_weekday (weekday), 
            KEY idx_engage_time_type (engage_time_type), 
            KEY idx_device (device)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc瀏覽紀錄(時段已分類)' 
        ;
        
        CREATE TABLE IF NOT EXISTS ${project_name}.data_pipeline (
            serial INT AUTO_INCREMENT NOT NULL COMMENT '流水號' unique, 
            tag_date DATE NOT NULL COMMENT '程式執行日期', 
            table_name VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '資料表名稱', 
            org_id INT NOT NULL COMMENT '廠商編號', 
            data_dt INT NOT NULL COMMENT '資料原始日期', 
            entries INT NOT NULL COMMENT '資料總量', 
            finished_at DATETIME NOT NULL COMMENT '資料表完成時間', 
            created_at DATETIME NOT NULL COMMENT '資料建立時間', 
            updated_at DATETIME NOT NULL COMMENT '資料更新時間', 
            PRIMARY KEY (table_name, org_id, data_dt), 
            KEY idx_table_name (table_name), 
            KEY idx_org_id (org_id), 
            KEY idx_data_dt (data_dt), 
            KEY idx_created_at (created_at),
            KEY idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='紀錄 ${table_name} 已完成的狀態表'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}]
    echo $sql_0
    #mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}.error


    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
            and deleted_at is null
    	;"
    echo ''
    echo [Get the db_id on web]
    echo $sql_1
    #mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    echo ''
    echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    

    while read db_id; 
    do 
        export sql_3="
            SET NAMES utf8mb4
            ;
            select 
                id,
                fpc, 
                ${db_id} db_id, 
                source domain, 
                '${vDate}' created_at, 
                case 
                    when dayofweek('${vDate}') = 2 then '星期一'
                    when dayofweek('${vDate}') = 3 then '星期二'
                    when dayofweek('${vDate}') = 4 then '星期三'
                    when dayofweek('${vDate}') = 5 then '星期四'
                    when dayofweek('${vDate}') = 6 then '星期五'
                    when dayofweek('${vDate}') = 7 then '星期六'
                    when dayofweek('${vDate}') = 1 then '星期日'
                end as weekday,
                case
                    when hour(from_unixtime(created_at)) >= 2 and hour(from_unixtime(created_at)) <  6 then '凌晨'
                    when hour(from_unixtime(created_at)) >= 6 and hour(from_unixtime(created_at)) < 10 then '早上'
                    when hour(from_unixtime(created_at)) >=10 and hour(from_unixtime(created_at)) < 14 then '中午'
                    when hour(from_unixtime(created_at)) >=14 and hour(from_unixtime(created_at)) < 18 then '下午'
                    when hour(from_unixtime(created_at)) >=18 and hour(from_unixtime(created_at)) < 22 then '晚上'
                else '深夜'
                end as engage_time_type, 
                case 
                    # each phone
                    when user_agent like '%android%' and user_agent like '%mobile%' then 'mobile'
                    when user_agent not like '%windows%' and user_agent like '%iphone%' then 'mobile'
                    when user_agent like '%ipod%' then 'mobile'    
                    when user_agent like '%windows%' and user_agent like '%phone%' then 'mobile'
                    when user_agent like '%blackberry%' and user_agent not like '%tablet%' then 'mobile'    
                    when user_agent like '%fxos%' and user_agent like '%mobile%' then 'mobile'        
                    when user_agent like '%meego%' then 'mobile' 
                    
                    # each tablet
                    when user_agent like '%ipad%' then 'tablet'      
                    when user_agent like '%android%' and user_agent not like '%mobile%' then 'tablet'
                    when user_agent like '%blackberry%' and user_agent like '%tablet%' then 'tablet'
                    when user_agent like '%windows%' and (user_agent like '%touch%' and user_agent not like (user_agent like '%windows%' and user_agent like '%phone%')) then 'tablet'
                    when user_agent like '%fxos%' and user_agent like '%tablet%' then 'tablet' 
                    
                    # desktop
                    when user_agent not like '%tablet%' and user_agent not like '%mobile%' then 'pc'
                    
                    else null
                end as device, 
                unix_timestamp(now()) updated_at
            from cdp_web_${db_id}.fpc_raw_data
            where created_at >= unix_timestamp('${vDate}')
                and created_at < unix_timestamp('${vDate}' + interval 1 day)
            ;"
        echo ''
        echo #### Export Data ####
        echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}.txt]
        echo $sql_3
        #mysql --login-path=${src_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.error

        echo ''
        echo #### Import Data ####
        echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}.txt to ${project_name}.${table_name}_${org_id}]
        #mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt' IGNORE INTO TABLE ${project_name}.${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    
    export sql_4="
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                '${table_name}_${org_id}' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                from_unixtime(max(updated_at)) finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id}
            where created_at >= ('${vDate}')
                and created_at < ('${vDate}' + interval 1 day)
        ;
        
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_4.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt

