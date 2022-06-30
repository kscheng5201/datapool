#!/usr/bin/bash
####################################################
# Project: 觸發型標籤
# Branch: 原始上游
# Author: Benson Cheng
# Created_at: 2022-04-29
# Updated_at: 2022-04-29
# Note: 
#####################################################
dest_login_path="datapool_prod"
export_dir="/root/datapool/export_file"
error_dir="/root/datapool/error_log"
project_name="tag_v2"
src_login_path="cdp"
table_name="triggered"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


while read org_id;
do
    echo ''
    echo [BEFOREHAND! DROP TABLE IF EXISTS ${project_name}.${table_name}_keyword_${org_id}]
    echo [BEFOREHAND! DELETE FROM ${project_name}.${table_name}_${org_id}_etl WHERE tag_date <= '${vDate}' - interval 90 day] 
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_keyword_${org_id};"
    mysql --login-path=$dest_login_path -e "DELETE FROM ${project_name}.${table_name}_${org_id}_etl WHERE tag_date <= '${vDate}' - interval 90 day;"

    sql_0="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'app'
    	;"
    echo ''
    echo [Get the app channel db_id on org ${org_id}]
    echo $sql_0
    mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_etl (
            id int unsigned NOT NULL COMMENT '原始 id', 
            tag_date date NOT NULL COMMENT '資料統計日',
            app varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',
            accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id',
            domain varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            datetime timestamp NOT NULL COMMENT '原始創建時間',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web/line/messenger/app/crm', 
            tag varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
            #tag_freq int NOT NULL COMMENT '貼標次數', 
            origin varchar(32) NOT NULL COMMENT '標籤來源: campaign/event/API/page',
            origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明',
            created_at timestamp NOT NULL COMMENT '創建時間', 
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (id, domain, datetime, channel, tag, origin, origin_desc) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='編號 ${org_id} 的客戶，昨天用戶所得的所有標籤'
        ;"
    echo ''
    echo [CREATE TABLE ${project_name}.${table_name}_keyword_${org_id}]
    echo [CREATE TABLE ${project_name}.${table_name}_${org_id}_etl]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_etl.error
    

    while read db_id; 
    do    
        sql_3="
            # 來自 campaign 所貼上的標籤
            SET NAMES utf8mb4
            ;
            select 
                a.id, 
                '${vDate}' + interval 1 day tag_date,
                app,
                null accu_id, 
                (select domain from cdp_organization.organization_domain where org_id = ${org_id} and db_id = ${db_id} and domain_type = 'app') domain,
                (a.updated_at) datetime, 
                case channel_type
                    when 1 then 'web'
                    when 2 then 'line'
                    when 3 then 'messenger'
                    when 4 then 'app'
                    when 5 then 'crm'
                else null
                end channel, 
                b.name tag, 
                #1 freq, 
                'campaign' origin, 
                e.name origin_desc,
                now() created_at, 
                now() updated_at
            from cdp_${org_id}.user_tag a
                inner join cdp_${org_id}.tag b 
                    on a.tag_id = b.id
                
                inner join cdp_app_${db_id}.app_unique d
                    on a.channel_id = d.id
                
                inner join cdp_${org_id}.campaign e
                    on a.campaign_id = e.id
                    
            where a.created_at = '${vDate}'
                and campaign_id >= 1
            ;"
        echo ''
        echo [來自 campaign 所貼上的標籤 at ${vDate}]
        echo $sql_3
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.error
        
        # Import Data
        echo ''
        echo 'start: ' `date`
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${src_login_path}/${project_name}/${project_name}.${table_name}_${org_id}_etl.error 
        

        sql_4="
            # 來自 event 所貼上的標籤
            SET NAMES utf8mb4
            ;
            select 
                a.id,
                '${vDate}' + interval 1 day tag_date,
                app,
                null accu_id, 
                domain,
                (a.updated_at) datetime, 
                case channel_type
                    when 1 then 'web'
                    when 2 then 'line'
                    when 3 then 'messenger'
                    when 4 then 'app'
                    when 5 then 'crm'
                else null
                end channel, 
                b.name tag, 
                #1 freq, 
                'event' origin, 
                f.name orgin_desc,
                now() created_at, 
                now() updated_at
            from cdp_${org_id}.user_tag a
            
                inner join cdp_${org_id}.tag b 
                    on a.tag_id = b.id
                
                inner join cdp_app_${db_id}.app_unique d
                    on a.channel_id = d.id

                inner join cdp_app_${db_id}.app_event_raw_data e
                    on a.channel_id = e.app_unique_id
                        and e.created_at = unix_timestamp(a.updated_at)
                
                inner join cdp_organization.events_main f
                    on e.kind = f.kind
                        and e.type = f.type
            where a.created_at = '${vDate}'
                and campaign_id = 0
            ;"
        echo ''
        echo [來自 event 所貼上的標籤 at ${vDate}]
        echo $sql_4
        mysql --login-path=$src_login_path -e "$sql_4" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.error
        
        # Import Data
        echo ''
        echo 'start: ' `date`
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${src_login_path}/${project_name}/${project_name}.${table_name}_${org_id}_etl.error 
  

        sql_5="
            # 來自 API 所貼上的標籤
            SET NAMES utf8mb4
            ;
            select 
                a.id, 
                '${vDate}' + interval 1 day tag_date,
                app,
                null accu_id, 
                (select domain from cdp_organization.organization_domain where org_id = ${org_id} and db_id = ${db_id} and domain_type = 'app') domain,
                (a.updated_at) datetime, 
                case channel_type
                    when 1 then 'web'
                    when 2 then 'line'
                    when 3 then 'messenger'
                    when 4 then 'app'
                    when 5 then 'crm'
                else null
                end channel, 
                b.name tag, 
                #1 freq, 
                'API' origin, 
                null orgin_desc,
                now() created_at, 
                now() updated_at
            from cdp_${org_id}.user_tag a
            
                inner join cdp_${org_id}.tag b 
                    on a.tag_id = b.id
                
                inner join cdp_app_${db_id}.app_unique d
                    on a.channel_id = d.id
                
                left join cdp_app_${db_id}.app_event_raw_data e
                    on a.channel_id = e.app_unique_id
                        and e.created_at = unix_timestamp(a.updated_at)
                        
            where a.created_at = '${vDate}'
                and campaign_id = 0
                and e.id is null
            ;"
        echo ''
        echo [來自 API 所貼上的標籤 at ${vDate}]
        echo $sql_5
        mysql --login-path=$src_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.error
        
        # Import Data
        echo ''
        echo 'start: ' `date`
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${src_login_path}/${project_name}/${project_name}.${table_name}_${org_id}_etl.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt  
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo 'end: ' `date`
