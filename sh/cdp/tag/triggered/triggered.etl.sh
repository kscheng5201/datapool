#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tag"
export src_login_path="cdp"
export table_name="triggered"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### Get DateName ####
if [ -n "$1" ]; 
then
    vDateName=`date -d $1 '+%a'`
else
    vDateName=`date -d "1 day ago" '+%a'`
fi

#### Get First and Last Date of Month ####
if [ -n "$1" ]; 
then 
    vMonthFirst=`date -d $1 +"%Y%m01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y%m01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi

#### Get Last Date of Season ####
seasonEnd="
`date +"%Y-03-31"`
`date +"%Y-06-30"`
`date +"%Y-09-30"`
`date +"%Y-12-31"`
"


while read org_id; 
do
    export sql_0="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
    	;"
    echo ''
    echo [Get the web channel db_id on org ${org_id}]
    mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    export sql_1="
        CREATE TABLE ${project_name}.${table_name}_keyword_${org_id} (
            serial int unsigned NOT NULL AUTO_INCREMENT, 
            tag_date date NOT NULL COMMENT '資料統計日',
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',
            domain varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            keyword varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面關鍵字',
            created_at timestamp NOT NULL COMMENT '原始創建時間', 
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            UNIQUE KEY serial (serial) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用戶瀏覽網頁所得的關鍵字'
        ;
        CREATE TABLE ${project_name}.${table_name}_${org_id}_etl (
            serial int unsigned NOT NULL AUTO_INCREMENT, 
            tag_date date NOT NULL COMMENT '資料統計日',
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',
            domain varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            channel 
            tag varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
            freq int NOT NULL COMMENT '貼標次數', 
            origin varchar(32) DEFAULT NULL COMMENT '標籤來源: campaign/event/API/page',
            origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明',
            created_at timestamp NOT NULL COMMENT '原始創建時間', 
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            UNIQUE KEY serial (serial) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用戶瀏覽網頁所得的關鍵字'
        ;"
    echo ''
    echo [CREATE TABLE ${project_name}.${table_name}_keyword_${org_id}]
    echo [CREATE TABLE ${project_name}.${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_etl.error
    

    while read db_id; 
    do    
        export sql_2="
            SET NAMES utf8mb4
            ;
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                fpc, 
                source domain, 
                json_array(page_keyword) keyword, 
                from_unixtime(created_at) created_at, 
                now() updated_at
            from cdp_web_${db_id}.fpc_raw_data
            where created_at >= UNIX_TIMESTAMP('${vDate}') 
                and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                and page_keyword is not null
                and page_keyword <> ''
            ;"
        # Export Data
        echo ''
        echo [Export Data from cdp_web_${db_id}.fpc_raw_data at ${vDate}]
        echo $sql_2
        mysql --login-path=$src_login_path -e "$sql_2" > $export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_keyword_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_keyword_${org_id}.error
        
        # Import Data
        echo ''
        echo 'start: ' date
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_keyword_${org_id}.txt' INTO TABLE ${project_name}.${table_name}_keyword_${org_id}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_keyword_${org_id}.txt' INTO TABLE ${project_name}.${table_name}_keyword_${org_id} IGNORE 1 LINES;" 2>>$error_dir/${project_name}/${table_name}/${project_name}.${table_name}_keyword_${org_id}.error 


        export sql_3="
            # 來自 campaign 所貼上的標籤
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                fpc,
                domain,
                max(a.updated_at) datetime, 
                case channel_type
                    when 1 then 'web'
                    when 2 then 'line'
                    when 3 then 'messenger'
                    when 4 then 'app'
                    when 5 then 'crm'
                else null
                end channel, 
                b.name tag, 
                count(*) freq, 
                'campaign' origin, 
                e.name origin_desc,
                now() created_at, 
                now() updated_at
            from cdp_${org_id}.user_tag a
                inner join cdp_${org_id}.tag b 
                    on a.tag_id = b.id
                
                inner join 
                (
                select db_id, domain
                from cdp_organization.organization_domain 
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) c
                on a.db_id = c.db_id
                
                inner join cdp_web_${db_id}.fpc_unique d
                    on a.channel_id = d.id
                
                inner join cdp_${org_id}.campaign e
                    on a.campaign_id = e.id
                    
            where a.created_at = '${vDate}'
                and campaign_id >= 1
            group by 
                fpc,
                domain,
                channel_type, 
                b.name, 
                e.name
            ;"
        echo ''
        echo [來自 campaign 所貼上的標籤 at ${vDate}]
        echo $sql_3
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.error
        
        # Import Data
        echo ''
        echo 'start: ' date
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${project_name}/${table_name}/${project_name}.${table_name}_${org_id}_etl.error 
        

        export sql_4="
            # 來自 event 所貼上的標籤
            SET NAMES utf8mb4
            ;
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                fpc,
                domain,
                max(a.updated_at) datetime, 
                case channel_type
                    when 1 then 'web'
                    when 2 then 'line'
                    when 3 then 'messenger'
                    when 4 then 'app'
                    when 5 then 'crm'
                else null
                end channel, 
                b.name tag, 
                count(*) freq, 
                'event' origin, 
                f.name orgin_desc,
                now() created_at, 
                now() updated_at
            from cdp_${org_id}.user_tag a
            
                inner join cdp_${org_id}.tag b 
                    on a.tag_id = b.id
                
                inner join cdp_web_${db_id}.fpc_unique d
                    on a.channel_id = d.id
                
                inner join cdp_web_${db_id}.fpc_event_raw_data e
                    on a.channel_id = e.fpc_unique_id
                        and e.created_at = unix_timestamp(a.updated_at)
                
                inner join cdp_organization.events_main f
                    on e.kind = f.kind
                        and e.type = f.type
            where a.created_at = '${vDate}'
                and campaign_id = 0
            group by 
                fpc,
                domain,
                channel_type, 
                b.name, 
                f.name
            ;"
        echo ''
        echo [來自 event 所貼上的標籤 at ${vDate}]
        echo $sql_4
        mysql --login-path=$src_login_path -e "$sql_4" > $export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.error
        
        # Import Data
        echo ''
        echo 'start: ' date
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${project_name}/${table_name}/${project_name}.${table_name}_${org_id}_etl.error 
  

        export sql_5="
            # 來自 API 所貼上的標籤
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                fpc,
                domain,
                max(a.updated_at) datetime, 
                case channel_type
                    when 1 then 'web'
                    when 2 then 'line'
                    when 3 then 'messenger'
                    when 4 then 'app'
                    when 5 then 'crm'
                else null
                end channel, 
                b.name tag, 
                count(*) freq, 
                'API' origin, 
                null orgin_desc,
                now() created_at, 
                now() updated_at
            from cdp_${org_id}.user_tag a
            
                inner join cdp_${org_id}.tag b 
                    on a.tag_id = b.id
                
                inner join cdp_web_${db_id}.fpc_unique d
                    on a.channel_id = d.id
                
                left join cdp_web_${db_id}.fpc_event_raw_data e
                    on a.channel_id = e.fpc_unique_id
                        and e.created_at = unix_timestamp(a.updated_at)
                        
            where a.created_at = '${vDate}'
                and campaign_id = 0
                and e.fpc_unique_id is null
            group by 
                fpc,
                domain,
                channel_type, 
                b.name
            ;"
        echo ''
        echo [來自 API 所貼上的標籤 at ${vDate}]
        echo $sql_5
        mysql --login-path=$src_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.error
        
        # Import Data
        echo ''
        echo 'start: ' date
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${project_name}/${table_name}/${project_name}.${table_name}_${org_id}_etl.error 
  





    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo 'end: ' date
