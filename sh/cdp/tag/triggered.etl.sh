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
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
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
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y%m%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y%m01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y%m%d"`
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
    echo ''
    echo [BEFOREHAND! DROP TABLE ${project_name}.${table_name}_keyword_${org_id}]
    echo [BEFOREHAND! DELETE FROM ${project_name}.${table_name}_${org_id}_etl WHERE tag_date <= '${vDate}' - interval 90 day] 
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${table_name}_keyword_${org_id};"
    mysql --login-path=$dest_login_path -e "DELETE FROM ${project_name}.${table_name}_${org_id}_etl WHERE tag_date <= '${vDate}' - interval 90 day;"

    export sql_0="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
    	;"
    echo ''
    echo [Get the web channel db_id on org ${org_id}]
    echo $sql_0
    mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    export sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_keyword_${org_id} (
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_etl (
            serial int unsigned NOT NULL AUTO_INCREMENT, 
            tag_date date NOT NULL COMMENT '資料統計日',
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',
            domain varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            datetime timestamp NOT NULL COMMENT '原始創建時間',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web/line/messenger/app/crm', 
            tag varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
            freq int NOT NULL COMMENT '貼標次數', 
            origin varchar(32) DEFAULT NULL COMMENT '標籤來源: campaign/event/API/page',
            origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明',
            created_at timestamp NOT NULL COMMENT '創建時間', 
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            UNIQUE KEY serial (serial) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='編號 3 的客戶，近 90 天用戶所得的所有標籤'
        ;"
    echo ''
    echo [CREATE TABLE ${project_name}.${table_name}_keyword_${org_id}]
    echo [CREATE TABLE ${project_name}.${table_name}_${org_id}_etl]
    echo $sql_1
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
                replace(json_array(page_keyword), ', ', '\", \"') keyword,
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
        echo 'start: ' `date`
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
                min(a.updated_at) datetime, 
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
        echo 'start: ' `date`
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
                min(a.updated_at) datetime, 
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
        echo 'start: ' `date`
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
                min(a.updated_at) datetime, 
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
        echo 'start: ' `date`
        echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/${project_name}.${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${project_name}/${table_name}/${project_name}.${table_name}_${org_id}_etl.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt  


    ##########################
    #### WORK in DATAPOOL ####
    ##########################
    
    export sql_6="
        INSERT INTO ${project_name}.${table_name}_${org_id}_etl
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                fpc, 
                domain, 
                min(created_at) datetime, 
                'web' channel, 
                tag, 
                count(*) freq, 
                'page_view' origin, 
                'keyword' origin_desc, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    t.fpc, 
                    t.domain, 
                    t.created_at, 
                    tag
                from ${project_name}.${table_name}_keyword_${org_id} t
                join json_table (
                    t.keyword, 
                    '$[*]' columns (tag varchar(16) path '$')
                    ) j
                ) a
            group by fpc, domain, tag
        ;"    
    echo ''
    echo [INSERT INTO ${project_name}.${table_name}_${org_id}_etl FROM ${project_name}.${table_name}_keyword_${org_id}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_etl.error
    
    export sql_7="
        CREATE TABLE ${project_name}.${table_name}_${org_id}_used (
            serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號' unique, 
            tag_date date NOT NULL COMMENT '資料運算日',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web/line/messenger/app/crm',    
            tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',  
            freq int unsigned NOT NULL DEFAULT '0' COMMENT '貼標次數',  
            user int unsigned NOT NULL DEFAULT '0' COMMENT '貼標人數',  
            origin varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '廣告/事件/網站頁面/自定義',    
            behavior varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '點擊UTM/(事件名稱)/頁面瀏覽/(-)',
            created_at timestamp NOT NULL COMMENT '原始創建時間', 
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            KEY idx_tag_date (tag_date), 
            KEY idx_channel (channel),
            KEY idx_tag (tag),
            KEY idx_origin (origin), 
            KEY idx_behavior (behavior)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='編號 ${org_id} 的客戶，近 90 天的觸發型標籤總表現'
        ;
        CREATE TABLE ${project_name}.${table_name}_${org_id}_user (
            serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號' unique, 
            tag_date date NOT NULL COMMENT '資料運算日',
            start_date date NOT NULL COMMENT '90 天起始日',     
            end_date date NOT NULL COMMENT '90 天最末日',
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',     
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web/line/messenger/app/crm',    
            tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',  
            freq int unsigned NOT NULL DEFAULT '0' COMMENT '貼標次數',  
            last_at datetime NOT NULL COMMENT '最近貼標時間',   
            ranking int NOT NULL COMMENT '標籤濃度', 
            created_at timestamp NOT NULL COMMENT '創建時間', 
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            KEY idx_tag_date (tag_date), 
            KEY idx_channel (channel),
            KEY idx_tag (tag),
            KEY idx_fpc (fpc)  
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='編號 ${org_id} 的客戶，近 90 天的用戶標籤表現'
        ;"
    echo ''
    echo [CREATE TABLE ${project_name}.${table_name}_keyword_${org_id}]
    echo [CREATE TABLE ${project_name}.${table_name}_${org_id}_etl]
    echo $sql_7
    mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_user.error
     
    export sql_8="
        INSERT INTO ${project_name}.${table_name}_${org_id}_used
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                channel, 
                tag, 
                sum(freq) freq, 
                count(distinct fpc) user,
                case origin
                    when 'campaign'     then '廣告'
                    when 'event'        then '事件'
                    when 'page_view'    then '網站頁面'
                    when 'API'          then '自定義'
                    else null
                end origin, 
                case origin
                    when 'campaign'     then '點擊UTM'
                    when 'event'        then origin_desc
                    when 'page_view'    then '頁面瀏覽'
                    when 'API'          then '-'
                    else null
                end behavior, 
                min(datetime) created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id}_etl
            where origin in ('campaign', 'event', 'page_view')
                and tag is not null
                and tag <> ''
            group by 
                domain, 
                channel, 
                tag, 
                origin
        ;
        ALTER TABLE ${project_name}.${table_name}_${org_id}_used AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${table_name}_${org_id}_used]
    echo $sql_8
    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_used.error
    
    export sql_9="
        INSERT INTO ${project_name}.${table_name}_${org_id}_user
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                fpc, 
                channel, 
                tag, 
                freq, 
                datetime last_at, 
                floor((1 - rid / sum_over) * 100) ranking, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    min(datetime) datetime, 
                    channel, 
                    replace(tag, ' ', '') tag, 
                    sum(freq) freq, 
                    sum(1) over (partition by tag) sum_over, 
                    row_number () over (partition by tag order by sum(freq) desc, min(datetime) desc) rid
                from ${project_name}.${table_name}_${org_id}_etl
                where origin in ('campaign', 'event', 'page_view')
                    and tag is not null
                    and tag <> ''
                group by 
                    fpc, 
                    channel, 
                    tag
                ) a
        ;
        ALTER TABLE ${project_name}.${table_name}_${org_id}_user AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${table_name}_${org_id}_user]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_user.error
        
    echo ''
    echo [AFTERWARDS! DROP TABLE ${project_name}.${table_name}_keyword_${org_id}]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${table_name}_keyword_${org_id};"

    echo ''
    echo [Keep the last 10 days data ONLY]
    echo [AFTERWARDS! DELETE FROM ${project_name}.${table_name}_${org_id}_used]
    echo [AFTERWARDS! DELETE FROM ${project_name}.${table_name}_${org_id}_user]
    mysql --login-path=$dest_login_path -e "DELETE FROM ${project_name}.${table_name}_${org_id}_used WHERE tag_date <= '${vDate}' - interval 9 day;"
    mysql --login-path=$dest_login_path -e "DELETE FROM ${project_name}.${table_name}_${org_id}_user WHERE tag_date <= '${vDate}' - interval 9 day;"
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo 'end: ' `date`
