#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 進站後頁面瀏覽
# Author: Benson Cheng
# Created_at: 2021-12-24
# Updated_at: 2022-01-05
####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type_s="session"
export type_p="person"
export table_name="title" 
export src_login_path="cdp"


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
`date +"%Y0331"`
`date +"%Y0630"`
`date +"%Y0930"`
`date +"%Y1231"`
`date -d "$(date +%Y-01-01) -1 day" +"%Y-%m-%d"`
"


while read org_id; 
do 
    export sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id}_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_campaign_id (campaign_id), 
            key idx_utm_id (utm_id), 
            key idx_fpc (fpc),
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站後頁面瀏覽（當天資料）- 有／無效流量之 fpc 整理'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_mid (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',            
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部',                         
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, page_title), 
            key idx_created_at (created_at), 
            key idx_tag_date (tag_date),  
            key idx_start_date (start_date),  
            key idx_span (span),
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站後頁面瀏覽（結算資料）- 有／無效流量之次數整理'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            campaign_start date DEFAULT NULL COMMENT '活動開始日期', 
            campaign_end date DEFAULT NULL COMMENT '活動結束日期',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',  
            utm_start date DEFAULT NULL COMMENT 'UTM開始日期', 
            utm_end date DEFAULT NULL COMMENT 'UTM結束日期',            
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部',                         
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, page_title), 
            key idx_span (span), 
            key idx_tag_date (tag_date),  
            key idx_campaign_id (campaign_id),
            key idx_campaign_start (campaign_start),  
            key idx_campaign_end (campaign_end), 
            key idx_utm_id (utm_id),
            key idx_utm_start (utm_start),  
            key idx_utm_end (utm_end),   
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站後頁面瀏覽（結算資料）- 有／無效流量之次數整理'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id}_mid (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',            
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部',                         
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 user', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 user', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 user / (有效 user + 無效 user)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 user / (有效 user + 無效 user)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, page_title), 
            key idx_created_at (created_at), 
            key idx_tag_date (tag_date),  
            key idx_start_date (start_date),  
            key idx_span (span),
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站後頁面瀏覽（結算資料）- 有／無效人數之次數整理'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            campaign_start date DEFAULT NULL COMMENT '活動開始日期', 
            campaign_end date DEFAULT NULL COMMENT '活動結束日期',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',  
            utm_start date DEFAULT NULL COMMENT 'UTM開始日期', 
            utm_end date DEFAULT NULL COMMENT 'UTM結束日期',            
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部',                         
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 user', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 user', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 user / (有效 user + 無效 user)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 user / (有效 user + 無效 user)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, page_title), 
            key idx_span (span), 
            key idx_tag_date (tag_date),  
            key idx_campaign_id (campaign_id),
            key idx_campaign_start (campaign_start),  
            key idx_campaign_end (campaign_end), 
            key idx_utm_id (utm_id),
            key idx_utm_start (utm_start),  
            key idx_utm_end (utm_end),   
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站後頁面瀏覽（結算資料）- 有／無效人數之次數整理'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_fpc and ${project_name}.${type_s}_${table_name}_${org_id}]
    echo $sql_1    
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql1.error 
                    

    export sql_2="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_mid
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'FULL' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                b.campaign_id,
                b.utm_id,
                b.page_title,
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * round(ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) invalid_ratio, 
                row_number () over (partition by b.campaign_id, b.utm_id order by ifnull(valid, 0) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    page_title, 
                    campaign_id, 
                    utm_id,
                    count(distinct fpc, session) valid
                from ${project_name}.${type_s}_both_${org_id}_etl_log
                where session_type = 'valid'
                group by page_title, campaign_id, utm_id
                ) b
                
                left join 
                (
                select 
                    page_title, 
                    campaign_id, 
                    utm_id,
                    count(distinct fpc, session) invalid
                from ${project_name}.${type_s}_both_${org_id}_etl_log
                where session_type = 'invalid'
                group by page_title, campaign_id, utm_id
                ) d
                
                on b.page_title = d.page_title
                    and b.campaign_id = d.campaign_id
                    and b.utm_id = d.utm_id
        ;
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"                
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id} by utm_id]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql2.error 
                    

    export sql_3="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_mid
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'FULL' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                b.campaign_id,
                0 utm_id,
                b.page_title,
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * round(ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) invalid_ratio, 
                row_number () over (partition by b.campaign_id order by ifnull(valid, 0) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    page_title, 
                    campaign_id, 
                    count(distinct fpc, session) valid
                from ${project_name}.${type_s}_both_${org_id}_etl_log
                where session_type = 'valid'
                group by page_title, campaign_id
                ) b
                
                left join 
                (
                select 
                    page_title, 
                    campaign_id, 
                    count(distinct fpc, session) invalid
                from ${project_name}.${type_s}_both_${org_id}_etl_log
                where session_type = 'invalid'
                group by page_title, campaign_id
                ) d
                
                on b.page_title = d.page_title
                    and b.campaign_id = d.campaign_id
        ;
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"                
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id} by campaign_id]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql3.error 


    export sql_delete="
        DELETE
        FROM ${project_name}.${type_s}_${table_name}_${org_id}
        WHERE utm_end >= '${vDate}'
            and span = 'FULL'
        ;
        DELETE
        FROM ${project_name}.${type_s}_${table_name}_${org_id}
        WHERE campaign_end >= '${vDate}'
            and utm_id = 0
            and span = 'FULL'
        ;

        DELETE
        FROM ${project_name}.${type_p}_${table_name}_${org_id}
        WHERE utm_end >= '${vDate}'
            and span = 'FULL'
        ;
        DELETE
        FROM ${project_name}.${type_p}_${table_name}_${org_id}
        WHERE campaign_end >= '${vDate}'
            and utm_id = 0
            and span = 'FULL'
        ;"
    echo ''
    echo [DELETE FROM ${project_name}.${type_s}_${table_name}_${org_id} WHERE utm_end \>= '${vDate}' OR campaign_end \>= '${vDate}' and utm_id = 0]
    echo $sql_delete
    mysql --login-path=$dest_login_path -e "$sql_delete" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_delete.error 


    while read campaign_utm; 
    do                  
        export sql_5="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_utm} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 3)' campaign_end,
                    '$(echo ${campaign_utm} | cut -d _ -f 4)' utm_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 5)' utm_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 6)' utm_end,
                    page_title, 
                    valid, 
                    invalid, 
                    valid_ratio, 
                    invalid_ratio, 
                    ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_s}_${table_name}_${org_id}_mid
                where campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1)
                    and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
            ;
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id} with campaign and utm period]
        echo $sql_5
        mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql5.error 

        export sql_5a="
            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select                             
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_utm} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 3)' campaign_end,
                    '$(echo ${campaign_utm} | cut -d _ -f 4)' utm_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 5)' utm_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 6)' utm_end,
                    page_title, 
                    count(distinct if(session_type = 'valid', fpc, null)) valid, 
                    count(distinct if(session_type = 'invalid', fpc, null)) * -1 invalid, 
                    round(100 * count(distinct if(session_type = 'valid', fpc, null)) / count(distinct fpc)) valid_ratio, 
                    round(100 * count(distinct if(session_type = 'invalid', fpc, null)) / count(distinct fpc)) invalid_ratio, 
                    row_number () over (partition by campaign_id, utm_id order by count(distinct if(session_type = 'valid', fpc, null)) desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at            
                from ${project_name}.${type_s}_both_${org_id}_etl_log
                where campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1)
                    and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
                    and created_at >= '$(echo ${campaign_utm} | cut -d _ -f 5)'
                    and created_at < '$(echo ${campaign_utm} | cut -d _ -f 6)' + interval 1 day
                group by 
                    page_title
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id} where span = 'FULL' and utm_id <> 0]
        echo $sql_5a
        mysql --login-path=$dest_login_path -e "$sql_5a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_sql5a.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.txt
    
    
    while read campaign_detail; 
    do
        export sql_6="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_detail} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_detail} | cut -d _ -f 3)' campaign_end,
                    0 utm_id, 
                    null utm_start, 
                    null utm_end,
                    page_title, 
                    valid, 
                    invalid, 
                    valid_ratio, 
                    invalid_ratio, 
                    ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_s}_${table_name}_${org_id}_mid
                where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    and utm_id = 0
            ;
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id} with campaign period and utm_id = 0]
        echo $sql_6
        mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql6.error 

        export sql_6a="
            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select                             
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_detail} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_detail} | cut -d _ -f 3)' campaign_end,
                    0 utm_id, 
                    null utm_start, 
                    null utm_end,
                    page_title, 
                    count(distinct if(session_type = 'valid', fpc, null)) valid, 
                    count(distinct if(session_type = 'invalid', fpc, null)) * -1 invalid, 
                    round(100 * count(distinct if(session_type = 'valid', fpc, null)) / count(distinct fpc)) valid_ratio, 
                    round(100 * count(distinct if(session_type = 'invalid', fpc, null)) / count(distinct fpc)) invalid_ratio, 
                    row_number () over (partition by campaign_id, utm_id order by count(distinct if(session_type = 'valid', fpc, null)) desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at            
                from ${project_name}.${type_s}_both_${org_id}_etl_log
                where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    and created_at >= '$(echo ${campaign_detail} | cut -d _ -f 2)'
                    and created_at <'$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                group by 
                    page_title
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id} where span = 'FULL' and utm_id = 0]
        echo $sql_6a
        mysql --login-path=$dest_login_path -e "$sql_6a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_sql6a.error 
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt


    export sql_9="
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        WHERE span = 'FULL'
            and campaign_end >= '${vDate}'
            and utm_id = 0
        ; 
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        WHERE span = 'FULL'
            and utm_end >= '${vDate}'
            and utm_id >= 1
        ;         

        UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        WHERE span = 'FULL'
            and campaign_end >= '${vDate}'
            and utm_id = 0
        ; 
        UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        WHERE span = 'FULL'
            and utm_end >= '${vDate}'
            and utm_id >= 1
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type_s}_${table_name}_${org_id} and ${type_p}_${table_name}_${org_id} on time_flag]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql9.error 

    echo ''
    echo [DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_mid]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_mid;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`      
