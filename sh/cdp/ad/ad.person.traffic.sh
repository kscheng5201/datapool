#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 進站前路徑分析 (導流分析)
# Author: Benson Cheng
# Created_at: 2021-12-24
# Updated_at: 2021-12-24
####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type="person"
export table_name="traffic" 
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, traffic_type, referrer, fpc, session_type),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_campaign_id (campaign_id), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天資料）- 有／無效流量之 fpc 整理'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, traffic_type, referrer),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_campaign_id (campaign_id), 
            key idx_span (span),
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at), 
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天資料）- 全部流量／自然流量／廣告流量／其它流量'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_fpc and ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_1    
    mysql --login-path=$dest_login_path -e "$sql_1"




    export sql_2="
        ##【頁面深度分析】導流分析
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                campaign_id,
                traffic_type, 
                referrer, 
                fpc,
                'valid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    fpc, 
                    traffic_type,
                    referrer,
                    session
                from ${project_name}.session_${table_name}_${org_id}_etl
                group by 
                    campaign_id,
                    fpc, 
                    traffic_type,
                    referrer,
                    session
                having count(*) >= 2
                ) a
                
            group by 
                campaign_id,
                traffic_type,
                referrer, 
                fpc
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_fpc AUTO_INCREMENT = 1
        ;"  
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc on valid session]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_fpc.error 
    
    export sql_3="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                campaign_id,
                traffic_type, 
                referrer, 
                fpc,
                'invalid' session_type,
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    fpc, 
                    traffic_type,
                    referrer,
                    session
                from ${project_name}.session_${table_name}_${org_id}_etl
                group by 
                    campaign_id,
                    fpc, 
                    traffic_type,
                    referrer,
                    session
                having count(*) = 1
                ) a
                
            group by 
                campaign_id,
                traffic_type,
                referrer, 
                fpc
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_fpc AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc on invalid session]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_fpc.error 


    export sql_4="
        ##【頁面深度分析】導流分析－流量種類
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date,
                '${vDate}' end_date,  
                a.campaign_id,
                a.traffic_type, 
                'ALL' referrer, 
                ifnull(valid, 0) valid,  
                -1 * ifnull(invalid, 0) invalid,
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by campaign_id order by valid desc) ranking,
                null time_flag, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    traffic_type, 
                    count(distinct fpc) valid
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where session_type = 'valid'
                group by campaign_id, traffic_type
                ) a
                
                left join
                (
                select 
                    campaign_id,
                    traffic_type, 
                    count(distinct fpc) invalid
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where session_type = 'invalid'
                group by traffic_type, campaign_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.traffic_type = b.traffic_type
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} on 'ALL' referrer]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql4.error 

    export sql_5="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date,
                '${vDate}' end_date,  
                a.campaign_id,
                a.traffic_type, 
                a.referrer, 
                ifnull(valid, 0) valid,  
                -1 * ifnull(invalid, 0) invalid,
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by campaign_id order by valid desc) ranking,
                null time_flag, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    traffic_type, 
                    referrer,
                    count(distinct fpc) valid
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where session_type = 'valid'
                group by campaign_id, traffic_type, referrer
                ) a
                
                left join
                (
                select 
                    campaign_id,
                    traffic_type, 
                    referrer,
                    count(distinct fpc) invalid
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where session_type = 'invalid'
                group by traffic_type, campaign_id, referrer
                ) b
                on a.campaign_id = b.campaign_id
                    and a.traffic_type = b.traffic_type
                    and a.referrer = b.referrer
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc on diverse referrer]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql5.error 

    export sql_6="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on time_flag]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql12.error 


    echo ''
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_fpc]
    echo [DROP TABLE ${project_name}.session_${table_name}_${org_id}_etl]    
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_fpc;"
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.session_${table_name}_${org_id}_etl;"

    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt


echo ''
echo 'end: ' `date`            
