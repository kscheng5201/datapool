#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 進站前路徑分析 (廣告登陸頁)
# Author: Benson Cheng
# Created_at: 2021-12-27
# Updated_at: 2021-12-27
# Note: 此檔案必須在 ad.session.traffic.sh 之後執行
####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type_s="session"
export type_p="person"
export table_name="landing" 
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            traffic_type varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Ad/Direct/Organic/Others',
            freq int NOT NULL DEFAULT 0 COMMENT 'frequency of session(工作階段)',
            prop int DEFAULT NULL COMMENT '此 traffic_type 佔整個 campaign 的%',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, traffic_type),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_campaign_id (campaign_id), 
            key idx_span (span),
            key idx_created_at (created_at), 
            key idx_traffic_type (traffic_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析－進站前路徑分析－廣告登陸頁'
        ; 
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            traffic_type varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Ad/Direct/Organic/Others',
            user int NOT NULL DEFAULT 0 COMMENT '不重複 fpc 總計',
            prop int DEFAULT NULL COMMENT '此 traffic_type 佔整個 campaign 的%',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, traffic_type),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_campaign_id (campaign_id), 
            key idx_span (span),
            key idx_created_at (created_at), 
            key idx_traffic_type (traffic_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析－進站前路徑分析－廣告登陸頁'
        ;" 
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id} and ${project_name}.${type_p}_${table_name}_${org_id}]
    echo $sql_1    
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql1.error
    
    export sql_2="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                campaign_id,
                ifnull(traffic_type, 'ALL') traffic_type,
                count(*) freq, 
                null prop, 
                null time_flag, 
                now() created_at, 
                now() updated_at
            from (
                select *, row_number() over (partition by campaign_id, fpc, session order by created_at) rid
                from ${project_name}.${type_s}_traffic_${org_id}_etl 
                ) a
            where rid = 1
            group by 
                campaign_id,
                traffic_type
                    with rollup
            having campaign_id is not null
        ;
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}]
    echo $sql_2  
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql2.error
    
    export sql_3="
        INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                campaign_id,
                ifnull(traffic_type, 'ALL') traffic_type,
                count(distinct fpc) user, 
                null prop, 
                null time_flag, 
                now() created_at, 
                now() updated_at
            from (
                select *, row_number() over (partition by campaign_id, fpc, session order by created_at) rid
                from ${project_name}.${type_s}_traffic_${org_id}_etl 
                ) a
            where rid = 1
            group by 
                campaign_id,
                traffic_type
                    with rollup
            having campaign_id is not null
        ;
        ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_sql3.error

    export sql_4="
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id} a
            INNER JOIN
            (
            select *
            from ${project_name}.${type_s}_${table_name}_${org_id}
            where traffic_type = 'ALL'
            ) b
            ON a.campaign_id = b.campaign_id
                and a.tag_date = b.tag_date
        SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
        WHERE a.tag_date = '${vDate}' + interval 1 day
        ;"
    echo ''
    echo [UPDATE prop ${project_name}.${type_s}_${table_name}_${org_id}]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql4.error
    
    export sql_5="
        UPDATE ${project_name}.${type_p}_${table_name}_${org_id} a
            INNER JOIN
            (
            select *
            from ${project_name}.${type_p}_${table_name}_${org_id}
            where traffic_type = 'ALL'
            ) b
            ON a.campaign_id = b.campaign_id
                and a.tag_date = b.tag_date
        SET a.prop = ifnull(100 * round(a.user / b.user, 2), 0)
        WHERE a.tag_date = '${vDate}' + interval 1 day
        ;"
    echo ''
    echo [UPDATE prop ${project_name}.${type_p}_${table_name}_${org_id}]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql5.error
    
    export sql_6="
        # UPDATE the time_flag
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        ;
        UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        ;"
    echo ''
    echo [UPDATE time_flag on ${project_name}.${type_s}_${table_name}_${org_id} and ${project_name}.${type_p}_${table_name}_${org_id}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql6.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo [end the ${vDate} data at `date`]
