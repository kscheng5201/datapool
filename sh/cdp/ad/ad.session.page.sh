#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 進站後頁面瀏覽
# Author: Benson Cheng
# Created_at: 2021-12-24
# Updated_at: 2021-12-30
####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type="session"
export table_name="page" 
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',  
            session int NOT NULL DEFAULT 0 COMMENT 'freqency of session', 
            session_prop int NOT NULL DEFAULT 0 COMMENT '% of session', 
            user int DEFAULT NULL COMMENT 'number of user', 
            user_prop int DEFAULT NULL COMMENT '% of user', 
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, campaign_id, utm_id), 
            key idx_span (span), 
            key idx_tag_date (tag_date),  
            key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站後頁面瀏覽（結算資料）- utm 佔比'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_1    
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql1.error 

    while read campaign_detail; 
    do
        export sql_2="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                # utm 於 campaign 中的分佈，依照流量與人數
                select 
                    null serial, 
                    '${vDate}' + interval 1 day, 
                    campaign_id, 
                    utm_id, 
                    count(distinct fpc, session) session, 
                    null session_prop, 
                    count(distinct fpc) user, 
                    null user_prop, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_both_${org_id}_etl_log
                where campaign_id = '$(echo ${campaign_detail} | cut -d _ -f 1)'
                    and created_at >= '$(echo ${campaign_detail} | cut -d _ -f 2)'
                    and created_at < '$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                    and behavior = 'page_view'
                group by 
                    campaign_id, 
                    utm_id
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
        echo $sql_2
        mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql2.error 
    
        export sql_3="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                # utm 於 campaign 中的分佈，依照總流量與總人數
                select 
                    null serial, 
                    '${vDate}' + interval 1 day, 
                    campaign_id, 
                    0 utm_id, 
                    count(distinct fpc, session) session, 
                    null session_prop, 
                    count(distinct fpc) user, 
                    null user_prop, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                where campaign_id = '$(echo ${campaign_detail} | cut -d _ -f 1)'
                    and created_at >= '$(echo ${campaign_detail} | cut -d _ -f 2)'
                    and created_at < '$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                    and behavior = 'page_view'
                group by 
                    campaign_id
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
        echo $sql_3
        mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql3.error 
    
        export sql_4="
           # utm 於 campaign 中的分佈，計算流量與人數的比例
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}
                where tag_date = '${vDate}' + interval 1 day, 
                    and utm_id = 0
                ) b
                on a.tag_date = b.tag_date
                    and a.campaign_id = b.campaign_id
            SET a.session_prop = round(100 * a.session / b.session), 
                a.user_prop = round(100 * a.user / b.user)
            ;"
        echo ''
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on prop]
        echo $sql_4
        mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql4.error 

        
        export sql_5="
            # 進行中的 campaign 先拿掉 time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET time_flag = null
            WHERE tag_date <= '$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
            ;

            # 進行中的 campaign 再貼上 time_flag 於最新日期上        
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET time_flag = 'last'
            WHERE tag_date <= '$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                and tag_date = '${vDate}' + interval 1 day
                and campaign_id = '$(echo ${campaign_detail} | cut -d _ -f 1)'
            ;"
        echo ''
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on time_flag]
        echo $sql_5
        mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql5.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`      

