#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Author: Benson Cheng
# Created_at: 2021-12-16
# Updated_at: 2021-12-16
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type="session"
export table_name="kpi" 
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) signed NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
            click int DEFAULT 0 NULL COMMENT '點擊次數',  
            user int DEFAULT 0 NULL COMMENT '進站人數', 
    	    new int DEFAULT 0 NULL COMMENT '新用戶數',
            new_prop int DEFAULT 0 NULL COMMENT '新用戶比例', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id),
            key idx_tag_date (tag_date), 
            key idx_span (span),             
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【指標統計】'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, fpc),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date), 
            key idx_domain (domain), 
            key idx_fpc (fpc)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每日 unique fpc 原始表'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_graph (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            x_axis date NOT NULL COMMENT '供前後端工程師繪製趨勢圖時的時間座標',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            session int DEFAULT 0 NULL COMMENT '工作階段數量',  
            user int DEFAULT 0 NULL COMMENT '不重複上線用戶', 
            reg int DEFAULT 0 NULL COMMENT '新用戶註冊數',
            page_view int DEFAULT 0 NULL COMMENT '頁面瀏覽次數',   
            event int DEFAULT 0 NULL COMMENT '事件觸發次數',
            stay_time int DEFAULT 0 NULL COMMENT '平均停留秒數', 
            invalid_session int DEFAULT 0 NULL COMMENT '僅動作一次的工作階段數量',
            bounce_rate int DEFAULT 0 NULL COMMENT '跳出率(%)', 
            #session_gr int DEFAULT 0 NULL COMMENT '工作階段數量, 相較上期之成長率(%)', 
            #user_gr int DEFAULT 0 NULL COMMENT '不重複上線用戶, 相較上期之成長率(%)',
            #reg_gr int DEFAULT 0 NULL COMMENT '新用戶註冊數, 相較上期之成長率(%)', 
            #page_view_gr int DEFAULT 0 NULL COMMENT '頁面瀏覽次數, 相較上期之成長率(%)', 
            #event_gr int DEFAULT 0 NULL COMMENT '事件觸發次數, 相較上期之成長率(%)', 
            #stay_time_gr int DEFAULT 0 NULL COMMENT '平均停留秒數, 相較上期之成長率(%)', 
            #bounce_rate_gr int DEFAULT 0 NULL COMMENT '跳出率(%), 相較上期之成長率(%)',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),
            key idx_domain (domain)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【長期統計】'
        ;"
    echo ''
    echo $sql_1
    echo [start: date on ${vDate}]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "$sql_1"


    export sql_2="
        select 
            null serial, 
            '${vDate}' + interval 1 day tag_date,
            'daily' span,
            '${vDate}' start_date,
            '${vDate}' end_date,
            campaign_id, 
            utm_id, 
            count(*) click,
            count(distinct concat_ws('|', db_id, channel_id)) user, 
            count(distinct(
                if(user_created_at = date_format(updated_at + interval 8 hour, '%Y%m%d'), concat_ws('|', db_id, channel_id), null)
                )) new, 
            100 * round(count(distinct(if(user_created_at = date_format(updated_at + interval 8 hour, '%Y%m%d'), concat_ws('|', db_id, channel_id), null))) / count(distinct concat_ws('|', db_id, channel_id)), 2) new_prop, 
            now() created_at, 
            now() updated_at
        from (
            # 取出正確日期的 00:00-15:59 資料
            select *
            from cdp_${org_id}.user_utm
            where created_at = ${vDate}
                and created_at = date_format(updated_at + interval 8 hour, '%Y%m%d')
                and updated_at >= '${vDate}'
		and channel_type = 1
        
            UNION ALL
        
            # 取出正確日期的 16:00-23:59 資料
            select *
            from cdp_${org_id}.user_utm
            where created_at = ${vDate} + 1
                and created_at = date_format(updated_at + interval 8 hour, '%Y%m%d')
                and updated_at < '${vDate}' + interval 1 day
		and channel_type = 1
            ) a
        group by
            campaign_id, 
            utm_id
        
        
        UNION ALL
        
        
        select 
            null serial, 
            '${vDate}' + interval 1 day tag_date,
            'daily' span,
            '${vDate}' start_date,
            '${vDate}' end_date,
            campaign_id, 
            null utm_id, 
            count(*) click,
            count(distinct concat_ws('|', db_id, channel_id)) user, 
            count(distinct(
                if(user_created_at = date_format(updated_at + interval 8 hour, '%Y%m%d'), concat_ws('|', db_id, channel_id), null)
                )) new, 
            100 * round(count(distinct(if(user_created_at = date_format(updated_at + interval 8 hour, '%Y%m%d'), concat_ws('|', db_id, channel_id), null))) / count(distinct concat_ws('|', db_id, channel_id)), 2) new_prop, 
            now() created_at, 
            now() updated_at
        from (
            # 取出正確日期的 00:00-15:59 資料
            select *
            from cdp_${org_id}.user_utm
            where created_at = ${vDate}
                and created_at = date_format(updated_at + interval 8 hour, '%Y%m%d')
                and updated_at >= '${vDate}'
                and channel_type = 1
        
            UNION ALL
        
            # 取出正確日期的 16:00-23:59 資料
            select *
            from cdp_${org_id}.user_utm
            where created_at = ${vDate} + 1
                and created_at = date_format(updated_at + interval 8 hour, '%Y%m%d')
                and updated_at < '${vDate}' + interval 1 day
                and channel_type = 1
            ) a
        group by
            campaign_id
    	;"
    echo $sql_2
    #### Export Data ####
    echo ''
    echo [start: `date` on ${vDate}]
    echo [exporting data to ${project_name}.${table_name}_${org_id}_src.txt]
    mysql --login-path=${src_login_path}_master -e "$sql_2" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_src_sql_2.error

    #### Import Data ####
    echo ''
    echo [start: `date` on ${vDate}]
    echo [import data from ${project_name}.${table_name}_${org_id}_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_src.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_src_sql_2.error 


    export sql_3="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    export sql_4="
        select concat_ws('_', campaign_id, utm_id, campaign_start, campaign_end) utm_detail
        from ${project_name}.fpc_${org_id}
        group by campaign_id, utm_id, campaign_start, campaign_end
    	;"
    echo ''
    echo [Get the utm_detail on web]
    mysql --login-path=${dest_login_path} -e "$sql_4" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt
    
    
    


done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`
