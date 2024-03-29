#!/usr/bin/bash
################
# Note: 
# 2022-05-13: _traffic_fpc 的日表增加 [and behavior = 'page_view'] 條件，
#             避免事件的資料納入計算，不讓 traffic_type = 'NULL' 出現。
###############


export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type="person"
export table_name="page" 
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

#### Get the Last Date of Season ####
seasonEnd="
`date +"%Y-03-31"`
`date +"%Y-06-30"`
`date +"%Y-09-30"`
`date +"%Y-12-31"`
`date -d "$(date +%Y-01-01) -1 day" +"%Y-%m-%d"`
"

################################################################################
echo ''
echo [start the job session.page_new.sh on ${vDate} at `date`]

while read org_id; 
do 
    export sql_1="  
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_landing (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            landing varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '每個 session 開始時的進入 domain',
            traffic_type varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            user int NOT NULL DEFAULT 0 COMMENT 'frequency of unique user',
            prop int DEFAULT NULL COMMENT '此 domain 佔所有 domain 的%',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, landing, traffic_type),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_landing (landing), 
            key idx_span (span),
            key idx_created_at (created_at), 
            key idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】-導流分析／廣告流量-登入網域'
        ; 
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_domain (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            user int NOT NULL DEFAULT 0 COMMENT 'frequency of unique user',
            prop int DEFAULT NULL COMMENT '此 domain 佔所有 domain 的%',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_domain (domain), 
            key idx_span (span),
            key idx_created_at (created_at), 
            key idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】-熱門頁面-瀏覽網域'
        ; 
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_traffic (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, traffic_type, referrer, domain),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_domain (domain), 
            key idx_span (span),
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at), 
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】導流分析-流量種類/自然流量/其它流量'
        ;
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_campaign (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'weekly/monthly/seasonal', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_campaign廣告活動; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign, domain),
            key idx_created_at (created_at), 
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_domain (domain), 
            key idx_span (span),
            key idx_campaign (campaign),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告活動'
        ;        
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_medium (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'yearweek/weekly/monthly/seasonal', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            source_medium varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_medium廣告來源 x utm_medium廣告媒介; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, source_medium, domain),
            key idx_created_at (created_at),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),  
            key idx_domain (domain), 
            key idx_span (span),
            key idx_medium_medium (source_medium),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告來源x廣告媒介'
        ;               
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_title (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, page_title, domain),
            key idx_created_at (created_at),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),  
            key idx_domain (domain), 
            key idx_span (span),
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】熱門頁面-頁面標題'
        ;
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_domain_fpc (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日', 
    	    landing varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session 開始時的第一個 domain',
            traffic_type varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
    	    session varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_fpc (accu_id), 
            key idx_domain (domain),
            key idx_session (session), 
            key idx_landing (landing), 
            key idx_traffic_type (traffic_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 accu_id on 綜合儀表板【頁面深度分析】-熱門頁面-瀏覽網域'
        ; 
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 accu_id on 綜合儀表板【頁面深度分析】導流分析-流量種類/自然流量/其它流量'
        ;
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_campaign廣告活動; # = 全部', 
    	    source_medium varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_medium廣告來源 x utm_medium廣告媒介; # = 全部',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_source_medium (source_medium), 
            key idx_domain (domain), 
            key idx_campaign (campaign), 
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_fpc (accu_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 accu_id on 綜合儀表板【頁面深度分析】廣告流量-廣告活動'
        ;        
       create table if not exists ${project_name}.${type}_${table_name}_${org_id}_title_fpc (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_page_title (page_title), 
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_fpc (accu_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 accu_id on 綜合儀表板【頁面深度分析】熱門頁面-頁面標題'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 

    export sql_2a="  
        ##【頁面深度分析】登入網域 vs 瀏覽網域
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                landing, 
                traffic_type,
                domain, 
                a.accu_id, 
                a.session, 
                now() created_at, 
                now() updated_at
            from (
                select accu_id, session, domain landing, traffic_type
                from (
                    select *, row_number () over (partition by accu_id, session order by created_at) rid
                    from ${project_name}.session_both_${org_id}_etl
                    where behavior = 'page_view'
                    ) a
                where rid = 1
                group by accu_id, session, domain
                ) a
                
                inner join
                (
                select accu_id, session, domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                group by accu_id, session, domain
                ) b
                on a.accu_id = b.accu_id
                    and a.session = b.session

            group by 
                landing, 
                domain, 
                a.accu_id, 
                a.session
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain_fpc AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain_fpc]
    echo $sql_2a
    mysql --login-path=$dest_login_path -e "$sql_2a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_fpc_daily.error 

    export sql_2b="
        ##【頁面深度分析】導流分析
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                traffic_type, 
                if(referrer = 'NULL', '', referrer) referrer,
                domain, 
                accu_id,
                'valid' session_type, 
                now() created_at, 
                now() updated_at
            from ${project_name}.session_both_${org_id}_etl
            where session_type = 'valid'
                and behavior = 'page_view'
            group by 
                traffic_type,
                if(referrer = 'NULL', '', referrer), 
                domain, 
                accu_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc AUTO_INCREMENT = 1
        ;  
        
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                traffic_type, 
                if(referrer = 'NULL', '', referrer) referrer, 
                domain, 
                accu_id,
                'invalid' session_type,
                now() created_at, 
                now() updated_at
            from ${project_name}.session_both_${org_id}_etl
            where session_type = 'invalid'
                and behavior = 'page_view'
            group by 
                traffic_type,
                if(referrer = 'NULL', '', referrer), 
                domain, 
                accu_id
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc]
    echo $sql_2b
    mysql --login-path=$dest_login_path -e "$sql_2b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_fpc_daily.error 


    export sql_2c="
        ##【頁面深度分析】廣告流量
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                campaign,
                source_medium, 
                domain, 
                accu_id,
                'valid' session_type,
                now() created_at, 
                now() updated_at
            from ${project_name}.session_both_${org_id}_etl
            where session_type = 'valid'
                and behavior = 'page_view'
                and traffic_type = 'Ad'
                and campaign is not null
                and campaign <> 'NULL'
                and campaign <> ''
            group by 
                campaign,
                source_medium, 
                domain, 
                accu_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc AUTO_INCREMENT = 1
        ;

        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                campaign,
                source_medium, 
                domain, 
                accu_id,
                'invalid' session_type, 
                now() created_at, 
                now() updated_at
            from ${project_name}.session_both_${org_id}_etl
            where session_type = 'invalid'
                and behavior = 'page_view'
                and traffic_type = 'Ad'
                and campaign is not null
                and campaign <> 'NULL'
                and campaign <> ''
            group by 
                campaign,
                source_medium, 
                domain, 
                accu_id
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc]
    echo $sql_2c
    mysql --login-path=$dest_login_path -e "$sql_2c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_fpc_daily.error 

    export sql_2d="
        ##【頁面深度分析】熱門頁面
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                page_title, 
                domain, 
                accu_id,
                'valid' session_type, 
                now() created_at, 
                now() updated_at
            from ${project_name}.session_both_${org_id}_etl
            where session_type = 'valid'
                and behavior = 'page_view'
            group by 
                page_title, 
                domain, 
                accu_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title_fpc AUTO_INCREMENT = 1
        ;
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span,  
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                page_title, 
                domain, 
                accu_id,
                'invalid' session_type, 
                now() created_at, 
                now() updated_at
            from ${project_name}.session_both_${org_id}_etl
            where session_type = 'invalid'
                and behavior = 'page_view'
            group by 
                page_title, 
                domain, 
                accu_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title_fpc AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium]
    echo $sql_2d
    mysql --login-path=$dest_login_path -e "$sql_2d" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_medium_daily.error 



    if [ ${vDateName} = Sun ]; 
    then 
        export sql_4a="
            ##【頁面深度分析】登入網域 vs 瀏覽網域       
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,
                    landing, 
                    traffic_type,
                    count(distinct fpc) user,
                    null prop, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by landing, traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    'ALL' landing, 
                    traffic_type,
                    count(distinct fpc) user,
                    100 prop,
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date,
                    '${vDate}' end_date,
                    ifnull(landing, 'ALL') landing,
                    'ALL' traffic_type, 
                    count(distinct fpc) user,
                    null prop,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by landing
                    with rollup
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            # 計算個別網域的比例分配
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                INNER JOIN
                (
                select traffic_type, sum(user) user
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'weekly'
                    and landing <> 'ALL'
                group by traffic_type
                ) b
                ON a.traffic_type = b.traffic_type
            SET a.prop = ifnull(100 * round(a.user / b.user, 2), 0)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'weekly'
                and a.landing <> 'ALL'
            ;

            # UPDATE the weekly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing weekly]
        echo $sql_4a
        mysql --login-path=$dest_login_path -e "$sql_4a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_landing_weekly.error 


        export sql_4b="
            ##【頁面深度分析】導流分析－流量種類
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.traffic_type, 
                    'ALL' referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by traffic_type, domain
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by traffic_type, domain
                    ) b
                    on a.domain = b.domain
                        and a.traffic_type = b.traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.traffic_type, 
                    'ALL' referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by traffic_type
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by traffic_type
                    ) b
                    on a.traffic_type = b.traffic_type
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;
            
            ##【頁面深度分析】導流分析－自然流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Organic'
                    group by referrer, domain
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Organic'
                    group by referrer, domain
                    ) b
                    on a.domain = b.domain
                        and a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) a
                    
                    left join
                    (
                    select 
                        referrer,
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;


            ##【頁面深度分析】導流分析－廣告流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date,
                    '${vDate}' end_date,  
                    'Ad' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Ad'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1), 
                        domain
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Ad'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1), 
                        domain
                    ) b
                    on a.domain = b.domain
                        and a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date,
                    '${vDate}' end_date,  
                    'Ad' traffic_type, 
                    a.referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Ad'
                    group by
                        substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer,
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Ad'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;
            
            ##【頁面深度分析】導流分析－其它流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Others' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Others'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1),
                        domain
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Others'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1),
                        domain
                    ) b
                    on a.domain = b.domain
                        and a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Others' traffic_type, 
                    a.referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Others'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Others'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;


            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'weekly'
                and referrer = 'ALL'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'weekly'
                and referrer = 'ALL'
                and tag_date = '${vDate}' + interval 1 day
            ;                       
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'weekly'
                and traffic_type = 'Organic'
                and referrer <> 'ALL'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'weekly'
                and traffic_type = 'Organic'
                and referrer <> 'ALL'
                and tag_date = '${vDate}' + interval 1 day
            ;           
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'weekly'
                and traffic_type = 'Ad'
                and referrer <> 'ALL'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'weekly'
                and traffic_type = 'Ad'
                and referrer <> 'ALL'
                and tag_date = '${vDate}' + interval 1 day
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'weekly'
                and traffic_type = 'Others'
                and referrer <> 'ALL'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'weekly'
                and traffic_type = 'Others'
                and referrer <> 'ALL'
                and tag_date = '${vDate}' + interval 1 day
	    ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
            SET referrer = if(referrer is null or referrer = '', 'Direct', referrer)
            WHERE span = 'weekly'
                and time_flag = 'last'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_4b
        mysql --login-path=$dest_login_path -e "$sql_4b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly.error 


        export sql_4c="
            ##【頁面深度分析】廣告流量－廣告活動
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.campaign, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by campaign, domain
                    ) a
                    
                    left join
                    (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by campaign, domain
                    ) b
                    on a.domain = b.domain
                        and a.campaign = b.campaign
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.campaign, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign,
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by campaign
                    ) a
                    
                    left join
                    (
                    select 
                        campaign,
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by campaign
                    ) b
                    on a.campaign = b.campaign
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign weekly]
        echo $sql_4c
        mysql --login-path=$dest_login_path -e "$sql_4c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_weekly.error 


        export sql_4d="
            ##【頁面深度分析】廣告流量－廣告方式
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.source_medium, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by source_medium, domain
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by source_medium, domain
                    ) b
                    on a.domain = b.domain
                        and a.source_medium = b.source_medium
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.source_medium, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by source_medium
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by source_medium
                    ) b
                    on a.source_medium = b.source_medium
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_medium
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium weekly]
        echo $sql_4d
        mysql --login-path=$dest_login_path -e "$sql_4d" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_medium_weekly.error 


        export sql_4e="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    domain, 
                    count(distinct fpc) user,
                    null prop, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    'ALL' domain, 
                    count(distinct fpc) user,
                    100 prop,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;            

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain a
                INNER JOIN
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where domain = 'ALL'
                ) b
                ON a.tag_date = b.tag_date
                    and a.span = b.span
            SET a.prop = 100 * round(a.user / b.user, 2)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'weekly'
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain weekly]
        echo $sql_4e
        mysql --login-path=$dest_login_path -e "$sql_4e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_weekly.error 


        export sql_4f="
            ##【頁面深度分析】熱門頁面 - 頁面標題
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.page_title, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by page_title, domain
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by page_title, domain
                    ) b
                    on a.domain = b.domain
                        and a.page_title = b.page_title
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.page_title, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by page_title
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by page_title
                    ) b
                    on a.page_title = b.page_title
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;
            
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title weekly]
        echo $sql_4f
        mysql --login-path=$dest_login_path -e "$sql_4f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_weekly.error 

    else 
        echo [today is ${vDateName}, not Sunday. No Need to work on the weekly statistics.]
    fi
    
    

    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_5a="
            ##【頁面深度分析】登入網域 vs 瀏覽網域       
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,    
                    landing,
                    traffic_type, 
                    count(distinct fpc) user,
                    null prop,
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                group by landing, traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,    
                    'ALL' landing, 
                    traffic_type,
                    count(distinct fpc) user,
                    100 prop,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                group by traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,    
                    ifnull(landing, 'ALL') landing,
                    'ALL' traffic_type, 
                    count(distinct fpc) user,
                    null prop,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                group by landing
                    with rollup
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            # 計算個別網域的比例分配
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                INNER JOIN
                (
                select traffic_type, sum(user) user
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'monthly'
                    and landing <> 'ALL'
                group by traffic_type
                ) b
                ON a.traffic_type = b.traffic_type
            SET a.prop = ifnull(100 * round(a.user / b.user, 2), 0)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'monthly'
                and a.landing <> 'ALL'
            ;

            # UPDATE the monthly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing monthly]
        echo $sql_5a
        mysql --login-path=$dest_login_path -e "$sql_5a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_landing_monthly.error 


        export sql_5b="
            ##【頁面深度分析】導流分析－流量種類
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    a.traffic_type, 
                    'ALL' referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by traffic_type, domain
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by traffic_type, domain
                    ) b
                    on a.domain = b.domain
                        and a.traffic_type = b.traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    a.traffic_type, 
                    'ALL' referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by traffic_type
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by traffic_type
                    ) b
                    on a.traffic_type = b.traffic_type
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            ##【頁面深度分析】導流分析－自然流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Organic'
                    group by referrer, domain
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Organic'
                    group by referrer, domain
                    ) b
                    on a.domain = b.domain
                        and a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;


            ##【頁面深度分析】導流分析－廣告流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    'Ad' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by a.domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Ad'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1),
                        domain
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Ad'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1),
                        domain
                    ) b
                    on a.domain = b.domain
                        and a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    'Ad' traffic_type, 
                    a.referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Ad'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Ad'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            ##【頁面深度分析】導流分析－其它流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    'Others' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Others'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1),
                        domain
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Others'
                    group by 
                        substring_index(substring_index(referrer, '://', -1), '/', 1),
                        domain
                    ) b
                    on a.domain = b.domain
                        and a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    'Others' traffic_type, 
                    a.referrer, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                        and traffic_type = 'Others'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) a
                    
                    left join
                    (
                    select 
                        substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                        and traffic_type = 'Others'
                    group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;


            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'monthly'
                and referrer = 'ALL'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'monthly'
                and referrer = 'ALL'
                and tag_date = '${vDate}' + interval 1 day
            ;                        
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'monthly'
                and traffic_type = 'Organic'
                and referrer <> 'ALL'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'monthly'
                and traffic_type = 'Organic'
                and referrer <> 'ALL'
                and tag_date = '${vDate}' + interval 1 day
            ;      
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'monthly'
                and traffic_type = 'Ad'
                and referrer <> 'ALL'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'monthly'
                and traffic_type = 'Ad'
                and referrer <> 'ALL'
                and tag_date = '${vDate}' + interval 1 day
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = null
            WHERE span = 'monthly'
                and traffic_type = 'Others'
                and referrer <> 'ALL'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = 'last'
            WHERE span = 'monthly'
                and traffic_type = 'Others'
                and referrer <> 'ALL'
                and tag_date = '${vDate}' + interval 1 day
	    ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
            SET referrer = if(referrer is null or referrer = '', 'Direct', referrer)
            WHERE span = 'monthly'
                and time_flag = 'last'
            ;
            ;" 
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic monthly]
        echo $sql_5b
        mysql --login-path=$dest_login_path -e "$sql_5b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_monthly.error 


        export sql_5c="
            ##【頁面深度分析】廣告流量－廣告活動
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    a.campaign, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by campaign, domain
                    ) a
                    
                    left join
                    (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by campaign, domain
                    ) b
                    on a.domain = b.domain
                        and a.campaign = b.campaign
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    a.campaign, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by campaign
                    ) a
                    
                    left join
                    (
                    select 
                        campaign, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by campaign
                    ) b
                    on a.campaign = b.campaign
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign monthly]
        echo $sql_5c
        mysql --login-path=$dest_login_path -e "$sql_5c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_monthly.error 


        export sql_5d="
            ##【頁面深度分析】廣告流量－廣告方式
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    a.source_medium, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by source_medium, domain
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by source_medium, domain
                    ) b
                    on a.domain = b.domain
                        and a.source_medium = b.source_medium
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    a.source_medium, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by source_medium
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by source_medium
                    ) b
                    on a.source_medium = b.source_medium
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ;
            
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_medium
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"            
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium monthly]
        echo $sql_5d
        mysql --login-path=$dest_login_path -e "$sql_5d" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_medium_monthly.error 

        export sql_5e="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,      
                    domain, 
                    count(distinct fpc) user,
                    null prop, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                group by domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,    
                    'ALL' domain, 
                    count(distinct fpc) user,
                    100 prop, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain a
                INNER JOIN
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where domain = 'ALL'
                ) b
                ON a.tag_date = b.tag_date
                    and a.span = b.span
            SET a.prop = 100 * round(a.user / b.user, 2)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'monthly'
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain monthly]
        echo $sql_5e
        mysql --login-path=$dest_login_path -e "$sql_5e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_monthly.error 


        export sql_5f="
            ##【頁面深度分析】熱門頁面 - 頁面標題
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    a.page_title, 
                    a.domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by page_title, domain
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by page_title, domain
                    ) b
                    on a.domain = b.domain
                        and a.page_title = b.page_title
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;     
            
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,   
                    a.page_title, 
                    'ALL' domain, 
                    ifnull(valid, 0) valid,  
                    -1 * ifnull(invalid, 0) invalid,
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                    row_number () over (order by valid desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'valid'
                    group by page_title
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 1 month
                        and start_date < '${vDate}' + interval 1 day
                        and session_type = 'invalid'
                    group by page_title
                    ) b
                    on a.page_title = b.page_title
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;        
            
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title monthly]
        echo $sql_5f
        mysql --login-path=$dest_login_path -e "$sql_5f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_monthly.error 

    else 
        echo [today is ${vDate}, not ${vDate}. No Need to work on the monthly statistics.]
    fi
    
    
    
    for seasonDate in $seasonEnd
    do 
        if [ ${vDate} = ${seasonDate} ]; 
        then 
            export sql_6a="
                ##【頁面深度分析】登入網域 vs 瀏覽網域       
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        landing,
                        traffic_type, 
                        count(distinct fpc) user,
                        null prop,
                    	null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                    group by landing, traffic_type
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        'ALL' landing,
                        traffic_type, 
                        count(distinct fpc) user,
                        100 prop,
                        null time_flag,  
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                    group by traffic_type
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        ifnull(landing, 'ALL') landing,
                        'ALL' traffic_type, 
                        count(distinct fpc) user,
                        null prop,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                    group by landing
                        with rollup
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ;

                # 計算個別網域的比例分配
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                    INNER JOIN
                    (
                    select traffic_type, sum(user) user
                    from ${project_name}.${type}_${table_name}_${org_id}_landing
                    where tag_date = '${vDate}' + interval 1 day
                        and span = 'seasonal'
                        and landing <> 'ALL'
                    group by traffic_type
                    ) b
                    ON a.traffic_type = b.traffic_type
                SET a.prop = ifnull(100 * round(a.user / b.user, 2), 0)
                WHERE a.tag_date = '${vDate}' + interval 1 day
                    and a.span = 'seasonal'
                    and a.landing <> 'ALL'
                ;

                # UPDATE the seasonal time_flag
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing seasonal]
            echo $sql_6a
            mysql --login-path=$dest_login_path -e "$sql_6a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_landing_seasonal.error 
                        
                        
            export sql_6b="
                ##【頁面深度分析】導流分析－流量種類
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,    
                        a.traffic_type, 
                        'ALL' referrer, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by a.domain order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            traffic_type, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by traffic_type, domain
                        ) a
                        
                        left join
                        (
                        select 
                            traffic_type, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by traffic_type, domain
                        ) b
                        on a.domain = b.domain
                            and a.traffic_type = b.traffic_type
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,    
                        a.traffic_type, 
                        'ALL' referrer, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            traffic_type, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by traffic_type
                        ) a
                        
                        left join
                        (
                        select 
                            traffic_type, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by traffic_type
                        ) b
                        on a.traffic_type = b.traffic_type
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;
    
                ##【頁面深度分析】導流分析－自然流量
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        'Organic' traffic_type, 
                        a.referrer, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by domain order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            referrer, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                            and traffic_type = 'Organic'
                        group by referrer, domain
                        ) a
                        
                        left join
                        (
                        select 
                            referrer, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                            and traffic_type = 'Organic'
                        group by referrer, domain
                        ) b
                        on a.domain = b.domain
                            and a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        'Organic' traffic_type, 
                        a.referrer, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            referrer, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                            and traffic_type = 'Organic'
                        group by referrer
                        ) a
                        
                        left join
                        (
                        select 
                            referrer, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                            and traffic_type = 'Organic'
                        group by referrer
                        ) b
                        on a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;
    
                ##【頁面深度分析】導流分析－廣告流量
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        'Ad' traffic_type, 
                        a.referrer, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by domain order by valid desc) ranking,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                            and traffic_type = 'Ad'
                        group by 
                            substring_index(substring_index(referrer, '://', -1), '/', 1), 
                            domain
                        ) a
                        
                        left join
                        (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                            and traffic_type = 'Ad'
                        group by 
                            substring_index(substring_index(referrer, '://', -1), '/', 1),
                            domain
                        ) b
                        on a.domain = b.domain
                            and a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        'Ad' traffic_type, 
                        a.referrer, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                            and traffic_type = 'Ad'
                        group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                        ) a
                        
                        left join
                        (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                            and traffic_type = 'Ad'
                        group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                        ) b
                        on a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;

                ##【頁面深度分析】導流分析－其它流量
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,    
                        'Others' traffic_type, 
                        a.referrer, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by domain order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                            and traffic_type = 'Others'
                        group by 
                            substring_index(substring_index(referrer, '://', -1), '/', 1), 
                            domain
                        ) a
                        
                        left join
                        (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                            and traffic_type = 'Others'
                        group by 
                            substring_index(substring_index(referrer, '://', -1), '/', 1), 
                            domain
                        ) b
                        on a.domain = b.domain
                            and a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,    
                        'Others' traffic_type, 
                        a.referrer, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                            and traffic_type = 'Others'
                        group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                        ) a
                        
                        left join
                        (
                        select 
                            substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                            and traffic_type = 'Others'
                        group by substring_index(substring_index(referrer, '://', -1), '/', 1)
                        ) b
                        on a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;


                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = null
                WHERE span = 'seasonal'
                    and referrer = 'ALL'
                ; 
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = 'last'
                WHERE span = 'seasonal'
                    and referrer = 'ALL'
                    and tag_date = '${vDate}' + interval 1 day
                ;                        
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = null
                WHERE span = 'seasonal'
                    and traffic_type = 'Organic'
                    and referrer <> 'ALL'
                ; 
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = 'last'
                WHERE span = 'seasonal'
                    and traffic_type = 'Organic'
                    and referrer <> 'ALL'
                    and tag_date = '${vDate}' + interval 1 day
                ;            
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = null
                WHERE span = 'seasonal'
                    and traffic_type = 'Ad'
                    and referrer <> 'ALL'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = 'last'
                WHERE span = 'seasonal'
                    and traffic_type = 'Ad'
                    and referrer <> 'ALL'
                    and tag_date = '${vDate}' + interval 1 day
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = null
                WHERE span = 'seasonal'
                    and traffic_type = 'Others'
                    and referrer <> 'ALL'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = 'last'
                WHERE span = 'seasonal'
                    and traffic_type = 'Others'
                    and referrer <> 'ALL'
                    and tag_date = '${vDate}' + interval 1 day
		;

                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
                SET referrer = if(referrer is null or referrer = '', 'Direct', referrer)
                WHERE span = 'seasonal'
                    and time_flag = 'last'
                ;"    
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic seasonal]
            echo $sql_6b
            mysql --login-path=$dest_login_path -e "$sql_6b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_seasonal.error 
                        
                        
            export sql_6c="
                ##【頁面深度分析】廣告流量－廣告活動
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                    select  
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        a.campaign, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by domain order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            campaign, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by campaign, domain
                        ) a
                        
                        left join
                        (
                        select 
                            campaign, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by campaign, domain
                        ) b
                        on a.domain = b.domain
                            and a.campaign = b.campaign
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                    select  
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        a.campaign, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            campaign, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by campaign
                        ) a
                        
                        left join
                        (
                        select 
                            campaign, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by campaign
                        ) b
                        on a.campaign = b.campaign
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
                ;
                
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign seasonal]
            echo $sql_6c
            mysql --login-path=$dest_login_path -e "$sql_6c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_seasonal.error 
                        
                        
            export sql_6d="
                ##【頁面深度分析】廣告流量－廣告方式
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        a.source_medium, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by domain order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            source_medium, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by source_medium, domain
                        ) a
                        
                        left join
                        (
                        select 
                            source_medium, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by source_medium, domain
                        ) b
                        on a.domain = b.domain
                            and a.source_medium = b.source_medium
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        a.source_medium, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            source_medium, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by source_medium
                        ) a
                        
                        left join
                        (
                        select 
                            source_medium, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by source_medium
                        ) b
                        on a.source_medium = b.source_medium
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
                ;
                
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_medium
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium seasonal]
            echo $sql_6d
            mysql --login-path=$dest_login_path -e "$sql_6d" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_medium_seasonal.error 
 
 
            export sql_6e="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,     
                        domain, 
                        count(distinct fpc) user,
                        null prop,
                        null time_flag,  
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                    group by domain
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,   
                        'ALL' domain, 
                        count(distinct fpc) user,
                        100 prop, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
                ;

                UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain a
                    INNER JOIN
                    (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id}_domain
                    where domain = 'ALL'
                    ) b
                    ON a.tag_date = b.tag_date
                        and a.span = b.span
                SET a.prop = 100 * round(a.user / b.user, 2)
                WHERE a.tag_date = '${vDate}' + interval 1 day
                    and a.span = 'seasonal'
                ;
                
                # UPDATE the seasonal time_flag
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain seasonal]
            echo $sql_6e
            mysql --login-path=$dest_login_path -e "$sql_6e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_seasonal.error 


            export sql_6f="
                ##【頁面深度分析】熱門頁面 - 頁面標題
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        a.page_title, 
                        a.domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (partition by domain order by valid desc) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            page_title, 
                            domain, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by page_title, domain
                        ) a
                        
                        left join
                        (
                        select 
                            page_title, 
                            domain, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by page_title, domain
                        ) b
                        on a.domain = b.domain
                            and a.page_title = b.page_title
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        a.page_title, 
                        'ALL' domain, 
                        ifnull(valid, 0) valid,  
                        -1 * ifnull(invalid, 0) invalid,
                        100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                        100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                        row_number () over (order by valid desc) ranking,
                        null time_flag,  
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            page_title, 
                            count(distinct fpc) valid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'valid'
                        group by page_title
                        ) a
                        
                        left join
                        (
                        select 
                            page_title, 
                            count(distinct fpc) invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and session_type = 'invalid'
                        group by page_title
                        ) b
                        on a.page_title = b.page_title
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
                ;

                UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"         
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title seasonal]
            echo $sql_6f
            mysql --login-path=$dest_login_path -e "$sql_6f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_seasonal.error 


            export sql_7="
                DELETE 
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain_fpc AUTO_INCREMENT = 1
                ;  

                DELETE 
                from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                where start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc AUTO_INCREMENT = 1
                ;  

                DELETE 
                from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                where start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc AUTO_INCREMENT = 1
                ;

                DELETE 
                from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                where start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title_fpc AUTO_INCREMENT = 1
                ;"
            echo ''
            echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id}_anything and keep the last 7 days]
            echo $sql_7
            mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_anything_delete.error 
                
        else 
            echo [The current date is ${vDate}. The seasonal statisitcs date is ${seasonDate}.]
        fi
    done
    

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id_4.txt
echo ''
echo [end the ${vDate} data at `date`]
