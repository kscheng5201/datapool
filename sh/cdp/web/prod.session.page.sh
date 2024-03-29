#!/usr/bin/bash
################
# Note: 
# 2022-05-13: _traffic 的日表增加 [and behavior = 'page_view'] 條件，
#             避免事件的資料納入計算，不讓 traffic_type = 'NULL' 出現。
###############

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type="session"
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_landing (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            landing varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '每個 session 開始時的進入 domain', 
            traffic_type varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Ad/Direct/Organic/Others',
            freq int NOT NULL DEFAULT 0 COMMENT 'frequency of session(工作階段)',
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
            key idx_traffic_type (traffic_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】-導流分析／廣告流量-登入網域'
        ; 
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_domain (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            freq int NOT NULL DEFAULT 0 COMMENT 'frequency of session(工作階段)',
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_traffic (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_campaign (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_campaign廣告活動; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
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
       CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_medium (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            source_medium varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_medium廣告來源 x utm_medium廣告媒介; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告活動-廣告來源x廣告媒介'
        ;               
       CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_title (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告活動-熱門頁面'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 

    export sql_2a="  
        ##【頁面深度分析】- 導流分析／廣告流量 - 登入網域 
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ifnull(domain, 'ALL') landing, 
                ifnull(traffic_type, 'ALL') traffic_type,
                count(*) freq, 
                null prop,
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select domain, traffic_type, row_number () over (partition by accu_id, session order by created_at) rid
                from ${project_name}.${type}_both_${org_id}_etl
                ) a
            where rid = 1
            group by domain, traffic_type
                with rollup
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ;
        
        # 計算個別網域的比例分配
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
            INNER JOIN
            (
            select *
            from ${project_name}.${type}_${table_name}_${org_id}_landing
            where landing = 'ALL'
            ) b
            ON a.tag_date = b.tag_date
                and a.span = b.span
                and a.traffic_type = b.traffic_type
        SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
        WHERE a.tag_date = '${vDate}' + interval 1 day
            and a.span = 'daily'
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing]
    echo $sql_2a
    mysql --login-path=$dest_login_path -e "$sql_2a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_landing_daily.error 

    export sql_2b="
        ##【頁面深度分析】導流分析 - 流量種類 - 流量種類 分向長條圖 (referer = ALL)
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                a.traffic_type, 
                'ALL' referrer, 
                a.domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by a.domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select domain, traffic_type, count(distinct accu_id, session) valid
                from (
                    select domain, traffic_type, accu_id, session, row_number () over (partition by domain, traffic_type, accu_id, session order by created_at) rid
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'valid'
                        and behavior = 'page_view'
                    ) a
                where rid = 1
                group by domain, traffic_type
                ) a
            
                left join
                (
                select domain, traffic_type, count(distinct accu_id, session) invalid
                from (
                    select domain, traffic_type, accu_id, session, row_number () over (partition by domain, traffic_type, accu_id, session order by created_at) rid
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'invalid'
                        and behavior = 'page_view'
                    ) a
                where rid = 1
                group by domain, traffic_type
                ) b
                on a.domain = b.domain
                    and a.traffic_type = b.traffic_type
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】導流分析 - 流量種類 - 流量種類 分向長條圖 (referer <> ALL)
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                a.traffic_type, 
                if(a.referrer = 'NULL', '', substring_index(substring_index(a.referrer, '://', -1), '/', 1)) referrer, 
                a.domain,
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by a.domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select domain, traffic_type, referrer, count(distinct accu_id, session) valid
                from (
                    select domain, traffic_type, referrer, accu_id, session, row_number () over (partition by domain, traffic_type, accu_id, session order by created_at) rid
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'valid'
                        and behavior = 'page_view'
                    ) a
                where rid = 1
                group by domain, traffic_type, referrer
                ) a
            
                left join
                (
                select domain, traffic_type, referrer, count(distinct accu_id, session) invalid
                from (
                    select domain, traffic_type, referrer, accu_id, session, row_number () over (partition by domain, traffic_type, accu_id, session order by created_at) rid
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'invalid'
                        and behavior = 'page_view'
                    ) a
                where rid = 1
                group by domain, traffic_type, referrer
                ) b
                on a.domain = b.domain
                    and a.traffic_type = b.traffic_type
                    and a.referrer = b.referrer
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;
        
        ##【頁面深度分析】導流分析 - 流量種類 - 流量種類 分向長條圖 (domain = ALL)
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                a.traffic_type, 
                if(a.referrer = 'NULL', '', substring_index(substring_index(a.referrer, '://', -1), '/', 1)) referrer, 
                'ALL' domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select traffic_type, referrer, count(distinct accu_id, session) valid
                from (
                    select traffic_type, referrer, accu_id, session, row_number () over (partition by traffic_type, accu_id, session order by created_at) rid
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'valid'
                        and behavior = 'page_view'
                    ) a
                where rid = 1
                group by traffic_type, referrer
                ) a
            
                left join
                (
                select traffic_type, referrer, count(distinct accu_id, session) invalid
                from (
                    select traffic_type, referrer, accu_id, session, row_number () over (partition by traffic_type, accu_id, session order by created_at) rid
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'invalid'
                        and behavior = 'page_view'
                    ) a
                where rid = 1
                group by traffic_type, referrer
                ) b
                on a.traffic_type = b.traffic_type
                    and a.referrer = b.referrer
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic]
    echo $sql_2b
    mysql --login-path=$dest_login_path -e "$sql_2b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_daily.error 


    export sql_2c="
        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告活動 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                if(a.campaign = 'NULL', '', a.campaign) campaign, 
                a.domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by a.domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select domain, campaign, count(distinct accu_id, session) valid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'valid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and campaign is not null
                    and campaign <> 'NULL'
                    and campaign <> ''
                group by domain, campaign
                ) a
            
                left join
                (
                select domain, campaign, count(distinct accu_id, session) invalid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'invalid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and campaign is not null
                    and campaign <> 'NULL'
                    and campaign <> ''
                group by domain, campaign
                ) b
                on a.domain = b.domain
                    and a.campaign = b.campaign
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告活動 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                if(a.campaign = 'NULL', '', a.campaign) campaign, 
                'ALL' domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select campaign, count(distinct accu_id, session) valid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'valid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and campaign is not null
                    and campaign <> 'NULL'
                    and campaign <> ''
                group by campaign
                ) a
            
                left join
                (
                select campaign, count(distinct accu_id, session) invalid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'invalid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and campaign is not null
                    and campaign <> 'NULL'
                    and campaign <> ''
                group by campaign
                ) b
                on a.campaign = b.campaign
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign]
    echo $sql_2c
    mysql --login-path=$dest_login_path -e "$sql_2c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_daily.error 


    export sql_2d="
        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告來源x廣告媒介 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                if(a.source_medium = 'NULL', '', a.source_medium) source_medium, 
                a.domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by a.domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select domain, source_medium, count(distinct accu_id, session) valid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'valid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and source_medium is not null
                    and source_medium <> 'NULL'
                    and source_medium <> ''
                group by domain, source_medium
                ) a
            
                left join
                (
                select domain, source_medium, count(distinct accu_id, session) invalid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'invalid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and source_medium is not null
                    and source_medium <> 'NULL'
                    and source_medium <> ''
                group by domain, source_medium
                ) b
                on a.domain = b.domain
                    and a.source_medium = b.source_medium
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告來源x廣告媒介 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                if(a.source_medium = 'NULL', '', a.source_medium) source_medium, 
                'ALL' domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select source_medium, count(distinct accu_id, session) valid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'valid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and source_medium is not null
                    and source_medium <> 'NULL'
                    and source_medium <> ''
                group by source_medium
                ) a
            
                left join
                (
                select source_medium, count(distinct accu_id, session) invalid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'invalid'
                    and behavior = 'page_view'
                    and traffic_type = 'Ad'
                    and source_medium is not null
                    and source_medium <> 'NULL'
                    and source_medium <> ''
                group by source_medium
                ) b
                on a.source_medium = b.source_medium
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium]
    echo $sql_2d
    mysql --login-path=$dest_login_path -e "$sql_2d" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_medium_daily.error 

    export sql_2e="
        ##【頁面深度分析】熱門頁面 - 頁面瀏覽 圓餅圖 
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ifnull(domain, 'ALL') domain, 
                count(*) freq,
                null prop,  
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    accu_id, 
                    domain,
                    session, 
                    count(*)
                from ${project_name}.${type}_both_${org_id}_etl
                where behavior = 'page_view'
                group by 
                    accu_id, 
                    domain,
                    session
                ) a
            group by domain with rollup
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
        SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
        WHERE a.tag_date = '${vDate}' + interval 1 day
            and a.span = 'daily'
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain]
    echo $sql_2e
    mysql --login-path=$dest_login_path -e "$sql_2e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_daily.error 


    export sql_2f="
        ##【頁面深度分析】熱門頁面 - 頁面標題 - 頁面標題 長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                a.page_title, 
                a.domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (partition by a.domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select domain, page_title, count(distinct accu_id, session) valid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'valid'
                    and page_title is not null
                    and page_title <> 'NULL'
                    and page_title <> ''
                group by domain, page_title
                ) a
            
                left join
                (
                select domain, page_title, count(distinct accu_id, session) invalid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'invalid'
                    and page_title is not null
                    and page_title <> 'NULL'
                    and page_title <> ''
                group by domain, page_title
                ) b
                on a.domain = b.domain
                    and a.page_title = b.page_title
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告活動 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                a.page_title, 
                'ALL' domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio,
                row_number () over (order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select page_title, count(distinct accu_id, session) valid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'valid'
                    and page_title is not null
                    and page_title <> 'NULL'
                    and page_title <> ''
                group by page_title
                ) a
            
                left join
                (
                select page_title, count(distinct accu_id, session) invalid
                from ${project_name}.${type}_both_${org_id}_etl
                where session_type = 'invalid'
                    and page_title is not null
                    and page_title <> 'NULL'
                    and page_title <> ''
                group by page_title
                ) b
                on a.page_title = b.page_title
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title]
    echo $sql_2f
    mysql --login-path=$dest_login_path -e "$sql_2f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_daily.error 


    export sql_3a="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing 
            (tag_date, span, start_date, end_date, landing, traffic_type, freq, prop)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'Organic', 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.landing = b.domain
            where a.landing is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ; 

        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing 
            (tag_date, span, start_date, end_date, landing, traffic_type, freq, prop)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'Ad', 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.landing = b.domain
            where a.landing is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ; 

        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing 
            (tag_date, span, start_date, end_date, landing, traffic_type, freq, prop)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'Others', 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.landing = b.domain
            where a.landing is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ; 

        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing 
            (tag_date, span, start_date, end_date, landing, traffic_type, freq, prop)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'ALL', 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.landing = b.domain
            where a.landing is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing]
    echo $sql_3a
    mysql --login-path=$dest_login_path -e "$sql_3a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_landing_daily.error 

    export sql_3b="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            (tag_date, span, start_date, end_date, traffic_type, referrer, domain, valid, invalid, valid_ratio, invalid_ratio)
            
            select 
                '${vDate}' + interval 1 day, 
                'daily', 
                '${vDate}', 
                '${vDate}', 
                'Organic' traffic_type, 
                'ALL' referrer, 
                b.domain, 
                0 valid,
                0 invalid, 
                0 valid_ratio, 
                0 invalid_ratio
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'    
                    and referrer = 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;  
        
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            (tag_date, span, start_date, end_date, traffic_type, referrer, domain, valid, invalid, valid_ratio, invalid_ratio)
            
            select 
                '${vDate}' + interval 1 day, 
                'daily', 
                '${vDate}', 
                '${vDate}', 
                'Ad' traffic_type, 
                'ALL' referrer, 
                b.domain, 
                0 valid,
                0 invalid, 
                0 valid_ratio, 
                0 invalid_ratio
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'    
                    and referrer = 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;  
        
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            (tag_date, span, start_date, end_date, traffic_type, referrer, domain, valid, invalid, valid_ratio, invalid_ratio)
            
            select 
                '${vDate}' + interval 1 day, 
                'daily', 
                '${vDate}', 
                '${vDate}', 
                'Others' traffic_type, 
                'ALL' referrer, 
                b.domain, 
                0 valid,
                0 invalid, 
                0 valid_ratio, 
                0 invalid_ratio
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'    
                    and referrer = 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_traffic]
    echo $sql_3b
    mysql --login-path=$dest_login_path -e "$sql_3b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_daily.error 

    export sql_3c="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
            (tag_date, span, start_date, end_date, domain, campaign, valid, invalid, valid_ratio, invalid_ratio)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'No Campaign', 0, 0, 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_campaign
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_campaign]
    echo $sql_3c
    mysql --login-path=$dest_login_path -e "$sql_3c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_daily.error 

    export sql_3d="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_medium
            (tag_date, span, start_date, end_date, domain, source_medium, valid, invalid, valid_ratio, invalid_ratio)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'No source x medium', 0, 0, 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_medium
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_medium]
    echo $sql_3d
    mysql --login-path=$dest_login_path -e "$sql_3d" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_medium_daily.error 


    export sql_3e="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_domain
            (tag_date, span, start_date, end_date, domain, freq, prop)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 0, 0
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
        ;" 
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_domain]
    echo $sql_3e
    mysql --login-path=$dest_login_path -e "$sql_3e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_daily.error 


    export sql_3f="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_title
            (tag_date, span, start_date, end_date, domain, page_title)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain, 'No Title'
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_title
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> 'ALL'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = '${project_name}'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
    	;"    
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_title]
    echo $sql_3f
    mysql --login-path=$dest_login_path -e "$sql_3f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_daily.error 



    if [ ${vDateName} = Sun ]; 
    then 
        export sql_4a="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    ifnull(landing, 'ALL') landing,
                    ifnull(traffic_type, 'ALL') traffic_type,
                    sum(freq) freq,
                    null prop, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> 'ALL'
                    and traffic_type <> 'ALL'
                    and traffic_type is not null
                    and traffic_type <> ''
                group by 
                    landing, 
                    traffic_type
                        with rollup
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;
            
            # 計算個別網域的比例分配
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                INNER JOIN
                (
                select traffic_type, sum(freq) freq
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'weekly'
                    and landing <> 'ALL'
                group by traffic_type
                ) b
                ON a.traffic_type = b.traffic_type
            SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
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


        export sql_4b1="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    traffic_type,
                    referrer, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,          
                    row_number () over (partition by domain, traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    #and traffic_type <> 'Others'
                    #and referrer <> 'ALL'
                    and span = 'daily'
                group by 
                    traffic_type,
                    referrer, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;" 
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_4b1
        mysql --login-path=$dest_login_path -e "$sql_4b1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_4b1.error 
        
        export sql_4b2="        
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    traffic_type,
                    referrer, 
                    'ALL' domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,          
                    row_number () over (partition by traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'weekly'
                    and referrer = 'ALL'
                group by 
                    traffic_type,
                    referrer
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_4b2
        mysql --login-path=$dest_login_path -e "$sql_4b2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_4b2.error 

        export sql_4b3="
            # UPDATE the weekly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
            SET referrer = if(referrer is null or referrer = '', 'Direct', referrer)
            WHERE span = 'weekly'
                and time_flag = 'last'
            ;"
        echo ''
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_4b3
        mysql --login-path=$dest_login_path -e "$sql_4b3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_4b3.error 


        export sql_4c="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    campaign, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,     
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_campaign
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and span = 'daily'
                group by 
                    campaign, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ; 
            
            # UPDATE the weekly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign weekly]
        echo $sql_4c
        mysql --login-path=$dest_login_path -e "$sql_4c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_weekly.error 


        export sql_4d="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,
                    source_medium, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,  
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_medium
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and span = 'daily'
                group by 
                    source_medium, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ; 

            # UPDATE the weekly time_flag
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
                    sum(freq) freq,
                    null prop,  
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and span = 'daily'
                group by domain
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
            SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'weekly'
            ;        

            # UPDATE the weekly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain weekly]
        echo $sql_4e
        mysql --login-path=$dest_login_path -e "$sql_4e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_weekly.error 


        export sql_4f="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    page_title, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,    
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_title
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and span = 'daily'
                group by 
                    page_title, 
                    domain
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ; 

            # UPDATE the weekly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title weekly]
        echo $sql_4f
        mysql --login-path=$dest_login_path -e "$sql_4f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_weekly.error 

    else 
        echo [today is ${vDateName}, not Sunday. No Need to do the weekly statistics.]
    fi
    
    

    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_5a="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    ifnull(landing, 'ALL') landing,
                    ifnull(traffic_type, 'ALL') traffic_type,
                    sum(freq) freq,
                    null prop, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> 'ALL'
                    and traffic_type <> 'ALL'
                    and traffic_type is not null
                    and traffic_type <> ''
                group by 
                    landing, 
                    traffic_type
                        with rollup
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;
            
            # 計算個別網域的比例分配
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                INNER JOIN
                (
                select traffic_type, sum(freq) freq
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'monthly'
                    and landing <> 'ALL'
                group by traffic_type
                ) b
                ON a.traffic_type = b.traffic_type
            SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
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


        export sql_5b1="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date,  
                    traffic_type,
                    referrer, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,          
                    row_number () over (partition by domain, traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                    and valid >= 1
                group by 
                    traffic_type,
                    referrer, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;" 
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_5b1
        mysql --login-path=$dest_login_path -e "$sql_5b1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_5b1.error 
        
        export sql_5b2="        
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date, 
                    traffic_type,
                    referrer, 
                    'ALL' domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,          
                    row_number () over (partition by traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'monthly'
                    and referrer = 'ALL'
                group by 
                    traffic_type,
                    referrer
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_5b2
        mysql --login-path=$dest_login_path -e "$sql_5b2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_5b2.error 

        export sql_5b3="
            # UPDATE the weekly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ; 
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
            SET referrer = if(referrer is null or referrer = '', 'Direct', referrer)
            WHERE span = 'monthly'
                and time_flag = 'last'
            ;"
        echo ''
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
        echo $sql_5b3
        mysql --login-path=$dest_login_path -e "$sql_5b3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_5b3.error 


        export sql_5c="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date, 
                    campaign, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,    
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_campaign
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                group by 
                    campaign, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ; 

            # UPDATE the monthly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign monthly]
        echo $sql_5c
        mysql --login-path=$dest_login_path -e "$sql_5c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_monthly.error 


        export sql_5d="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial,
                    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date, 
                    source_medium, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_medium
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                group by 
                    source_medium, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ; 

            # UPDATE the monthly time_flag
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
                    sum(freq) freq,
                    null prop,
                    null time_flag,   
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                group by domain
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
            SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'monthly'
            ;

            # UPDATE the monthly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain monthly]
        echo $sql_5e
        mysql --login-path=$dest_login_path -e "$sql_5e" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_monthly.error 


        export sql_5f="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial,
                    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vDate}' + interval 1 day - interval 1 month start_date, 
                    '${vDate}' end_date, 
                    page_title, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                    ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,  
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_title
                where start_date >= '${vDate}' + interval 1 day - interval 1 month
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                group by 
                    page_title, 
                    domain
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;

            # UPDATE the monthly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title monthly]
        echo $sql_5f
        mysql --login-path=$dest_login_path -e "$sql_5f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_monthly.error 

    else 
        echo [today is ${vDate}, not ${vDate}. No Need to do the monthly statistics.]
    fi
    
    
    
    for seasonDate in $seasonEnd
    do 
        if [ ${vDate} = ${seasonDate} ]; 
        then 
            export sql_6a="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        ifnull(landing, 'ALL') landing,
                        ifnull(traffic_type, 'ALL') traffic_type,
                        sum(freq) freq,
                        null prop, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_landing
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                        and landing <> 'ALL'
                        and traffic_type <> 'ALL'
                        and traffic_type is not null
                        and traffic_type <> ''
                    group by 
                        landing, 
                        traffic_type
                            with rollup
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ;
                
    
                # 計算個別網域的比例分配
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                    INNER JOIN
                    (
                    select traffic_type, sum(freq) freq
                    from ${project_name}.${type}_${table_name}_${org_id}_landing
                    where tag_date = '${vDate}' + interval 1 day
                        and span = 'seasonal'
                        and landing <> 'ALL'
                    group by traffic_type
                    ) b
                    ON a.traffic_type = b.traffic_type
                SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
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


            export sql_6b1="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        traffic_type,
                        referrer, 
                        domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                        ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,          
                        row_number () over (partition by domain, traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                        and valid >= 1
                    group by 
                        traffic_type,
                        referrer, 
                        domain
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;" 
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
            echo $sql_6b1
            mysql --login-path=$dest_login_path -e "$sql_6b1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_6b1.error 
            
            export sql_6b2="        
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        traffic_type,
                        referrer, 
                        'ALL' domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                        ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,          
                        row_number () over (partition by traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic
                    where tag_date = '${vDate}' + interval 1 day
                        and span = 'seasonal'
                        and referrer = 'ALL'
                    group by 
                        traffic_type,
                        referrer
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
            echo $sql_6b2
            mysql --login-path=$dest_login_path -e "$sql_6b2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_6b2.error 
    
            export sql_6b3="
                # UPDATE the weekly time_flag
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ; 
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic  
                SET referrer = if(referrer is null or referrer = '', 'Direct', referrer)
                WHERE span = 'seasonal'
                    and time_flag = 'last'
                ;"
            echo ''
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic weekly]
            echo $sql_6b3
            mysql --login-path=$dest_login_path -e "$sql_6b3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_traffic_weekly_sql_6b3.error 


            export sql_6c="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        campaign, 
                        domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                        ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,  
                        row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by 
                        campaign, 
                        domain
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
                ; 
                # UPDATE the seasonal time_flag
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"      
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign seasonal]
            echo $sql_6c
            mysql --login-path=$dest_login_path -e "$sql_6c" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_campaign_seasonal.error 
                        
                        
            export sql_6d="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        source_medium, 
                        domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                        ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,  
                        row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_medium
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by 
                        source_medium, 
                        domain
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
                ; 

                # UPDATE the seasonal time_flag
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
                        sum(freq) freq, 
                        null prop, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by domain
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
                SET a.prop = ifnull(100 * round(a.freq / b.freq, 2), 0)
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
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        page_title, 
                        domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        ifnull(100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2), 0) valid_ratio, 
                        ifnull(100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)), 0) invalid_ratio,  
                        row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_title
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by 
                        page_title, 
                        domain
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
                ;

                # UPDATE the seasonal time_flag
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title seasonal]
            echo $sql_6f
            mysql --login-path=$dest_login_path -e "$sql_6f" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_title_seasonal.error 

        else 
            echo [The current date is ${vDate}. The seasonal statisitcs date is ${seasonDate}.]
        fi
    done
    

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt
echo ''
echo [end the ${vDate} data at `date`]
