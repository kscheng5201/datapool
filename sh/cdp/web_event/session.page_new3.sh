#!/usr/bin/bash
export dest_login_path="datapool"
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
"

################################################################################
echo ''
echo [start the job session.page_new.sh on ${vDate} at `date`]

#### loop by org_id ####
export sql_0="
    select org_id
    from cdp_organization.organization_domain
    where domain_type = 'web'
    group by org_id
    limit 2, 2
    ;"    
echo ''
echo [Get the org_id]
#mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.error
#sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt


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
            freq int NOT NULL DEFAULT 0 COMMENT 'frequency of session(工作階段)',
	    prop int DEFAULT NULL COMMENT '此 domain 佔所有 domain 的%',
	    time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, landing),
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
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_traffic (   
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
            valid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, span, traffic_type, referrer),
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
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_campaign廣告活動; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, span, campaign),
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
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            source_medium varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_medium廣告來源 x utm_medium廣告媒介; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, span, source_medium),
            key idx_created_at (created_at),
            key idx_tag_date (tag_date),   
            key idx_start_date (start_date),  
            key idx_domain (domain), 
            key idx_span (span),
            key idx_medium_medium (source_medium),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告活動-廣告來源x廣告媒介'
        ;               
       create table if not exists ${project_name}.${type}_${table_name}_${org_id}_title (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            page_title varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int NOT NULL DEFAULT 0 COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, span, page_title),
            key idx_created_at (created_at), 
            key idx_tag_date (tag_date),  
            key idx_start_date (start_date),  
            key idx_domain (domain), 
            key idx_span (span),
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告活動-廣告來源x廣告媒介'
        ;"
    # echo $sql_1
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 

    export sql_2="  
        ##【頁面深度分析】- 導流分析／廣告流量 - 登入網域 
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ifnull(domain, '#') landing, 
                count(*) freq, 
		null prop,
		null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select *, row_number () over (partition by fpc, session order by created_at) rid
                from ${project_name}.${type}_both_${org_id}_etl
                where behavior = 'page_view'
                ) a
            where rid = 1
            group by domain 
                with rollup
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ;

        UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
            INNER JOIN
            (
            select *
            from ${project_name}.${type}_${table_name}_${org_id}_landing
            where landing = '#'
            ) b
            ON a.tag_date = b.tag_date
                and a.span = b.span
        SET a.prop = 100 * round(a.freq / b.freq, 2)
        WHERE a.tag_date = '${vDate}' + interval 1 day
            and a.span = 'daily'
	;


        ##【頁面深度分析】導流分析 - 流量種類 - 流量種類 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                traffic_type, 
                referrer, 
                domain, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.traffic_type, 
                    '#' referrer, 
                    ifnull(b.domain, '#') domain, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio 
                from (
                    select 
                        traffic_type, 
                        domain, 
                        count(*) valid
                    from (
                        select 
                            fpc, 
                            traffic_type,
                            session, 
                            domain, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        where behavior = 'page_view'
                        group by 
                            fpc, 
                            traffic_type,
                            session, 
                            domain
                        having count(*) >= 2
                        ) a
                        
                    group by domain, traffic_type
                    ) b
                
                    left join
                    (
                    select 
                        traffic_type, 
                        domain, 
                        count(*) invalid
                    from (
                        select 
                            fpc, 
                            traffic_type,
                            session, 
                            domain, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        where behavior = 'page_view'
                        group by 
                            fpc, 
                            traffic_type,
                            session, 
                            domain
                        having count(*) = 1
                        ) c
                        
                    group by domain, traffic_type
                    ) d
                    
                    on b.traffic_type = d.traffic_type
                        and b.domain = d.domain          
                group by 
                    b.traffic_type, 
                    b.domain
                        with rollup
                having b.traffic_type is not null
                ) e
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;
        
        ##【頁面深度分析】導流分析 - 自然流量 - 搜尋引擎 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                'Organic' traffic_type, 
                referrer, 
                domain, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.referrer, 
                    ifnull(b.domain, '#') domain, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio             
                from (
                    select 
                        referrer, 
                        domain, 
                        count(*) valid
                    from (
                        select 
                            fpc, 
                            referrer,
                            session, 
                            domain, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        where traffic_type = 'Organic'
                            and behavior = 'page_view'
                        group by 
                            fpc, 
                            referrer,
                            session, 
                            domain
                        having count(*) >= 2
                        ) a
                        
                    group by domain, referrer
                    ) b
                
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(*) invalid
                    from (
                        select 
                            fpc, 
                            referrer,
                            session, 
                            domain, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        where traffic_type = 'Organic'
                            and behavior = 'page_view'
                        group by 
                            fpc, 
                            referrer,
                            session, 
                            domain
                        having count(*) = 1
                        ) a
                        
                    group by domain, referrer
                    ) d
                    
                    on b.referrer = d.referrer
                        and b.domain = d.domain   
                group by 
                    b.referrer, 
                    b.domain
                        with rollup
                having b.referrer is not null
                ) e
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;
        
        ##【頁面深度分析】導流分析 - 其他流量 - 第三方網站 分向長條圖   
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                'Others' traffic_type, 
                referrer, 
                domain, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by domain order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.referrer, 
                    ifnull(b.domain, '#') domain, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio             
                from (
                    select 
                        referrer, 
                        domain, 
                        count(*) valid
                    from (
                        select 
                            fpc, 
                            referrer,
                            session, 
                            domain, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        where traffic_type = 'Others'
                            and behavior = 'page_view'
                        group by 
                            fpc, 
                            referrer,
                            session, 
                            domain
                        having count(*) >= 2
                        ) a
                        
                    group by domain, referrer
                    ) b
                
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(*) invalid
                    from (
                        select 
                            fpc, 
                            referrer,
                            session, 
                            domain, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        where traffic_type = 'Others'
                            and behavior = 'page_view'
                        group by 
                            fpc, 
                            referrer,
                            session, 
                            domain
                        having count(*) = 1
                        ) a
                        
                    group by domain, referrer
                    ) d
                    
                    on b.referrer = d.referrer
                        and b.domain = d.domain   
                group by 
                    b.referrer, 
                    b.domain
                        with rollup
                having b.referrer is not null
                ) e
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告活動 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                b.campaign, 
                ifnull(b.domain, '#') domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * round(ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) invalid_ratio, 
                row_number () over (partition by b.tag_date, b.domain order by ifnull(valid, 0) desc, ifnull(invalid, 0) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    tag_date, 
                    campaign, 
                    domain, 
                    count(*) valid
                from (
                    select 
                        date(created_at) tag_date, 
                        fpc, 
                        campaign,
                        session, 
                        domain, 
                        count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where campaign <> 'NULL'
                        and campaign is not null
                        and campaign <> ''
                        and behavior = 'page_view'
                    group by 
                        date(created_at),
                        fpc, 
                        campaign,
                        session, 
                        domain
                    having count(*) >= 2
                    ) a
                    
                group by tag_date, domain, campaign
                ) b
            
                left join
                (
                select 
                    tag_date, 
                    campaign, 
                    domain, 
                    count(*) invalid
                from (
                    select 
                        date(created_at) tag_date, 
                        fpc, 
                        campaign,
                        session, 
                        domain, 
                        count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where campaign <> 'NULL'
                        and campaign is not null
                        and campaign <> ''
                        and behavior = 'page_view'
                    group by 
                        date(created_at),
                        fpc, 
                        campaign,
                        session, 
                        domain
                    having count(*) = 1
                    ) c
                    
                group by tag_date, domain, campaign
                ) d
                
                on b.tag_date = d.tag_date 
                    and b.campaign = d.campaign
                    and b.domain = d.domain
            group by 
                b.campaign, 
                b.domain
                    with rollup
            having b.campaign is not null
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
        ;
        
        ##【頁面深度分析】廣告流量 - 廣告活動 - 廣告來源x廣告媒介 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                b.source_medium,
                ifnull(b.domain, '#') domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * round(ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) invalid_ratio, 
                row_number () over (partition by b.tag_date, b.domain order by ifnull(valid, 0) desc, ifnull(invalid, 0) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    tag_date, 
                    source_medium,
                    domain, 
                    count(*) valid
                from (
                    select 
                        date(created_at) tag_date, 
                        fpc, 
                        source_medium, 
                        session, 
                        domain, 
                        count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where campaign <> 'NULL'
                        and campaign is not null
                        and campaign <> ''
                	and behavior = 'page_view'
                    group by 
                        date(created_at),
                        fpc, 
                        source_medium, 
                        session, 
                        domain
                    having count(*) >= 2
                    ) a
                    
                group by 
                    tag_date, 
                    source_medium, 
                    domain
                ) b
            
                left join
                (
                select 
                    tag_date, 
                    source_medium,
                    domain, 
                    count(*) invalid
                from (
                    select 
                        date(created_at) tag_date, 
                        fpc, 
                        source_medium, 
                        session, 
                        domain, 
                        count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where campaign <> 'NULL'
                        and campaign is not null
                        and campaign <> ''
                	and behavior = 'page_view'
                    group by 
                        date(created_at),
                        fpc, 
                        source_medium, 
                        session, 
                        domain
                    having count(*) = 1
                    ) c
                    
                group by 
                    tag_date, 
                    source_medium, 
                    domain
                ) d
                
                on b.tag_date = d.tag_date 
                    and b.source_medium = d.source_medium
                    and b.domain = d.domain
            group by 
                b.source_medium,
                b.domain
                    with rollup
            having b.source_medium is not null
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】熱門頁面 - 頁面瀏覽 圓餅圖 
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ifnull(domain, '#') domain, 
                count(*) freq,
		null prop,  
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    date(created_at) tag_date, 
                    fpc, 
                    domain,
                    session, 
                    count(*)
                from ${project_name}.${type}_both_${org_id}_etl
                where behavior = 'page_view'
                group by 
                    date(created_at),
                    fpc, 
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
            where domain = '#'
            ) b
            ON a.tag_date = b.tag_date
                and a.span = b.span
        SET a.prop = 100 * round(a.freq / b.freq, 2)
        WHERE a.tag_date = '${vDate}' + interval 1 day
            and a.span = 'daily'
	;

        ##【頁面深度分析】熱門頁面 - 頁面標題 - 頁面標題 長條圖        
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                b.page_title,
                ifnull(b.domain, '#') domain, 
                ifnull(valid, 0) valid, 
                ifnull(invalid, 0) * -1 invalid, 
                100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                100 * round(ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) invalid_ratio, 
                row_number () over (partition by b.tag_date, b.domain order by ifnull(valid, 0) desc, ifnull(invalid, 0) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    tag_date, 
                    page_title, 
                    domain, 
                    count(*) valid
                from (
                    select 
                        date(created_at) tag_date, 
                        fpc, 
                        session, 
                        page_title, 
                        domain, 
                        count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where behavior = 'page_view'
                    group by 
                        date(created_at),
                        fpc, 
                        session, 
                        page_title, 
                        domain
                    having count(*) >= 2
                    ) a
                    
                group by tag_date, page_title, domain
                ) b
                
                left join 
                (
                select 
                    tag_date, 
                    page_title, 
                    domain, 
                    count(*) invalid
                from (
                    select 
                        date(created_at) tag_date, 
                        fpc, 
                        session, 
                        page_title, 
                        domain
                    from ${project_name}.${type}_both_${org_id}_etl
                    where behavior = 'page_view'
                    group by 
                        date(created_at),
                        fpc, 
                        session, 
                        page_title, 
                        domain
                    having count(*) = 1
                    ) c
                    
                group by tag_date, page_title, domain
                ) d
                
                on b.tag_date = d.tag_date 
                    and b.page_title = d.page_title
                    and b.domain = d.domain
            group by 
                b.page_title, 
                b.domain
                    with rollup
            having b.page_title is not null
        ;    
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
        ;"
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl.error 


    export sql_3="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing 
            (tag_date, span, start_date, end_date, landing)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and landing <> '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.landing = b.domain
            where a.landing is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ; 
        #INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_landing 
            #(tag_date, span, start_date, end_date, landing)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', '#')
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
        ; 


        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_traffic 
            (tag_date, span, start_date, end_date, traffic_type, referrer, domain)
            
            select 
                '${vDate}' + interval 1 day, 
                'daily', 
                '${vDate}', 
                '${vDate}', 
                ifnull(traffic_type, '#') traffic_type, 
                ifnull(referrer, '#') referrer, 
                b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> '#'    
                    and referrer = '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ;  
        #INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_traffic 
            #(tag_date, span, start_date, end_date, domain)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', '#')
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
        ; 

        
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
            (tag_date, span, start_date, end_date, domain)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_campaign
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
        ;
        #INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_campaign 
            #(tag_date, span, start_date, end_date, domain)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', '#')
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
        ; 


        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_medium
            (tag_date, span, start_date, end_date, domain)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_medium
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
        ;
        #INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_medium 
            #(tag_date, span, start_date, end_date, domain)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', '#')
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
        ; 


        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_domain
            (tag_date, span, start_date, end_date, domain)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
        ;
        #INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_domain 
            #(tag_date, span, start_date, end_date, domain)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', '#')
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
        ; 


        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_title
            (tag_date, span, start_date, end_date, domain)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_title
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and domain <> '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
	;
        #INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_title 
            #(tag_date, span, start_date, end_date, domain)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', '#')
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
    	;"
    echo ''
    echo [INSERT the missing domain INTO ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 


    if [ ${vDateName} = Sun ]; 
    then 
        export sql_4="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    landing, 
                    sum(freq) freq,
		    null prop, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and span = 'daily'
                group by landing
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                INNER JOIN
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where landing = '#'
                ) b
                ON a.tag_date = b.tag_date
                    and a.span = b.span
            SET a.prop = 100 * round(a.freq / b.freq, 2)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'weekly'
	    ; 

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
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,     
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
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
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
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,          
                    row_number () over (partition by domain, traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and traffic_type <> 'Others'
                    and referrer <> '#'
                    and span = 'daily'
                group by 
                    traffic_type,
                    referrer, 
                    domain
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
                    max(traffic_type) traffic_type,
                    referrer, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                    and traffic_type = 'Others'
                    and referrer <> '#'
                    and span = 'daily'
                group by 
                    referrer,
                    domain 
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ; 
            
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
                where domain = '#'
                ) b
                ON a.tag_date = b.tag_date
                    and a.span = b.span
            SET a.prop = 100 * round(a.freq / b.freq, 2)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'weekly'
	    ;

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
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,    
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
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_medium
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;
        "  
        echo ''
        echo [Do the ${vDateName} stuff. INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
        mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 

    else 
        echo [today is ${vDateName}, not Sunday. No Need to do the weekly statistics.]
    fi


    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_5="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,  
                    landing, 
                    sum(freq) freq,
		    null prop,  
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
                    and span = 'daily'
                group by landing
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                INNER JOIN
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_landing
                where landing = '#'
                ) b
                ON a.tag_date = b.tag_date
                    and a.span = b.span
            SET a.prop = 100 * round(a.freq / b.freq, 2)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'monthly'
	    ;


            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date, 
                    campaign, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,    
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_campaign
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
                    and span = 'daily'
                group by 
                    campaign, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ; 
            
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial,
		    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date, 
                    source_medium, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_medium
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
                    and span = 'daily'
                group by 
                    source_medium, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ; 
            
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial,
		    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date, 
                    traffic_type,
                    referrer, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,    
                    row_number () over (partition by domain, traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
                    and traffic_type <> 'Others'
                    and referrer <> '#'
                    and span = 'daily'
                group by 
                    traffic_type,
                    referrer, 
                    domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;             
            
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial,
		    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date, 
                    max(traffic_type) traffic_type,
                    referrer, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_traffic
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
                    and traffic_type = 'Others'
                    and referrer <> '#'
                    and span = 'daily'
                group by 
                    referrer,
                    domain 
            ;         
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ; 
            
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial,
		    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,  
                    domain, 
                    sum(freq) freq,
		    null prop,
                    null time_flag,   
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
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
                where domain = '#'
                ) b
                ON a.tag_date = b.tag_date
                    and a.span = b.span
            SET a.prop = 100 * round(a.freq / b.freq, 2)
            WHERE a.tag_date = '${vDate}' + interval 1 day
                and a.span = 'monthly'
	    ;


            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial,
		    '${vDate}' + interval 1 day tag_date,  
                    'monthly' span, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date, 
                    page_title, 
                    domain, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                    100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
                    row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_title
                where start_date >= '${vMonthFirst}'
                    and start_date < '${vMonthLast}' + interval 1 day
                    and span = 'daily'
                group by 
                    page_title, 
                    domain
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;


            # UPDATE the monthly time_flag
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_medium
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ; 
        "  
    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi
    echo ''
    echo [Do the ${vMonthLast} stuff. INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 


    for seasonDate in $seasonEnd
    do 
        if [ ${vDate} = ${seasonDate} ]; 
        then 
            export sql_6="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,  
                        landing, 
                        sum(freq) freq,
			null prop,  
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_landing
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by landing
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ; 

                UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing a
                    INNER JOIN
                    (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id}_landing
                    where landing = '#'
                    ) b
                    ON a.tag_date = b.tag_date
                        and a.span = b.span
                SET a.prop = 100 * round(a.freq / b.freq, 2)
                WHERE a.tag_date = '${vDate}' + interval 1 day
                    and a.span = 'seasonal'
		;
            
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
                        100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                        100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
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
                        100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                        100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
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
                        100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                        100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
                        row_number () over (partition by domain, traffic_type order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                        and traffic_type <> 'Others'
                        and referrer <> '#'
                    group by 
                        traffic_type,
                        referrer, 
                        domain
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
                        max(traffic_type) traffic_type,
                        referrer, 
                        domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                        100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
                        row_number () over (partition by domain order by sum(valid) desc, sum(invalid)) ranking, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic
                    where start_date >= '${vDate}' + interval 1 day - interval 3 month
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                        and traffic_type = 'Others'
                        and referrer <> '#'
                    group by 
                        referrer,
                        domain 
                ;         

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
                    where domain = '#'
                    ) b
                    ON a.tag_date = b.tag_date
                        and a.span = b.span
                SET a.prop = 100 * round(a.person / b.person, 2)
                WHERE a.tag_date = '${vDate}' + interval 1 day
                    and a.span = 'seasonal'
		;
 
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day - interval 3 month tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        page_title, 
                        domain, 
                        sum(valid) valid, 
                        sum(invalid) invalid, 
                        100 * round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2) valid_ratio, 
                        100 * (1 - round(sum(valid) / (sum(valid) + sum(invalid * -1)), 2)) invalid_ratio,  
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
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_landing
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_domain
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_traffic
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_campaign
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_medium
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_title
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ; 
            "  
        else 
            echo [The current date is ${vDate}. The seasonal statisitcs date is ${seasonDate}.]
        fi
    done
    echo ''
    echo [Do the ${season} stuff. INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain/traffic/campaign/source/title]
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_domain_traffic_campaign_medium_title.error 


#done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt
done < /root/datapool/export_file/cdp/web_event/web_event.cdp_org_id.txt
echo ''
echo [end the ${vDate} data at `date`]
