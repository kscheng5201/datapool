#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web_event"
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
"


#### loop by org_id ####
export sql_0="
    select org_id
    from cdp_organization.organization_domain
    where domain_type = 'web'
    group by org_id
    limit 4
    ;"    
echo ''
echo [Get the org_id]
mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt


while read org_id; 
do 
    export sql_1="  
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_landing (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            period varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            landing varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '每個 session 開始時的進入 domain', 
            person int NOT NULL DEFAULT 0 COMMENT 'frequency of unique user',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, period, landing),
            key idx_stat_date (stat_date), 
            key idx_landing (landing), 
            key idx_period (period),
            key idx_created_at (created_at), 
            key idx_updated_at (updated_at)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】-導流分析／廣告流量-登入網域'
        ; 
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_domain (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            period varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            person int NOT NULL DEFAULT 0 COMMENT 'frequency of unique user',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, period, domain),
            key idx_stat_date (stat_date), 
            key idx_domain (domain), 
            key idx_period (period),
            key idx_created_at (created_at), 
            key idx_updated_at (updated_at)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】-熱門頁面-瀏覽網域'
        ; 
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_traffic (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            period varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            p_valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            p_invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            p_valid_ratio int NOT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            p_invalid_ratio int NOT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'p_valid 由多至少的排名',             
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, period, traffic_type, referrer),
            key idx_stat_date (stat_date), 
            key idx_domain (domain), 
            key idx_period (period),
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at), 
            key idx_ranking (ranking)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】導流分析-流量種類/自然流量/其它流量'
        ;
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_campaign (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            period varchar(8) NOT NULL COMMENT 'weekly/monthly/seasonal', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_campaign廣告活動; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            p_valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            p_invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            p_valid_ratio int NOT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            p_invalid_ratio int NOT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'p_valid 由多至少的排名',             
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, period, campaign),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_period (period),
            key idx_campaign (campaign),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告活動'
        ;        
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_medium (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            period varchar(8) NOT NULL COMMENT 'yearweek/weekly/monthly/seasonal', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            source_medium varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_medium廣告來源 x utm_medium廣告媒介; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            p_valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            p_invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            p_valid_ratio int NOT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            p_invalid_ratio int NOT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'p_valid 由多至少的排名',             
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, period, source_medium),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_period (period),
            key idx_medium_medium (source_medium),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】廣告流量-廣告來源x廣告媒介'
        ;               
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_title (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            period varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            p_valid int NOT NULL COMMENT '不重複用戶數 in 有效 session', 
            p_invalid int NOT NULL COMMENT '不重複用戶數 in 無效 session', 
            p_valid_ratio int NOT NULL COMMENT '% of 有效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            p_invalid_ratio int NOT NULL COMMENT '% of 無效 session 人數 / (有效 session 人數 + 無效 session 人數)', 
            ranking int DEFAULT NULL COMMENT 'p_valid 由多至少的排名',             
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, period, page_title),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_period (period),
            key idx_page_title (page_title),
            key idx_ranking (ranking)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【頁面深度分析】熱門頁面-頁面標題'
        ;

        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_domain_fpc (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
    	    landing varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session 開始時的第一個 domain', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
    	    session varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, fpc, domain, session),
            key idx_stat_date (stat_date), 
            key idx_fpc (fpc), 
            key idx_domain (domain),
            key idx_session (session), 
            key idx_landing (landing)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 fpc on 綜合儀表板【頁面深度分析】-熱門頁面-瀏覽網域'
        ; 
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; # = 全部', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, traffic_type, referrer, fpc, session_type),
            key idx_stat_date (stat_date), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 fpc on 綜合儀表板【頁面深度分析】導流分析-流量種類/自然流量/其它流量'
        ;
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            campaign varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_campaign廣告活動; # = 全部', 
    	    source_medium varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'utm_medium廣告來源 x utm_medium廣告媒介; # = 全部',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, campaign, fpc, source_medium, session_type),
            key idx_source_medium (source_medium), 
            key idx_domain (domain), 
            key idx_campaign (campaign), 
            key idx_stat_date (stat_date), 
            key idx_fpc (fpc)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 fpc on 綜合儀表板【頁面深度分析】廣告流量-廣告活動'
        ;        
       create table if not exists ${project_name}.${type}_${table_name}_${org_id}_title_fpc (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            stat_date date NOT NULL COMMENT '資料統計日', 
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',             
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            session_type varchar(9) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'in/valid', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (stat_date, domain, page_title, fpc, session_type),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_page_title (page_title), 
            key idx_stat_date (stat_date), 
            key idx_fpc (fpc)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='原始 fpc on 綜合儀表板【頁面深度分析】熱門頁面-頁面標題'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} and ${project_name}.${type}_${table_name}_${org_id}_fpc]
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error


    export sql_2="  
        ##【頁面深度分析】導流分析
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
            select 
                null serial, 
                '${vDate}', 
                traffic_type, 
                referrer, 
                domain, 
                fpc,
                'p_valid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    traffic_type,
                    referrer,
                    session, 
                    domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                group by 
                    fpc, 
                    traffic_type,
                    referrer,
                    session, 
                    domain
                having count(*) >= 2
                ) a
                
            group by 
                traffic_type,
                referrer, 
                domain, 
                fpc
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc AUTO_INCREMENT = 1
        ;  
        
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
            select 
                null serial, 
                '${vDate}', 
                traffic_type, 
                referrer, 
                domain, 
                fpc,
                'p_invalid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    traffic_type,
		    referrer,
                    session, 
                    domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                group by 
                    fpc, 
                    traffic_type,
                    referrer,
                    session, 
                    domain
                having count(*) = 1
                ) a
                
            group by 
                traffic_type,
                referrer, 
                domain, 
                fpc
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】廣告流量
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
            select 
                null serial, 
                '${vDate}', 
                campaign,
                source_medium, 
                domain, 
                fpc,
                'p_valid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    traffic_type,
                    campaign,
                    source_medium, 
                    session, 
                    domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                    and traffic_type = 'Ad'
                group by 
                    fpc, 
                    traffic_type,
                    campaign,
                    source_medium, 
                    session, 
                    domain
                having count(*) >= 2
                ) a
                
            group by 
                campaign,
                source_medium, 
                domain, 
                fpc
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc AUTO_INCREMENT = 1
        ;

        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
            select 
                null serial, 
                '${vDate}', 
                campaign,
                source_medium, 
                domain, 
                fpc,
                'p_invalid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    traffic_type,
                    campaign,
                    source_medium, 
                    session, 
                    domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                    and traffic_type = 'Ad'
                group by 
                    fpc, 
                    traffic_type,
                    campaign,
                    source_medium, 
                    session, 
                    domain
                having count(*) = 1
                ) a
                
            group by 
                campaign,
                source_medium, 
                domain, 
                fpc
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】熱門頁面
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title_fpc
            select 
                null serial, 
                '${vDate}', 
                page_title, 
                domain, 
                fpc,
                'p_valid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    page_title, 
                    session, 
                    domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                    and traffic_type = 'Ad'
                group by 
                    fpc, 
                    page_title, 
                    session, 
                    domain
                having count(*) >= 2
                ) a
                
            group by 
                page_title, 
                domain, 
                fpc
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title_fpc AUTO_INCREMENT = 1
        ;
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title_fpc
            select 
                null serial, 
                '${vDate}', 
                page_title, 
                domain, 
                fpc,
                'p_invalid' session_type, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc, 
                    page_title, 
                    session, 
                    domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                    and traffic_type = 'Ad'
                group by 
                    fpc, 
                    page_title, 
                    session, 
                    domain
                having count(*) = 1
                ) a
                
            group by 
                page_title, 
                domain, 
                fpc
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title_fpc AUTO_INCREMENT = 1
        ;

        ##【頁面深度分析】登入網域 vs 瀏覽網域
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
            select 
                null serial, 
                '${vDate}' stat_date, 
                landing, 
                domain, 
                a.fpc, 
                a.session, 
                now() created_at, 
                now() updated_at
            from (
                select fpc, session, domain landing
                from (
                    select *, row_number () over (partition by fpc, session order by created_at) rid
                    from ${project_name}.session_both_${org_id}_etl
                    where behavior = 'page_view'
                    ) a
                where rid = 1
                group by fpc, session, domain
                ) a
                
                inner join
                (
                select fpc, session, domain
                from ${project_name}.session_both_${org_id}_etl
                where behavior = 'page_view'
                group by fpc, session, domain
                ) b
                on a.fpc = b.fpc
                    and a.session = b.session

            group by 
                landing, 
                domain, 
                a.fpc, 
                a.session
    	;   
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain_fpc AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc]
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error

    if [ ${vDateName} = Sun ]; 
    then 
        export sql_3="
            ##【頁面深度分析】登入網域 vs 瀏覽網域       
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    landing, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and stat_date < '${vDate}' + interval 1 day
                group by landing
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    '#' landing, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and stat_date < '${vDate}' + interval 1 day
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;
            
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    domain, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and stat_date < '${vDate}' + interval 1 day
                group by domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,     
                    '#' domain, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and stat_date < '${vDate}' + interval 1 day
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;            

            
            ##【頁面深度分析】導流分析－流量種類
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.traffic_type, 
                    '#' referrer, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by traffic_type, domain
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.traffic_type, 
                    '#' referrer, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by traffic_type
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Organic'
                    group by referrer, domain
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) a
                    
                    left join
                    (
                    select 
                        referrer,
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;
            
            ##【頁面深度分析】導流分析－其它流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Others' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Others'
                    group by referrer, domain
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
                        and traffic_type = 'Others'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    'Others' traffic_type, 
                    a.referrer, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Others'
                    group by referrer
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
                        and traffic_type = 'Others'
                    group by referrer
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            ##【頁面深度分析】廣告流量－廣告活動
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.campaign, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by campaign, domain
                    ) a
                    
                    left join
                    (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.campaign, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign,
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by campaign
                    ) a
                    
                    left join
                    (
                    select 
                        campaign,
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
                    group by campaign
                    ) b
                    on a.campaign = b.campaign
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ;


            ##【頁面深度分析】廣告流量－廣告方式
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.source_medium, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by source_medium, domain
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.source_medium, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by source_medium
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
                    group by source_medium
                    ) b
                    on a.source_medium = b.source_medium
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ;

        
            ##【頁面深度分析】熱門頁面 - 頁面標題
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.page_title, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by page_title, domain
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vDate}' stat_date, 
                    'weekly' period, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date,  
                    a.page_title, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_valid'
                    group by page_title
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and stat_date < '${vDate}' + interval 1 day
                        and session_type = 'p_invalid'
                    group by page_title
                    ) b
                    on a.page_title = b.page_title
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;
            "
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_anything_weekly]
        mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_anything_weekly.error
    
    else 
        echo [today is ${vDateName}, not Sunday. No Need to do the weekly statistics.]
    fi


    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_4="
            ##【頁面深度分析】登入網域 vs 瀏覽網域       
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,    
                    landing, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= '${vMonthFirst}'
                    and stat_date < '${vMonthLast}' + interval 1 day
                group by landing
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,    
                    '#' landing, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= '${vMonthFirst}'
                    and stat_date < '${vMonthLast}' + interval 1 day
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
            ;
        
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,      
                    domain, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= '${vMonthFirst}'
                    and stat_date < '${vMonthLast}' + interval 1 day
                group by domain
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,    
                    '#' domain, 
                    count(distinct fpc) person,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date >= '${vMonthFirst}'
                    and stat_date < '${vMonthLast}' + interval 1 day
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
            ;
            
            ##【頁面深度分析】導流分析－流量種類
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    a.traffic_type, 
                    '#' referrer, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by traffic_type, domain
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    a.traffic_type, 
                    '#' referrer, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        traffic_type, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by traffic_type
                    ) a
                    
                    left join
                    (
                    select 
                        traffic_type, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Organic'
                    group by referrer, domain
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,  
                    'Organic' traffic_type, 
                    a.referrer, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (\order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
                        and traffic_type = 'Organic'
                    group by referrer
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;

            ##【頁面深度分析】導流分析－其它流量
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    'Others' traffic_type, 
                    a.referrer, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Others'
                    group by referrer, domain
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
                        and traffic_type = 'Others'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    'Others' traffic_type, 
                    a.referrer, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        referrer, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                        and traffic_type = 'Others'
                    group by referrer
                    ) a
                    
                    left join
                    (
                    select 
                        referrer, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
                        and traffic_type = 'Others'
                    group by referrer
                    ) b
                    on a.referrer = b.referrer
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
            ;


            ##【頁面深度分析】廣告流量－廣告活動
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,  
                    a.campaign, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by campaign, domain
                    ) a
                    
                    left join
                    (
                    select 
                        campaign, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,  
                    a.campaign, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by campaign
                    ) a
                    
                    left join
                    (
                    select 
                        campaign, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
                    group by campaign
                    ) b
                    on a.campaign = b.campaign
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
            ;

            ##【頁面深度分析】廣告流量－廣告方式
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    a.source_medium, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by source_medium, domain
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    a.source_medium, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        source_medium, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by source_medium
                    ) a
                    
                    left join
                    (
                    select 
                        source_medium, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
                    group by source_medium
                    ) b
                    on a.source_medium = b.source_medium
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
            ;
        
            ##【頁面深度分析】熱門頁面 - 頁面標題
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                select 
                    null serial, 
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    a.page_title, 
                    a.domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (partition by domain order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by page_title, domain
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        domain, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
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
                    '${vMonthLast}' stat_date, 
                    'monthly' period, 
                    '${vMonthFirst}' start_date, 
                    '${vMonthLast}' end_date,   
                    a.page_title, 
                    '#' domain, 
                    p_valid, 
                    p_invalid * -1 p_invalid, 
                    100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                    100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                    row_number () over (order by p_valid desc) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        page_title, 
                        count(distinct fpc) p_valid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_valid'
                    group by page_title
                    ) a
                    
                    left join
                    (
                    select 
                        page_title, 
                        count(distinct fpc) p_invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                    where stat_date >= '${vMonthFirst}'
                        and stat_date < '${vMonthLast}' + interval 1 day
                        and session_type = 'p_invalid'
                    group by page_title
                    ) b
                    on a.page_title = b.page_title
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
            ;        
            "  
        echo ''
	echo $sql_4
        echo [Do the ${vMonthLast} stuff. INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_anything_monthly]
        mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_anything_monthly.error 

    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi


    for seasonDate in $seasonEnd
    do 
        if [ ${vDate} = ${seasonDate} ]; 
        then 
            export sql_5="
                ##【頁面深度分析】登入網域 vs 瀏覽網域       
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,   
                        landing, 
                        count(distinct fpc) person,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                        and stat_date < '${vDate}' + interval 1 day
                    group by landing
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_landing
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,   
                        '#' landing, 
                        count(distinct fpc) person,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                        and stat_date < '${vDate}' + interval 1 day
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_landing AUTO_INCREMENT = 1
                ;
            
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,     
                        domain, 
                        count(distinct fpc) person,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                        and stat_date < '${vDate}' + interval 1 day
                    group by domain
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_domain
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day - interval 3 month stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,   
                        '#' domain, 
                        count(distinct fpc) person,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                    where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                        and stat_date < '${vDate}' + interval 1 day
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain AUTO_INCREMENT = 1
                ;
                
                ##【頁面深度分析】導流分析－流量種類
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,    
                        a.traffic_type, 
                        '#' referrer, 
                        a.domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (partition by a.domain order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            traffic_type, 
                            domain, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by traffic_type, domain
                        ) a
                        
                        left join
                        (
                        select 
                            traffic_type, 
                            domain, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,    
                        a.traffic_type, 
                        '#' referrer, 
                        '#' domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            traffic_type, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by traffic_type
                        ) a
                        
                        left join
                        (
                        select 
                            traffic_type, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,   
                        'Organic' traffic_type, 
                        a.referrer, 
                        a.domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (partition by domain order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            referrer, 
                            domain, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                            and traffic_type = 'Organic'
                        group by referrer, domain
                        ) a
                        
                        left join
                        (
                        select 
                            referrer, 
                            domain, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,   
                        'Organic' traffic_type, 
                        a.referrer, 
                        '#' domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            referrer, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                            and traffic_type = 'Organic'
                        group by referrer
                        ) a
                        
                        left join
                        (
                        select 
                            referrer, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
                            and traffic_type = 'Organic'
                        group by referrer
                        ) b
                        on a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;
    
                ##【頁面深度分析】導流分析－其它流量
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_traffic
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,    
                        'Others' traffic_type, 
                        a.referrer, 
                        a.domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (partition by domain order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            referrer, 
                            domain, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                            and traffic_type = 'Others'
                        group by referrer, domain
                        ) a
                        
                        left join
                        (
                        select 
                            referrer, 
                            domain, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
                            and traffic_type = 'Others'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,    
                        'Others' traffic_type, 
                        a.referrer, 
                        '#' domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            referrer, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                            and traffic_type = 'Others'
                        group by referrer
                        ) a
                        
                        left join
                        (
                        select 
                            referrer, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
                            and traffic_type = 'Others'
                        group by referrer
                        ) b
                        on a.referrer = b.referrer
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic AUTO_INCREMENT = 1
                ;
    
                ##【頁面深度分析】廣告流量－廣告活動
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_campaign
                    select  
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        a.campaign, 
                        a.domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (partition by domain order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            campaign, 
                            domain, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by campaign, domain
                        ) a
                        
                        left join
                        (
                        select 
                            campaign, 
                            domain, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        a.campaign, 
                        '#' domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            campaign, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by campaign
                        ) a
                        
                        left join
                        (
                        select 
                            campaign, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
                        group by campaign
                        ) b
                        on a.campaign = b.campaign
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign AUTO_INCREMENT = 1
                ;
    
                ##【頁面深度分析】廣告流量－廣告方式
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_medium
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        a.source_medium, 
                        a.domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (partition by domain order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            source_medium, 
                            domain, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by source_medium, domain
                        ) a
                        
                        left join
                        (
                        select 
                            source_medium, 
                            domain, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        a.source_medium, 
                        '#' domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            source_medium, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by source_medium
                        ) a
                        
                        left join
                        (
                        select 
                            source_medium, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
                        group by source_medium
                        ) b
                        on a.source_medium = b.source_medium
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_medium AUTO_INCREMENT = 1
                ;

                ##【頁面深度分析】熱門頁面 - 頁面標題
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_title
                    select 
                        null serial, 
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        a.page_title, 
                        a.domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (partition by domain order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            page_title, 
                            domain, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by page_title, domain
                        ) a
                        
                        left join
                        (
                        select 
                            page_title, 
                            domain, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
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
                        '${vDate}' stat_date, 
                        'seasonal' period, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${seasonDate}' end_date,  
                        a.page_title, 
                        '#' domain, 
                        p_valid, 
                        p_invalid * -1 p_invalid, 
                        100 * round(p_valid / (p_valid + p_invalid), 2) p_valid_ratio, 
                        100 * (1 - round(p_valid / (p_valid + p_invalid), 2)) p_invalid_ratio, 
                        row_number () over (order by p_valid desc) ranking, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select 
                            page_title, 
                            count(distinct fpc) p_valid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_valid'
                        group by page_title
                        ) a
                        
                        left join
                        (
                        select 
                            page_title, 
                            count(distinct fpc) p_invalid
                        from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                        where stat_date >= '${vDate}' + interval 1 day - interval 3 month
                            and stat_date < '${vDate}' + interval 1 day
                            and session_type = 'p_invalid'
                        group by page_title
                        ) b
                        on a.page_title = b.page_title
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title AUTO_INCREMENT = 1
                ;        


                delete 
                from ${project_name}.${type}_${table_name}_${org_id}_domain_fpc
                where stat_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_domain_fpc AUTO_INCREMENT = 1
                ;  

                delete 
                from ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc
                where stat_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_traffic_fpc AUTO_INCREMENT = 1
                ;  

                delete 
                from ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc
                where stat_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_campaign_fpc AUTO_INCREMENT = 1
                ;  

                delete 
                from ${project_name}.${type}_${table_name}_${org_id}_title_fpc
                where stat_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_title_fpc AUTO_INCREMENT = 1
                ;  
                "  
            echo ''
            echo [Do the ${season} stuff. INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_anything_seasonal]
            mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_anything_seasonal.error 

        else 
            echo [The current date is ${vDate}. The seasonal statisitcs date is ${seasonDate}.]
        fi
    done
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt


echo ''
echo [end the ${vDate} data at `date`]
