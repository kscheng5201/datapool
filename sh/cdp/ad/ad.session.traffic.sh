#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 進站前路徑分析 (導流分析)
# Author: Benson Cheng
# Created_at: 2021-12-23
# Updated_at: 2021-12-30
####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type="session"
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
    echo ''
    echo [beforehand: DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_etl;"

    export sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_before (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '原始 referrer', 
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_created_at (created_at),  
            key idx_fpc (fpc), 
    	    key idx_campaign_id (campaign_id),
            key idx_referrer (referrer),
            key idx_traffic_type (traffic_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天原始資料）'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '原始 referrer', 
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_created_at (created_at),  
            key idx_fpc (fpc), 
    	    key idx_campaign_id (campaign_id),
            key idx_referrer (referrer),
            key idx_traffic_type (traffic_type), 
            key idx_session (session)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天原始資料），加入 session'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_mid (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '整理後 referrer', 
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_created_at (created_at),  
            key idx_fpc (fpc), 
    	    key idx_campaign_id (campaign_id),
            key idx_referrer (referrer),
            key idx_traffic_type (traffic_type), 
            key idx_session (session)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天資料），加入 session，referrer 初整理'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '整理後 referrer', 
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            session_type varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in/valid',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_created_at (created_at),  
            key idx_fpc (fpc), 
    	    key idx_campaign_id (campaign_id),
            key idx_referrer (referrer),
            key idx_traffic_type (traffic_type), 
            key idx_session (session), 
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天資料），加入 session，referrer 整理完'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; ALL = 全部',        
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 session', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 session', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 session / (有效 session + 無效 session)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 session / (有效 session + 無效 session)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, traffic_type, referrer),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),  
            key idx_span (span),
            key idx_campaign_id (campaign_id),
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at), 
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天資料）- 全部流量／自然流量／廣告流量／其它流量'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.person_${table_name}_${org_id} (   
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 流水編號',
            traffic_type varchar(16) NOT NULL COMMENT 'Ad/Direct/Organic/Others', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'http 參照位址; ALL = 全部',        
            valid int NOT NULL DEFAULT 0 COMMENT 'freqency of 有效 user', 
            invalid int NOT NULL DEFAULT 0 COMMENT 'freqency of 無效 user', 
            valid_ratio int DEFAULT NULL COMMENT '% of 有效 user / (有效 user + 無效 user)', 
            invalid_ratio int DEFAULT NULL COMMENT '% of 無效 user / (有效 user + 無效 user)', 
            ranking int NOT NULL DEFAULT 0 COMMENT 'valid 由多至少的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, traffic_type, referrer),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),  
            key idx_span (span),
            key idx_campaign_id (campaign_id),
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_created_at (created_at), 
            key idx_ranking (ranking)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析——進站前路徑分析（90天資料）- 全部流量／自然流量／廣告流量／其它流量'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_before]
    echo $sql_1    
    mysql --login-path=$dest_login_path -e "$sql_1"
    
    export sql_2="
        select concat_ws('_', campaign_id, campaign_start, campaign_end) campaign_detail
        from ${project_name}.fpc_${org_id}
        where campaign_start <= '${vDate}'
            and campaign_end >= '${vDate}'
        group by campaign_id
        ;"
    echo ''
    echo [Get the campaign_detail on web]
    mysql --login-path=${dest_login_path} -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt

    while read campaign_detail; 
    do 
        while read db_id; 
        do 
            export sql_3="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    $(echo ${campaign_detail} | cut -d _ -f 1) campaign_id,
                    source domain,
                    fpc, 
                    case 
                        when page_url REGEXP 'utm_' then 'Ad'
                        when referrer is null or referrer = '' then 'Direct'
                        when referrer REGEXP 'google|yahoo|bing|MSN' then 'Organic'
                        else 'Others'
                    end traffic_type, 
                    referrer,
                    from_unixtime(created_at) created_at,  
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data
                where created_at >= UNIX_TIMESTAMP('${vDate}' - interval 89 day)
                    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and fpc in
                    (
                    select fpc
                    from cdp_${org_id}.user_campaign a
                        inner join cdp_web_${db_id}.fpc_unique b
                            on a.channel_id = b.id
                    where a.created_at = ${vDate}
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                        and db_id = ${db_id}
                        and channel_type = 1
                    )
                ;"    
            #### Export Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [exporting data FROM cdp_web_${db_id}.fpc_raw_data]
            echo $sql_3
            mysql --login-path=${src_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.error
            echo $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.txt
            tail $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.txt

            #### Import Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.txt to ${project_name}.${type}_${table_name}_${org_id}_before]
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_before IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_$(echo ${campaign_detail} | cut -d _ -f 1)_before.error

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt


    export sql_4="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                tag_date,
                campaign_id, 
                domain,
                fpc, 
                traffic_type, 
                referrer, 
                concat(date_format(created_at, '%Y%m%d'), LPAD(session_break, 2 ,'0')) session,
                created_at, 
                now() updated_at
            from (
                select c.*, 
                    if(@campaign_id = campaign_id, 
                        if(@date = date(created_at), 
                            if(@fpc = fpc, 
                                if(session_pre = 1, @session_pre := @session_pre + 1, @session_pre), 
                                @session_pre := 1), 
                            @session_pre := 1), 
                        @session_pre := 1) session_break, 
                    @campaign_id := campaign_id campaign_v, 
                    @date := date(created_at) date_v, 
                    @fpc := fpc fpc_v
                from (
                    select a.*, if(timestampdiff(minute, b.created_at, a.created_at) >= 30, 1, 0) session_pre
                    from (
                        select 
                            campaign_id,
                            ifnull(g.fpc, s.fpc) fpc, 
                            s.domain, 
                            traffic_type, 
                            referrer,
                            created_at,  
                            row_number () over (partition by campaign_id, date(created_at), fpc order by created_at) rid
                        from ${project_name}.${type}_${table_name}_${org_id}_before s
                            left join uuid.cdp_fpc_mapping g
                            on s.fpc = g.origin_fpc and g.domain = substring_index(s.domain, '/', 1)
                        ) a
                        
                        left join
                        (
                        select 
                            campaign_id,
                            ifnull(g.fpc, s.fpc) fpc, 
                            s.domain, 
                            traffic_type, 
                            referrer,
                            created_at,  
                            row_number () over (partition by campaign_id, date(created_at), fpc order by created_at) rid
                        from ${project_name}.${type}_${table_name}_${org_id}_before s
                            left join uuid.cdp_fpc_mapping g
                            on s.fpc = g.origin_fpc and g.domain = substring_index(s.domain, '/', 1)
                        ) b
                        on a.campaign_id = b.campaign_id
                            and date(a.created_at) = date(b.created_at)
                            and a.fpc = b.fpc
                            and a.rid = b.rid + 1
                    ) c, 
                    (select @session_pre := 1, @campaign_id, @date, @fpc) d
                ) e
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error


    export sql_5="    
        select domain
        from codebook_cdp.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web' 
            
        union all
        
        select string
        from codebook_cdp.referrer_detail
        ;"
    echo ''
    echo [Get the referrer_detail for ${org_id}]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_referrer_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_referrer_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_referrer_detail.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_referrer_detail.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_referrer_detail.txt
   
    export sql_6="    
        select group_concat(domain separator '|')
        from (
            select domain
            from codebook_cdp.organization_domain
            where org_id = ${org_id}
                and domain_type = 'web'
            
            union all
            
            select string
            from codebook_cdp.referrer_detail
            ) a
        ;"
    echo ''
    echo [Get the NOT referrer_detail for ${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_6" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_NOTreferrer_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_NOTreferrer_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_NOTreferrer_detail.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_NOTreferrer_detail.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_NOTreferrer_detail.txt
   


    while read referrer_detail; 
    do 
        export sql_7="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_mid
                select 
                    null serial, 
                    tag_date, 
                    campaign_id, 
                    a.domain, 
                    fpc,
                    traffic_type, 
                    if(referrer REGEXP '${referrer_detail}', '${referrer_detail}', referrer) referrer,
                    session, 
                    created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_src a
                where referrer REGEXP '${referrer_detail}'
            ;"
        echo ''
        echo [read ${referrer_detail} INTO ${project_name}.${type}_${table_name}_${org_id}_mid]
        echo $sql_7
        mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_mid.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_referrer_detail.txt


    while read NOTreferrer_detail; 
    do 
        export sql_8="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_mid
                select 
                    null serial, 
                    tag_date, 
                    campaign_id, 
                    a.domain, 
                    fpc,
                    traffic_type, 
                    referrer,
                    session, 
                    created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_src a
                where referrer NOT REGEXP '${NOTreferrer_detail}'
            ;"
        echo ''
        echo [read ${referrer_detail} INTO ${project_name}.${type}_${table_name}_${org_id}_mid]
        echo $sql_8
        mysql --login-path=$dest_login_path -e "$sql_8" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_mid.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_mid.error
    done < $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_NOTreferrer_detail.txt

    export sql_9="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl
            select 
                null serial, 
                tag_date, 
                campaign_id, 
                a.domain, 
                fpc,
                traffic_type, 
                ifnull(b.output, substring_index(substring_index(a.referrer, '://', -1), '/', 1)) referrer,
                session, 
                null session_type, 
                a.created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_mid a
                left join codebook_cdp.referrer_detail b
                on a.referrer REGEXP b.string
        ;"
    echo ''
    echo [reset the referrer INTO ${project_name}.${type}_${table_name}_${org_id}_etl]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_etl.error

    export sql_9a="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_etl a
            INNER JOIN
            (
            select fpc, session, count(*)
            from ${project_name}.${type}_${table_name}_${org_id}_etl
            group by fpc, session
            having count(*) >= 2
            ) b
            ON a.fpc = b.fpc
                and a.session = b.session
        SET session_type = 'valid'
        ;
        
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_etl
        SET session_type = 'invalid'
        WHERE session_type is null
            or session_type = ''
        ;"
    echo [start: date on ${vDate}]
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_etl on session_type]
    echo $sql_9a
    mysql --login-path=$dest_login_path -e "$sql_9a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl_sql9a.error

    export sql_10="
        ##【進站前路徑分析】導流分析 - 全部流量 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                campaign_id,
                traffic_type, 
                referrer, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by campaign_id order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.campaign_id,
                    b.traffic_type, 
                    'ALL' referrer, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio 
                from (
                    select 
                        campaign_id,
                        traffic_type, 
                        count(distinct fpc, session) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'valid'
                    group by campaign_id, traffic_type
                    ) b
                
                    left join
                    (
                    select 
                        campaign_id,
                        traffic_type, 
                        count(distinct fpc, session) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'invalid'
                    group by campaign_id, traffic_type
                    ) d
                    
                    on b.traffic_type = d.traffic_type
                        and b.campaign_id = d.campaign_id          
                group by 
                    b.traffic_type, 
                    b.campaign_id
                having b.traffic_type is not null
                ) e
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
    echo [【進站前路徑分析】導流分析 - 全部流量 分向長條圖]
    echo $sql_10
    mysql --login-path=$dest_login_path -e "$sql_10" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_daily.error 

    export sql_11="
        ##【進站前路徑分析】導流分析 - 個別流量 分向長條圖
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                campaign_id,
                traffic_type, 
                referrer, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by campaign_id, traffic_type order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.campaign_id,
                    b.traffic_type, 
                    b.referrer, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio 
                from (
                    select 
                        campaign_id,
                        traffic_type, 
                        referrer,
                        count(distinct fpc, session) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'valid'
                    group by campaign_id, traffic_type, referrer
                    ) b
                
                    left join
                    (
                    select 
                        campaign_id,
                        traffic_type, 
                        referrer,
                        count(distinct fpc, session) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'invalid'
                    group by campaign_id, traffic_type, referrer
                    ) d
                    
                    on b.traffic_type = d.traffic_type
                        and b.campaign_id = d.campaign_id   
                        and b.referrer = d.referrer
                group by 
                    b.traffic_type, 
                    b.campaign_id, 
                    b.referrer
                having b.traffic_type is not null
                ) e
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
    echo [【進站前路徑分析】導流分析 - 個別流量 分向長條圖]]
    echo $sql_11
    mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_daily.error 


    export sql_10a="
        ##【進站前路徑分析】導流分析 - 全部人數 分向長條圖
        INSERT INTO ${project_name}.person_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                campaign_id,
                traffic_type, 
                referrer, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by campaign_id order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.campaign_id,
                    b.traffic_type, 
                    'ALL' referrer, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio 
                from (
                    select 
                        campaign_id,
                        traffic_type, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'valid'
                    group by campaign_id, traffic_type
                    ) b
                
                    left join
                    (
                    select 
                        campaign_id,
                        traffic_type, 
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'invalid'
                    group by campaign_id, traffic_type
                    ) d
                    
                    on b.traffic_type = d.traffic_type
                        and b.campaign_id = d.campaign_id          
                group by 
                    b.traffic_type, 
                    b.campaign_id
                having b.traffic_type is not null
                ) e
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.person_${table_name}_${org_id}]
    echo [【進站前路徑分析】導流分析 - 全部人數 分向長條圖]
    echo $sql_10a
    mysql --login-path=$dest_login_path -e "$sql_10a" 2>>$error_dir/$project_name/${project_name}.person_${table_name}_${org_id}_daily.error 

    export sql_11a="
        ##【進站前路徑分析】導流分析 - 個別人數 分向長條圖
        INSERT INTO ${project_name}.person_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                campaign_id,
                traffic_type, 
                referrer, 
                valid, 
                invalid, 
                valid_ratio, 
                invalid_ratio, 
                row_number () over (partition by campaign_id, traffic_type order by valid desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    b.campaign_id,
                    b.traffic_type, 
                    b.referrer, 
                    ifnull(valid, 0) valid, 
                    ifnull(invalid, 0) * -1 invalid, 
                    100 * round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2) valid_ratio, 
                    100 * (1 - round(ifnull(valid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0)), 2)) invalid_ratio 
                from (
                    select 
                        campaign_id,
                        traffic_type, 
                        referrer,
                        count(distinct fpc) valid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'valid'
                    group by campaign_id, traffic_type, referrer
                    ) b
                
                    left join
                    (
                    select 
                        campaign_id,
                        traffic_type, 
                        referrer,
                        count(distinct fpc) invalid
                    from ${project_name}.${type}_${table_name}_${org_id}_etl 
                    where session_type = 'invalid'
                    group by campaign_id, traffic_type, referrer
                    ) d
                    
                    on b.traffic_type = d.traffic_type
                        and b.campaign_id = d.campaign_id   
                        and b.referrer = d.referrer
                group by 
                    b.traffic_type, 
                    b.campaign_id, 
                    b.referrer
                having b.traffic_type is not null
                ) e
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.person_${table_name}_${org_id}]
    echo [【進站前路徑分析】導流分析 - 個別人數 分向長條圖]
    echo $sql_11a
    mysql --login-path=$dest_login_path -e "$sql_11a" 2>>$error_dir/$project_name/${project_name}.person_${table_name}_${org_id}_daily.error 

    export sql_12="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        ;
        UPDATE ${project_name}.person_${table_name}_${org_id}
        SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} and person_${table_name}_${org_id} on time_flag]
    echo $sql_12
    mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql12.error 

    echo ''
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_before]
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_src]
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_mid]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_before;"
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_src;"
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_mid;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt


echo ''
echo 'end: ' `date`            
