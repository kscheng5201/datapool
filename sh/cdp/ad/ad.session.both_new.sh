#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 來源資料彙整
# Author: Benson Cheng
# Created_at: 2021-12-16
# Updated_at: 2021-12-30
# Note: 所有程式的資料源頭
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type="session"
export table_name="both" 
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
`date -d "$(date +%Y-01-01) -1 day" +"%Y%m%d"`
"


while read org_id; 
do 
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_d]
    mysql --login-path=${dest_login_path} -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl;"
    mysql --login-path=${dest_login_path} -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src;"
    mysql --login-path=${dest_login_path} -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_d;"

    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
        ;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    export sql_3="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_d (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) signed NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            event_type tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '事件功能代碼',
            on_utm tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此紀錄是否踩到 utm: 1-有, 0-無',
            is_new tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此 fpc 是否為 utm 開始後才出現的新用戶: 1-是, 0-否',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_created_at (created_at), 
            key idx_fpc (fpc),             
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的每日 src 整合表'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) signed NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            event_type tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '事件功能代碼',
            on_utm tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此紀錄是否踩到 utm: 1-有, 0-無',
            is_new tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此 fpc 是否為 utm 開始後才出現的新用戶: 1-是, 0-否',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_created_at (created_at), 
            key idx_fpc (fpc),             
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的每日 src 整合表(原始 fpc)，有 session'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_log (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) signed NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            event_type tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '事件功能代碼',
            on_utm tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此紀錄是否踩到 utm: 1-有, 0-無',
            is_new tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此 fpc 是否為 utm 開始後才出現的新用戶: 1-是, 0-否',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            session_type varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in/valid',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_created_at (created_at), 
            key idx_fpc (fpc),             
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id), 
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的歷史 src 整合表(原始 fpc)，有 session'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) signed NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer',
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
       	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            event_type tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '事件功能代碼',
            on_utm tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此紀錄是否踩到 utm: 1-有, 0-無',
            is_new tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此 fpc 是否為 utm 開始後才出現的新用戶: 1-是, 0-否',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            session_type varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in/valid',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_created_at (created_at), 
            key idx_fpc (fpc),             
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id), 
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的每日 etl 整合表(browser fpc)，有 session'
        ;"         
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_d]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_3    
    mysql --login-path=$dest_login_path -e "$sql_3"

    while read db_id; 
    do 
        while read utm_detail; 
        do 
            export sql_4="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    '$(echo ${utm_detail} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${utm_detail} | cut -d _ -f 2)' utm_id,
                    a.fpc, 
                    source domain, 
                    'page_view' behavior, 
                    case 
                        when a.page_url REGEXP 'utm_' then 'Ad'
                        when referrer is null or referrer = '' then 'Direct'
                        when referrer REGEXP 'google|yahoo|bing|MSN' then 'Organic'
                        else 'Others'
                    end traffic_type, 
                    referrer,
                    page_title,
		    a.page_url, 
                    null event_type,
                    if(b.page_url is not null, 1, 0) on_utm,     
                    if(c.fpc is not null, 1, 0) is_new,
                    FROM_UNIXTIME(a.created_at) created_at,
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data a
                    left join 
                    (
                    select url page_url
                    from cdp_${org_id}.campaign_utm
                    where campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                        and id = $(echo ${utm_detail} | cut -d _ -f 2)
                    ) b
                    on a.page_url = b.page_url

                    left join cdp_web_${db_id}.fpc_unique c
                    on a.fpc = c.fpc
                where a.created_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and a.fpc in 
                        (
                        select distinct fpc
                        from cdp_${org_id}.user_utm a, 
                            cdp_web_${db_id}.fpc_unique b
                        where a.channel_id = b.id
                            and a.created_at >= convert(replace('$(echo ${utm_detail} | cut -d _ -f 3)', '-', ''), unsigned) 
                            and a.created_at <= convert(if('${vDate}' <= replace('$(echo ${utm_detail} | cut -d _ -f 4)', '-', ''), '${vDate}', replace('$(echo ${utm_detail} | cut -d _ -f 4)', '-', '')), unsigned)
                            and a.campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                            and a.utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                            and a.db_id = ${db_id}
                            and a.channel_type = 1
                        )    
                ;"
            #### Export Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.txt]
            echo $sql_4
            mysql --login-path=${src_login_path} -e "$sql_4" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.error
            #echo $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.txt
            tail $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.txt

            #### Import Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_page_src_d.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src_d IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_page_src_d.error


            export sql_5="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    '$(echo ${utm_detail} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${utm_detail} | cut -d _ -f 2)' utm_id,
                    fpc, 
                    domain, 
                    'event' behavior, 
                    null traffic_type, 
                    null referrer,
                    null page_title,
		    null page_url, 
                    type event_type,
                    0 on_utm,
                    if(b.created_at >= UNIX_TIMESTAMP('${vDate}') and b.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day), 1, 0) is_new,
                    FROM_UNIXTIME(a.created_at) created_at,
                    now() updated_at
                from cdp_web_${db_id}.fpc_event_raw_data a
                    left join cdp_web_${db_id}.fpc_unique b
                    on a.fpc_unique_id = b.id
                where a.created_at >= UNIX_TIMESTAMP('${vDate}')
                   and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and fpc_unique_id in 
                        (
                        select distinct channel_id
                        from cdp_${org_id}.user_utm a
                        where a.created_at >= convert(replace('$(echo ${utm_detail} | cut -d _ -f 3)', '-', ''), unsigned) 
                            and a.created_at <= convert(if('${vDate}' <= replace('$(echo ${utm_detail} | cut -d _ -f 4)', '-', ''), '${vDate}', replace('$(echo ${utm_detail} | cut -d _ -f 4)', '-', '')), unsigned)
                            and a.campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                            and a.utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                            and a.db_id = ${db_id}
                            and a.channel_type = 1
                        )
                ;"
            #### Export Data ####
            echo ''
            echo [exporting data to ${project_name}.${table_name}_${org_id}_src_d.txt]
            echo $sql_5            
            mysql --login-path=${src_login_path} -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_event_src_d.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_event_src_d_sql5.error
        
            #### Import Data ####
            echo ''
            echo [import data from ${project_name}.${table_name}_${org_id}_src_d.txt to ${project_name}.${type}_${table_name}_${org_id}_src_d]
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_event_src_d.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src_d IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_event_src_d_sql5.error
    
        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    
    export sql_6="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                campaign_id, 
                utm_id, 
                fpc, 
                domain, 
                behavior, 
                traffic_type, 
                referrer, 
                page_title,
		page_url, 
                event_type, 
                on_utm, 
                is_new,
                concat('${vDate}', LPAD(session_break, 2 ,'0')) session,
                created_at, 
                now() updated_at
            from (
                select c.*, 
                    if(@campaign_id = campaign_id, 
                        if(@utm_id = utm_id, 
                            if(@fpc = fpc, 
                                if(session_pre = 1, @session_pre := @session_pre + 1, @session_pre), 
                                @session_pre := 1), 
                            @session_pre := 1), 
                        @session_pre := 1) session_break, 
                    @campaign_id := campaign_id campaign_v, 
                    @utm_id := utm_id utm_v, 
                    @fpc := fpc fpc_v
                from (
                    select a.*, if(timestampdiff(minute, b.created_at, a.created_at) >= 30, 1, 0) session_pre
                    from (
                        select *, row_number () over (partition by campaign_id, utm_id, fpc order by created_at) rid
                        from ${project_name}.${type}_${table_name}_${org_id}_src_d
                        ) a
                        
                        left join
                        (
                        select *, row_number () over (partition by campaign_id, utm_id, fpc order by created_at) rid
                        from ${project_name}.${type}_${table_name}_${org_id}_src_d
                        ) b
                        on a.campaign_id = b.campaign_id
                            and a.utm_id = b.utm_id
                            and a.fpc = b.fpc
                            and a.rid = b.rid + 1
                    ) c, 
                    (select @session_pre := 1, @campaign_id, @utm_id, @fpc) d
                ) e
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_src_sql6.error


    export sql_7="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl
            select 
                null serial, 
                campaign_id, 
                utm_id, 
                fpc, 
                domain, 
                behavior, 
                traffic_type, 
                substring_index(substring_index(referrer, '://', -1), '/', 1) referrer, 
                page_title,
		page_url, 
                event_type, 
                on_utm, 
                is_new,
                concat('${vDate}', LPAD(session_break, 2 ,'0')) session,
                null session_type,
                created_at, 
                now() updated_at
            from (
                select c.*, 
                    if(@campaign_id = campaign_id, 
                        if(@utm_id = utm_id, 
                            if(@fpc = fpc, 
                                if(session_pre = 1, @session_pre := @session_pre + 1, @session_pre), 
                                @session_pre := 1), 
                            @session_pre := 1), 
                        @session_pre := 1) session_break, 
                    @campaign_id := campaign_id campaign_v, 
                    @utm_id := utm_id utm_v, 
                    @fpc := fpc fpc_v
                from (
                    select a.*, if(timestampdiff(minute, b.created_at, a.created_at) >= 30, 1, 0) session_pre
                    from (
                        select 
                            campaign_id, 
                            utm_id, 
                            ifnull(g.fpc, s.fpc) fpc, 
                            s.domain, 
                            behavior, 
                            traffic_type, 
                            referrer, 
                            page_title,
			    page_url, 
                            event_type, 
                            on_utm, 
                            is_new, 
                            created_at,
                            row_number () over (partition by campaign_id, utm_id, ifnull(g.fpc, s.fpc) order by created_at) rid
                        from ${project_name}.${type}_${table_name}_${org_id}_src_d s
                            left join uuid.cdp_fpc_mapping g
                            on s.fpc = g.origin_fpc and g.domain = substring_index(s.domain, '/', 1)
                        ) a
                        
                        left join
                        (
                        select 
                            campaign_id, 
                            utm_id, 
                            ifnull(g.fpc, s.fpc) fpc, 
                            s.domain, 
                            behavior, 
                            traffic_type, 
                            referrer, 
                            page_title,
			    page_url, 
                            event_type, 
                            on_utm, 
                            is_new, 
                            created_at,
                            row_number () over (partition by campaign_id, utm_id, ifnull(g.fpc, s.fpc) order by created_at) rid
                        from ${project_name}.${type}_${table_name}_${org_id}_src_d s
                            left join uuid.cdp_fpc_mapping g
                            on s.fpc = g.origin_fpc and g.domain = substring_index(s.domain, '/', 1)
                        ) b
                        on a.campaign_id = b.campaign_id
                            and a.utm_id = b.utm_id
                            and a.fpc = b.fpc
                            and a.rid = b.rid + 1
                    ) c, 
                    (select @session_pre := 1, @campaign_id, @utm_id, @fpc) d
                ) e
        ;
        DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_d
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_d]
    echo $sql_7
    mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_src_sql7.error

    export sql_8="
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
    echo $sql_8
    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl_sql8.error


    export sql_9="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_log
            select 
                null serial, 
                campaign_id, 
                utm_id, 
                fpc, 
                domain, 
                behavior, 
                traffic_type, 
                referrer, 
                page_title,
		page_url, 
                event_type, 
                on_utm, 
                is_new, 
                session, 
                session_type,
                created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_etl
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src_log]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl_log.error
                
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`
