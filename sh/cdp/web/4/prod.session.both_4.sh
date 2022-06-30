#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type="session"
export table_name="both" 
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



while read org_id; 
do 
    echo ''
    echo [TRUNCATE etl table beforehand]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_c;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src;"   

    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
            and domain NOT REGEXP 'deprecated'
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    export sql_2="    
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer', 
            campaign varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告活動', 
            source_medium varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告方式: source x medium', 
    	    event_type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼',  
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
    	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            updated_at timestamp not null default current_timestamp on update current_timestamp,
            primary key (serial),
            key idx_fpc (fpc),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ;" 
    echo ''
    echo [create table if not exists ${project_name}.${type}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error


    export sql_3="    
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
    mysql --login-path=$dest_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt


    export sql_4="    
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
    mysql --login-path=$dest_login_path -e "$sql_4" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt

    while read db_id; 
    do 
        while read referrer_detail; 
        do 
            export sql_5="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    fpc, 
                    from_unixtime(created_at) created_at, 
                    source domain,
                    'page_view' behavior,  
                    if(page_url REGEXP concat('utm', '_'), 'Ad', null) traffic_type,
                    case 
                        when referrer like '%${referrer_detail}%' then '${referrer_detail}' 
                        else referrer
                    end referrer,
                    if(page_url like '%utm_source=%utm_medium=%utm_campaign=%utm_term=%utm_content=%', 
                        case when substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1) is null then 'undefined'
                             when substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1) = '' then 'undefined'
                        else substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1)
                        end,
                        null
                    ) campaign,
                    if(page_url like '%utm_source=%utm_medium=%utm_campaign=%utm_term=%utm_content=%', 
                        concat_ws(' x ', 
                            substring_index(substring_index(page_url, 'utm_source=', -1), '&', 1), 
                            substring_index(substring_index(page_url, 'utm_medium=', -1), '&', 1)
                            ), 
                        null
                    ) source_medium,
                    null event_type,  
                    page_title, 
                    page_url,
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data
                where created_at >= 
UNIX_TIMESTAMP('${vDate}') 
                    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and referrer like '%${referrer_detail}%'
                ;"
                echo $sql_5
                #### Export Data ####
                echo ''
                echo [start: date on ${vDate}]
                echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_src.txt]
                mysql --login-path=${src_login_path} -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d / -f 1)_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d / -f 1)_src.error
           
                #### Import Data ####
                echo ''
                echo [start: date on ${vDate}]
                echo [truncate table ${project_name}.${type}_${table_name}_${org_id}_src]
                mysql --login-path=$dest_login_path -e "delete from ${project_name}.${type}_${table_name}_${org_id}_src where created_at < '${vDate}';"
                echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
                mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_${referrer_detail}_src.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_${referrer_detail}_src.error 
                echo notice: ${project_name}.${table_name}_${org_id}_src' is disposable data, 1 day data for once.'

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt

        while read NOTreferrer_detail; 
        do 
            export sql_6="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    fpc, 
                    from_unixtime(created_at) created_at, 
                    source domain, 
                    'page_view' behavior, 
                    if(page_url REGEXP concat('utm', '_'), 'Ad', null) traffic_type,
                    referrer,
                    if(page_url like '%utm_source=%utm_medium=%utm_campaign=%utm_term=%utm_content=%', 
                        case when substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1) is null then 'undefined'
                             when substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1) = '' then 'undefined'
                        else substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1)
                        end,
                        null
                    ) campaign,
                    if(page_url like '%utm_source=%utm_medium=%utm_campaign=%utm_term=%utm_content=%', 
                        concat_ws(' x ', 
                            substring_index(substring_index(page_url, 'utm_source=', -1), '&', 1), 
                            substring_index(substring_index(page_url, 'utm_medium=', -1), '&', 1)
                            ), 
                        null
                    ) source_medium,
                    null event_type,  
                    page_title, 
                    page_url,
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data
                where created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and referrer NOT REGEXP '^(${NOTreferrer_detail})$'
                ;"
                echo $sql_6
                #### Export Data ####
                echo ''
                echo [start: date on ${vDate}]
                echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_src.txt]
                mysql --login-path=${src_login_path} -e "$sql_6" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.error
           
                #### Import Data ####
                echo ''
                echo [start: date on ${vDate}]
                echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
                mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.error 
                echo [notice: ${project_name}.${table_name}_${org_id}_src is disposable data, 1 day data for once.]

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt


        export sql_9="
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                fpc, 
                from_unixtime(a.created_at) created_at, 
                domain,
                'event' behavior,  
                null traffic_type, 
                null referrer, 
                null campaign, 
                null source_medium, 
                type event_type, 
                null page_title,        
                null page_url, 
                now() updated_at
            from cdp_web_${db_id}.fpc_event_raw_data a, 
                cdp_web_${db_id}.fpc_unique b
            where a.fpc_unique_id = b.id
                and a.created_at >= UNIX_TIMESTAMP('${vDate}') 
                and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
            ;"
        echo $sql_9
        #### Export Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_logic_src.txt]
        mysql --login-path=${src_login_path} -e "$sql_9" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_logic_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_logic_src.error
    
        #### Import Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_logic_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_logic_src.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_logic_src.error 
        echo [notice: ${project_name}.${table_name}_${org_id}_src is disposable data, 1 day data for once.]

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ##################################
   
    export sql_11="  
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer', 
            campaign varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告活動', 
            source_medium varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告方式: source x medium',
    	    event_type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
    	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            rid int NOT NULL COMMENT '依照各人各 session 中的時間先後排序', 
            updated_at timestamp not null default current_timestamp on update current_timestamp,
            primary key (serial),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_fpc (fpc), 
            key idx_rid (rid),
            key idx_fpc_rid (fpc, rid)      
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的 etl 整合表（加入 accu_id)'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer', 
            campaign varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告活動', 
            source_medium varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告方式: source x medium',
    	    event_type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
    	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',            
            session_pre int NOT NULL COMMENT '判斷是否與前一個時間戳超過 30 分鐘的 flag', 
            updated_at timestamp not null default current_timestamp on update current_timestamp,
            primary key (serial),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的 etl 整合表（加入 accu_id)'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_c (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer', 
            campaign varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告活動', 
            source_medium varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告方式: source x medium',
    	    event_type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
    	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            session_type varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in/valid',
            updated_at timestamp not null default current_timestamp on update current_timestamp,
            primary key (serial),
            key idx_fpc (fpc), 
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_session (session), 
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的 etl 整合表'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer', 
            campaign varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告活動', 
            source_medium varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'UTM (Urchin Tracking Module) 廣告方式: source x medium',
    	    event_type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
    	    page_url varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '進站 URL',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            session_type varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in/valid',
            updated_at timestamp not null default current_timestamp on update current_timestamp,
            primary key (serial),
            key idx_created_at (created_at), 
            key idx_fpc (fpc),
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_session (session), 
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的 etl 整合表'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_session (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            session int NOT NULL COMMENT 'session/工作階段',
            updated_at timestamp not null default current_timestamp on update current_timestamp,
            primary key (serial),
            key idx_created_at (created_at), 
            key idx_fpc (fpc),
            key idx_session (session)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='紀錄各 accu_id 的最後時間戳'
        ;"
    echo ''
    echo [start: `date` on ${vDate}]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_new and _etl]
    echo $sql_11    
    mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql11.error

    export sql_11z="
        DELETE
        FROM ${project_name}.${type}_${table_name}_${org_id}_src
        WHERE traffic_type = 'Organic'
            and referrer NOT REGEXP 'google|yahoo|bing|MSN'
        ;"
    echo ''
    echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_11z
    mysql --login-path=$dest_login_path -e "$sql_11z" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql11z.error

    export sql_12="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src a
            INNER JOIN uuid.accu_mapping_${org_id} b
            ON a.fpc = b.id
        SET a.fpc = accu_id
        WHERE id_type = 'fpc'
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_12
    mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql12.error


    export sql_13="    
        select fpc
        from ${project_name}.${type}_${table_name}_${org_id}_src
        group by fpc
        ;"
    echo ''
    echo [Get the accu_id list]
    echo $sql_13
    mysql --login-path=$dest_login_path -e "$sql_13" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_fpc_list.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_fpc_list.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_fpc_list.txt

#    while read fpc_list; 
#    do 
        export sql_14ba="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_a    
                select 
                    null serial, 
                    fpc,
                    s.created_at, 
                    s.domain, 
                    s.behavior, 
                    s.traffic_type, 
                    s.referrer, 
                    s.campaign, 
                    s.source_medium, 
                    s.event_type, 
                    s.page_title, 
                    s.page_url, 
                    row_number () over (partition by (s.fpc) order by created_at) rid, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_src s
                #where fpc = '${fpc_list}'
            ;"
        echo ''
        echo [start: `date` on ${vDate}]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl_a]
        echo $sql_14ba
        mysql --login-path=$dest_login_path -e "$sql_14ba" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14ba.error

        export sql_14bb1="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_b  
                select 
                    null serial, 
                    a.fpc,
                    a.created_at, 
                    a.domain, 
                    a.behavior, 
                    a.traffic_type, 
                    a.referrer, 
                    a.campaign, 
                    a.source_medium, 
                    a.event_type, 
                    a.page_title, 
                    a.page_url,
                    0 session_pre,
                    now() updated_at                
                from ${project_name}.${type}_${table_name}_${org_id}_etl_a a    
                where rid = 1
            ;"
        echo ''
        echo [start: `date` on ${vDate}]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl_b]
        echo $sql_14bb1
        mysql --login-path=$dest_login_path -e "$sql_14bb1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14bb1.error
        
        export sql_14bb2="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_b  
                select 
                    null serial,
                    a.fpc,
                    a.created_at, 
                    a.domain, 
                    a.behavior, 
                    a.traffic_type, 
                    a.referrer, 
                    a.campaign, 
                    a.source_medium, 
                    a.event_type, 
                    a.page_title, 
                    a.page_url,
                    if(timestampdiff(minute, b.created_at, a.created_at) >= 30, 1, 0) session_pre,
                    now() updated_at                
                from ${project_name}.${type}_${table_name}_${org_id}_etl_a a
                    inner join ${project_name}.${type}_${table_name}_${org_id}_etl_a b
                    on a.fpc = b.fpc and a.rid = b.rid + 1
                #where a.fpc = '${fpc_list}'
            ;"
        echo ''
        echo [start: `date` on ${vDate}]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl_b]
        echo $sql_14bb2
        mysql --login-path=$dest_login_path -e "$sql_14bb2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14bb2.error


        export sql_14bc="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_c
                select 
                    null serial, 
                    fpc, 
                    created_at, 
                    domain, 
                    behavior, 
                    traffic_type, 
                    ifnull(output, e.referrer) referrer,
                    campaign, 
                    source_medium,
                    event_type,  
                    page_title, 
                    page_url, 
                    concat(date_format(created_at, '%Y%m%d'), LPAD(session_break, 2 ,'0')) session, 
                    null session_type,
                    now() updated_at
                from (
                    select 
                        fpc, 
                        created_at, 
                        domain, 
                        behavior, 
                        session_pre, 
                        case 
                            when traffic_type = 'Ad' then 'Ad'
                            when referrer is null or referrer = '' then 'Direct'
                            when referrer REGEXP 'google\\.|yahoo\\.|bing\\.|MSN\\.' then 'Organic'
                            else 'Others'
                        end traffic_type, 
                        referrer,
                        if(traffic_type = 'Ad' and campaign is null, 'Others', campaign) campaign,
                        if(traffic_type = 'Ad' and source_medium is null, 'Others', source_medium) source_medium,
                        event_type, 
                        page_title, 
                        page_url, 
                        IF(@fpc = fpc, 
                            IF(session_pre = 1, @session_pre := @session_pre + 1, @session_pre), 
                                @session_pre := 1) session_break, 
                        @fpc := fpc
                    from ${project_name}.${type}_${table_name}_${org_id}_etl_b c,
                        (select @session_pre := 1, @fpc) d
                    #where c.fpc = '${fpc_list}'
                    ) e
    
                    left join
                    (
                    select string referrer, output
                    from codebook_cdp.referrer_detail
                    ) f
                    on e.referrer like concat('%', f.referrer, '%')
            ;"
        echo ''
        echo [start: `date` on ${vDate}]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl_c]
        echo $sql_14bc
        mysql --login-path=$dest_login_path -e "$sql_14bc" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14bc.error
   

        export sql_14bd="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl
                select 
                    null serial, 
                    fpc, 
                    created_at, 
                    domain, 
                    behavior, 
                    traffic_type, 
                    referrer,
                    campaign, 
                    source_medium,
                    event_type,  
                    page_title, 
                    page_url, 
                    session, 
                    null session_type,
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}_etl_c
                #where fpc = '${fpc_list}' 
                group by
                    fpc, 
                    created_at, 
                    domain, 
                    behavior, 
                    traffic_type, 
                    referrer,
                    campaign, 
                    source_medium,
                    event_type,  
                    page_title, 
                    page_url, 
                    session
            ;"
        echo ''
        echo [start: `date` on ${vDate}]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl]
        echo $sql_14bd
        mysql --login-path=$dest_login_path -e "$sql_14bd" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14bd.error

    #done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_fpc_list.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_4.txt

echo ''
echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_4.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_4.txt
