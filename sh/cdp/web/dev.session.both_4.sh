echo `date`
#!/usr/bin/bash
####################################################
# Project: 互動分析儀表板儀表板
# Branch: session_both 大表產製
# Author: Benson Cheng
# Created_at: 2022-03-21
# Updated_at: 2022-03-21
# Note: 
#####################################################

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



while read org_id; 
do 
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src] 
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src;"   
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b;"  

    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
            and deleted_at is null
    	;"
    echo ''
    echo [Get the db_id on web]
    echo $sql_1
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    export sql_2="    
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_src (
            id int NOT NULL COMMENT '原始 id', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'accu_id',
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
            primary key (id, domain, behavior),
            key idx_fpc (fpc),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ;" 
    echo ''
    echo [create table if not exists ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error


    export sql_3="    
        select concat_ws('=', string, output)
        from web.referrer
        order by serial
        ;"
    echo ''
    echo [Get the referrer_detail for ${org_id}]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt
    echo ''
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt

    export sql_4="    
        select group_concat(string separator '|')
        from web.referrer
        ;"
    echo ''
    echo [Get the NOT referrer_detail for ${org_id}]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt
    echo ''
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt


    while read db_id; 
    do 
        while read referrer_detail; 
        do 
            export sql_5="
                SET NAMES utf8mb4
                ;
                select 
                    id,
                    fpc, 
                    null accu_id, 
                    from_unixtime(created_at) created_at, 
                    source domain,
                    'page_view' behavior,  
                    if(page_url REGEXP concat('utm', '_'), 'Ad', null) traffic_type,
                    if(referrer like concat('%', '$(echo ${referrer_detail} | cut -d = -f 1)', '.', '%'),
                        '$(echo ${referrer_detail} | cut -d = -f 2)', referrer) referrer,
                    null campaign,
                    null source_medium,
                    null event_type,  
                    page_title, 
                    page_url,
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data
                where created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and referrer REGEXP '$(echo ${referrer_detail} | cut -d = -f 1)'
                ;"
            echo $sql_5
            #### Export Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_src.txt]
            mysql --login-path=${src_login_path} -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d = -f 1)_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d = -f 1)_src.error
       
            #### Import Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d = -f 1)_src.txt' IGNORE INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d = -f 1)_src.error 

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_referrer_detail.txt

        while read NOTreferrer_detail; 
        do 
            export sql_6="
                SET NAMES utf8mb4
                ;
                select 
                    id, 
                    fpc, 
                    null accu_id, 
                    from_unixtime(created_at) created_at, 
                    source domain, 
                    'page_view' behavior, 
                    if(page_url REGEXP concat('utm', '_'), 'Ad', null) traffic_type,
                    substring_index(substring_index(referrer, '://', -1), '/', 1) referrer,
                    null campaign, 
                    null source_medium,
                    null event_type,  
                    page_title, 
                    page_url,
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data
                where created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and referrer NOT REGEXP '${NOTreferrer_detail}'
                ;"
            echo $sql_6
            #### Export Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_src.txt]
            mysql --login-path=${src_login_path} -e "$sql_6" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.error
       
            #### Import Data ####
            echo ''
            echo [start: `date` on ${vDate}]
            echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.txt' IGNORE INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.error 

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt

        export sql_9="
            SET NAMES utf8mb4
            ;
            select 
                a.id, 
                fpc, 
                null accu_id, 
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
            where a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                and a.created_at >= UNIX_TIMESTAMP('${vDate}') 
                and a.fpc_unique_id = b.id
            ;"
        echo $sql_9
        #### Export Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_event_src.txt]
        mysql --login-path=${src_login_path} -e "$sql_9" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_event_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_event_src.error
    
        #### Import Data ####
        echo ''
        echo [start: `date` on ${vDate}]
        echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_event_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_event_src.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_event_src.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt


    export sql_up1="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src
        SET traffic_type = 'Direct'
        WHERE behavior = 'page_view'
            and (referrer is null 
             or referrer = '')
        ;"
    echo ''
    echo [ UPDATE ${project_name}.${type}_${table_name}_${org_id}_src ]
    echo $sql_up1
    mysql --login-path=$dest_login_path -e "$sql_up1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src_sql_up1.error

    export sql_up2="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src a
            INNER JOIN web.referrer b
            ON a.referrer = b.output
        SET a.traffic_type = b.traffic_type
        WHERE behavior = 'page_view'
            and a.traffic_type <> 'Ad'
        ;"
    echo ''
    echo [ UPDATE ${project_name}.${type}_${table_name}_${org_id}_src ]
    echo $sql_up2
    mysql --login-path=$dest_login_path -e "$sql_up2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src_sql_up2.error

    export sql_up3="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src
        SET traffic_type = 'Others'
        WHERE behavior = 'page_view'
            and traffic_type = 'NULL'
        ;"
    echo ''
    echo [ UPDATE ${project_name}.${type}_${table_name}_${org_id}_src ]
    echo $sql_up3
    mysql --login-path=$dest_login_path -e "$sql_up3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src_sql_up3.error

    export sql_up4="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src
        SET campaign = if(page_url like '%utm_source=%utm_medium=%utm_campaign=%utm_term=%utm_content=%', 
                substring_index(substring_index(page_url, 'utm_campaign=', -1), '&', 1), '非愛酷格式'), 
            source_medium = if(page_url like '%utm_source=%utm_medium=%utm_campaign=%utm_term=%utm_content=%',
                concat_ws(' x ', 
                    substring_index(substring_index(page_url, 'utm_source=', -1), '&', 1), 
                    substring_index(substring_index(page_url, 'utm_medium=', -1), '&', 1)
                    ), '非愛酷格式')
        WHERE behavior = 'page_view'
            and traffic_type = 'Ad'
        ;"
    echo ''
    echo [ UPDATE ${project_name}.${type}_${table_name}_${org_id}_src ]
    echo $sql_up4
    mysql --login-path=$dest_login_path -e "$sql_up4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src_sql_up4.error


    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ##################################

    export sql_11="  
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a (
            id int NOT NULL COMMENT '原始 id', 
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'accu_id',
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
            primary key (id, domain, behavior),
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
            id int NOT NULL COMMENT '原始 id',
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'accu_id',
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
            primary key (id, domain, behavior),
            key idx_created_at (created_at), 
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的 etl 整合表（加入 accu_id)'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl (
            id int NOT NULL COMMENT '原始 id',
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'accu_id',
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
            primary key (id, domain, behavior),
            key idx_created_at (created_at), 
            key idx_fpc (fpc),
            key idx_domain (domain), 
            key idx_traffic_type (traffic_type),
            key idx_referrer (referrer), 
            key idx_session (session), 
            key idx_session_type (session_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的 etl 整合表'
        ;"
    echo ''
    echo [start: `date` on ${vDate}]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_new and _etl]
    echo $sql_11    
    mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql11.error


    export sql_12="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src a
            INNER JOIN uuid.accu_mapping_${org_id} b
            ON a.fpc = b.id
        SET a.accu_id = b.accu_id
        WHERE id_type = 'fpc'
        ;
        
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src
        SET accu_id = fpc
        WHERE accu_id = 'NULL'
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_12
    mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql12.error


    export sql_14ba="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_a    
            select 
                id, 
                accu_id, 
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
                row_number () over (partition by accu_id order by created_at) rid, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_src
        ;"
    echo ''
    echo [start: `date` on ${vDate}]
    echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl_a]
    echo $sql_14ba
    mysql --login-path=$dest_login_path -e "$sql_14ba" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14ba.error

    export sql_14bb1="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl_b  
            select 
                id, 
                accu_id, 
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
                a.id,
                a.accu_id, 
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
                on a.accu_id = b.accu_id and a.rid = b.rid + 1
        ;"
    echo ''
    echo [start: `date` on ${vDate}]
    echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl_b]
    echo $sql_14bb2
    mysql --login-path=$dest_login_path -e "$sql_14bb2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14bb2.error


    export sql_14bc="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_etl
            select 
                id, 
                accu_id, 
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
                concat(date_format(created_at, '%Y%m%d'), LPAD(session_break, 2 ,'0')) session, 
                null session_type,
                now() updated_at
            from (
                select 
                    id, 
                    accu_id,
                    fpc, 
                    created_at, 
                    domain, 
                    behavior, 
                    session_pre, 
                    traffic_type, 
                    referrer,
                    campaign,
                    source_medium,
                    event_type, 
                    page_title, 
                    page_url, 
                    IF(@accu_id = accu_id, 
                        IF(session_pre = 1, @session_pre := @session_pre + 1, @session_pre), 
                            @session_pre := 1) session_break, 
                    @accu_id := accu_id
                from ${project_name}.${type}_${table_name}_${org_id}_etl_b c,
                    (select @session_pre := 1, @accu_id) d
                ) e
        ;"
    echo ''
    echo [start: `date` on ${vDate}]
    echo [insert into ${project_name}.${type}_${table_name}_${org_id}_etl]
    echo $sql_14bc
    mysql --login-path=$dest_login_path -e "$sql_14bc" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14bc.error


    export sql_23="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_etl a
            INNER JOIN
            (
            select accu_id, session, count(*)
            from ${project_name}.${type}_${table_name}_${org_id}_etl
            group by accu_id, session
            having count(*) >= 2
            ) b
            ON a.accu_id = b.accu_id
                and a.session = b.session
        SET session_type = 'valid'
        ;
        
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_etl
        SET session_type = 'invalid'
        WHERE session_type is null
            or session_type = ''
        ;"
    echo ''
    echo [start: `date` on ${vDate}]
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_etl on session_type]
    echo $sql_23
    mysql --login-path=$dest_login_path -e "$sql_23" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl_sql_23.error
                
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b;"  


    export sql_6x="
        INSERT IGNORE INTO env_config.events_menu
            select 
                null serial, 
                ${org_id} org_id, 
                '${project_name}' channel, 
                a.type event_main_id, 
                1 kind, 
                b.type type, 
                name event, 
                if(b.type in (10, 13), 1, 0) is_funnel,
                now() created_at, 
                now() updated_at
            from (
                select event_type type
                from ${project_name}.${type}_${table_name}_${org_id}_etl
                group by event_type
                ) a, 
                codebook_cdp.events_main b
            where a.type = b.type
        ;
        ALTER TABLE env_config.events_menu AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO env_config.events_menu]
    echo $sql_6x
    mysql --login-path=$dest_login_path -e "$sql_6x" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl_sql_6x.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id_4.txt

echo ''
echo `date`
