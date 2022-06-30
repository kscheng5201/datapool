echo `date`
#!/usr/bin/bash
####################################################
# Project: 互動分析儀表板儀表板
# Branch: session_both 大表產製
# Author: Benson Cheng
# Created_at: 2022-03-08
# Updated_at: 2022-03-08
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
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_c]
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]  
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_a;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_b;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_etl_c;"
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src;"   

    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
            and deleted_at is null
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    export sql_2="    
        create table if not exists ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            id int NOT NULL COMMENT '原始 id', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的時間戳記', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
            traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量', 
            referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer', 
            referrer_origin varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '未分類 referrer',
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
                    id,
                    fpc, 
                    from_unixtime(created_at) created_at, 
                    source domain,
                    'page_view' behavior,  
                    if(page_url REGEXP concat('utm', '_'), 'Ad', null) traffic_type,
                    case 
                        when referrer like '%${referrer_detail}%' then '${referrer_detail}' 
                        else referrer
                    end referrer,
                    referrer referrer_origin, 
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
                where created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and referrer like '%${referrer_detail}%'
                ;"
                echo $sql_5
                #### Export Data ####
                echo ''
                echo [start: `date` on ${vDate}]
                echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_src.txt]
                mysql --login-path=${src_login_path} -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d / -f 1)_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_$(echo ${referrer_detail} | cut -d / -f 1)_src.error
           
                #### Import Data ####
                echo ''
                echo [start: `date` on ${vDate}]
                echo [truncate table ${project_name}.${type}_${table_name}_${org_id}_src]
                #mysql --login-path=$dest_login_path -e "delete from ${project_name}.${type}_${table_name}_${org_id}_src where created_at < '${vDate}';"
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
                    id, 
                    fpc, 
                    from_unixtime(created_at) created_at, 
                    source domain, 
                    'page_view' behavior, 
                    if(page_url REGEXP concat('utm', '_'), 'Ad', null) traffic_type,
                    referrer,
                    referrer referrer_origin, 
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
                mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_NOTreferrer_detail_src.error 
                echo [notice: ${project_name}.${table_name}_${org_id}_src is disposable data, 1 day data for once.]

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_NOTreferrer_detail.txt


        export sql_9="
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                id, 
                fpc, 
                from_unixtime(a.created_at) created_at, 
                domain,
                'event' behavior,  
                null traffic_type, 
                null referrer, 
                null referrer_origin, 
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

    export sql_u="
        UPDATE ${project_name}.${type}_${table_name}_${org_id}_src
        SET traffic_type = 
                if(referrer is null or referrer = '', 'Direct', 
                    if(referrer_origin REGEXP 'google\.|yahoo\.|bing\.|MSN\.', 'Organic', 
                        'Others')
                        )
        WHERE traffic_type = 'NULL'
            and behavior = 'page_view'
        ;"
    echo ''
    echo [ UPDATE ${project_name}.${type}_${table_name}_${org_id}_src ]
    echo $sql_u
    mysql --login-path=$dest_login_path -e "$sql_u" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src_sql_u.error
        


done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo `date`

