echo ''
echo `date`
#!/usr/bin/bash
####################################################
# Project: APP 互動分析儀表板
# Branch: 行銷漏斗上游
# Author: Benson Cheng
# Created_at: 2022-04-13
# Updated_at: 2022-04-13
# Note: 主程式
#####################################################
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type_p="person"
export type_s="session"
export table_name="event" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
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
    vMonthFirst=`date -d $1 +"%Y-%m-01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y-%m-01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi

#### Get Last Date of Season ####
seasonEnd="
`date +"%Y-03-31"`
`date +"%Y-06-30"`
`date +"%Y-09-30"`
`date +"%Y-12-31"`
`date -d "$(date +%Y-01-01) -1 day" +"%Y-%m-%d"`
"



while read org_id; 
do 
    echo [at the end: DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre;"
    echo [at the end: DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl;"

    
    export sql_0="
        DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl (
            id int NOT NULL COMMENT '原始 id', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'accu_id',
            domain varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',
            type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '事件名稱',
            col1 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col2 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col3 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col4 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col5 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col6 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col7 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col8 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col9 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col10 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL, 
            col11 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL, 
            col12 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col13 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            col14 varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            session varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'session/工作階段', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間', 
            PRIMARY KEY (id, domain) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE,
            KEY idx_type (type) USING BTREE,
            KEY idx_fpc (fpc) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc_event_raw_data 全表複製' 
        ;" 
    echo ''
    echo [create table if not exists ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    echo $sql_0
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl_sql_0.error
    
    
    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
            and domain NOT REGEXP 'deprecated'
    	;"
    echo ''
    echo [Get the db_id on ${project_name}]
    mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt


    while read db_id; 
    do
        export sql_3="    
            SET NAMES utf8mb4
            ;
            select 
                a.id, 
                fpc, 
                null accu_id, 
                domain, 
                a.type, 
                name event, 
                col1, 
                col2, 
                col3, 
                col4, 
                col5, 
                col6, 
                col7, 
                col8, 
                col9, 
                col10, 
                col11,
                col12,
                col13,
                col14,
                null session,
                from_unixtime(a.created_at) created_at, 
                now() updated_at
            from cdp_${project_name}_${db_id}.fpc_event_raw_data a, 
                cdp_${project_name}_${db_id}.fpc_unique b, 
                cdp_organization.events_main c
            where a.fpc_unique_id = b.id
                and a.type = c.type
                and a.created_at >= UNIX_TIMESTAMP('${vDate}')
                and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
            ;" 
    
        echo $sql_3
        #### Export Data ####
        echo ''
        echo [start: date on ${vDate}]
        echo [exporting data to ${project_name}.${type_s}_${table_name}_${org_id}_etl.txt]
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_etl.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_etl.error
    
        #### Import Data ####
        echo ''
        echo [start: date on ${vDate}]
        echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}_etl]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_etl.txt' INTO TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_etl.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    

    export sql_up="
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id}_etl a
            INNER JOIN ${project_name}.${type_s}_both_${org_id}_etl b
            ON a.id = b.id and a.domain = b.domain
        SET a.session = b.session, 
            a.accu_id = b.accu_id
        WHERE b.behavior = 'event'
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "$sql_up" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl_sql_up.error


    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ###### Making the PRE table ######
    ##################################

    export sql_4="   
        DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',
    	    type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此', 
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段', 
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (accu_id, domain, event, attribute, content, created_at, session) USING BTREE, 
            KEY idx_accu_id (accu_id) USING BTREE,
            KEY idx_domain (domain) USING BTREE,
            KEY idx_type (type) USING BTREE,
            KEY idx_attribute (attribute) USING BTREE,
            KEY idx_content (content) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='event pre-etl data' 
        ;"
    echo ''
    echo $sql_4
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_pre.error


    export sql_6="  
        select concat_ws('-', type, column_location)
        from env_config.events_analysis_base
        where column_location is not null
            and column_location <> ''
        ;"
    echo ''
    echo [Get the column_index on ${project_name}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt
    echo ''
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt


    while read column_index; 
    do 
        export sql_7="            
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_pre
                select 
                    null serial, 
                    accu_id,
                    domain,
                    $(echo ${column_index} | cut -d - -f 1) type, 
                    event, 
                    (select $(echo ${column_index} | cut -d - -f 2) from codebook_cdp.events_function where type = $(echo ${column_index} | cut -d - -f 1)) attribute, 
                    $(echo ${column_index} | cut -d - -f 2) content, 
                    session,
                    created_at, 
                    now() updated_at
                from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                where type = $(echo ${column_index} | cut -d - -f 1)
                group by 
                    accu_id, 
                    domain,
                    event, 
                    $(echo ${column_index} | cut -d - -f 2), 
                    session,
                    created_at
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_pre]
        echo $sql_7
        mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_mid.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt

   
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ####### Start the Analysis #######
    ##################################

    export sql_10="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            freq int DEFAULT NULL COMMENT '事件-屬性的數量',  
            ranking int DEFAULT NULL COMMENT '依照事件-屬性的數量所做的排名', 
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain, event, attribute, content),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_domain (domain), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【事件深度分析】流量相關各項資料'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            user int DEFAULT NULL COMMENT '事件-人數的數量',  
            ranking int DEFAULT NULL COMMENT '依照事件-人數所做的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain, event, attribute, content),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_span (span), 
            key idx_domain (domain), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【事件深度分析】人數相關各項資料'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id}_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日',
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日', 
            accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id', 
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, accu_id, domain, event, attribute, content),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_accu_id (accu_id), 
            key idx_domain (domain), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='event 保留 accu_id 原始表'
        ;"
    echo ''
    echo [start: date on ${vDate}]
    echo [create table if not exists ${project_name}.${type_s}/${type_p}_${table_name}_${org_id}/_fpc]
    echo $sql_10
    mysql --login-path=$dest_login_path -e "$sql_10" 2>>$error_dir/$src_login_path/$project_name/${project_name}.create_table_sql_10.error


    export sql_11a="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                domain,  
                event, 
                attribute, 
                content, 
                count(distinct accu_id, session) freq,
                row_number () over (partition by domain, event, attribute order by count(distinct accu_id, session) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from ${project_name}.${type_s}_${table_name}_${org_id}_pre
            group by 
                domain,  
                event, 
                attribute, 
                content 
        ; 
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ; 

        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                'ALL' domain,  
                event, 
                attribute, 
                content, 
                count(distinct accu_id, session) freq,
                row_number () over (partition by event, attribute order by count(distinct accu_id, session) desc) ranking, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from ${project_name}.${type_s}_${table_name}_${org_id}_pre
            group by 
                event, 
                attribute, 
                content 
        ; 
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [start: date on ${vDate}]
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}]
    echo $sql_11a
    mysql --login-path=$dest_login_path -e "$sql_11a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_11a.error


    export sql_11b="
        INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                accu_id,
                domain, 
                event, 
                attribute, 
                content, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type_s}_${table_name}_${org_id}_pre
            group by 
                accu_id,
                domain, 
                event, 
                attribute, 
                content 
        ;
        ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id}_fpc AUTO_INCREMENT = 1
        ;" 
    echo ''
    echo [start: date on ${vDate}]
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_fpc]
    echo $sql_11b
    mysql --login-path=$dest_login_path -e "$sql_11b" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_11b.error

#####################################
#####################################


    if [ ${vDateName} = Sun ];
    then 
        export sql_12="
            INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    sum(freq) freq, 
                    row_number () over (partition by domain, event, attribute order by sum(freq) desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_s}_${table_name}_${org_id}
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                    and content is not null
                    and content <> ''
                group by 
                    domain, 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 
            UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    count(distinct accu_id) person, 
                    row_number () over (partition by domain, event, attribute order by count(distinct accu_id) desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                    and content is not null
                    and content <> ''
                group by 
                    domain, 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date,
                    '${vDate}' end_date, 
                    'ALL' domain, 
                    event, 
                    attribute, 
                    content, 
                    count(distinct accu_id) person, 
                    row_number () over (partition by event, attribute order by count(distinct accu_id) desc) ranking,
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                    and content is not null
                    and content <> ''
                group by 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}]
        echo $sql_12
        mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_12.error    

    else 
        echo [today is ${vDateName}, not Sun. No Need to do the weekly statistics.]
    fi 
  
  
    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_13="
            INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date,
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    sum(freq) freq, 
                    row_number () over (partition by domain, event, attribute order by sum(freq) desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_s}_${table_name}_${org_id}
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                    and content is not null
                    and content <> ''
                group by 
                    domain, 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1  
            ;
            UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date,
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    count(distinct accu_id) person, 
                    row_number () over (partition by domain, event, attribute order by count(distinct accu_id) desc) ranking, 
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                    and content is not null
                    and content <> ''
                group by 
                    domain, 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1  
            ;

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date,
                    'ALL' domain, 
                    event, 
                    attribute, 
                    content, 
                    count(distinct accu_id) person, 
                    row_number () over (partition by event, attribute order by count(distinct accu_id) desc) ranking,
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                    and content is not null
                    and content <> ''
                group by 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [start: date on ${vDate}. ${vDate} = ${vMonthLast}?]
        echo [INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}]
        echo $sql_13
        mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_13.error    

    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi 
    
    for seasonDate in $seasonEnd
    do
        if [ ${vDate} = ${seasonDate} ];
        then 
            export sql_14="
                INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' - interval 3 month + interval 1 day start_date, 
                        '${vDate}' end_date, 
                        domain, 
                        event, 
                        attribute, 
                        content, 
                        sum(freq) freq, 
                        row_number () over (partition by domain, event, attribute order by sum(freq) desc) ranking, 
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_s}_${table_name}_${org_id}
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'daily'
                        and content is not null
                        and content <> ''
                    group by 
                        domain, 
                        event, 
                        attribute, 
                        content
                ; 
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;
                UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;

                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' - interval 3 month + interval 1 day start_date, 
                        '${vDate}' end_date, 
                        domain, 
                        event, 
                        attribute, 
                        content, 
                        count(distinct accu_id) person, 
                        row_number () over (partition by domain, event, attribute order by count(distinct accu_id) desc) ranking,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                        and content is not null
                        and content <> ''
                    group by 
                        domain, 
                        event, 
                        attribute, 
                        content
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;
                
                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' - interval 3 month + interval 1 day start_date, 
                        '${vDate}' end_date, 
                        'ALL' domain, 
                        event, 
                        attribute, 
                        content, 
                        count(distinct accu_id) person, 
                        row_number () over (partition by event, attribute order by count(distinct accu_id) desc) ranking,
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                        and content is not null
                        and content <> ''
                    group by 
                        event, 
                        attribute, 
                        content
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [start: date on ${vDate}. ${vDate} = ${seasonDate}?]
            echo [INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}]
            echo $sql_14
            mysql --login-path=$dest_login_path -e "$sql_14" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_14.error    
        else 
            echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
        fi 
    done 

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_4.txt

echo ''
echo [end the ${vDate} data on `date`]


