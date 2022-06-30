#!/usr/bin/bash
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
"


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
##mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_7.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.error
##sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_7.txt


while read org_id; 
do 
    echo ''
    echo [at beginning: TRUNCATE TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "TRUNCATE TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src;"
    echo [at beginning: TRUNCATE TABLE ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "TRUNCATE TABLE ${project_name}.${type_s}_${table_name}_${org_id}_pre;"
    echo [at beginning: TRUNCATE TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "TRUNCATE TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl;"

    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    while read db_id; 
    do
        export sql_2="    
            CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src (
                serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
                fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
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
                created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間', 
                updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間', 
                PRIMARY KEY (serial) USING BTREE,
                KEY idx_created_at (created_at) USING BTREE,
                KEY idx_type (type) USING BTREE,
                KEY idx_fpc (fpc) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc_event_raw_data 全表複製' 
            ;" 
        echo ''
        echo [create table if not exists ${project_name}.${type_s}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src.error
    
        export sql_3="    
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                fpc, 
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
                from_unixtime(a.created_at, '%Y-%m-%d %H:%m:%s') created_at, 
                now() updated_at
            from cdp_web_${db_id}.fpc_event_raw_data a, 
                cdp_web_${db_id}.fpc_unique b, 
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
        echo [exporting data to ${project_name}.${type_s}_${table_name}_${org_id}_src.txt]
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.error
    
        #### Import Data ####
        echo ''
        echo [start: date on ${vDate}]
        echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.txt' INTO TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ###### Making the ETL table ######
    ##################################

    export sql_4="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',
	    type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此', 
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (fpc, domain, event, attribute, content, created_at) USING BTREE, 
            KEY idx_fpc (fpc) USING BTREE,
            KEY idx_domain (domain) USING BTREE,
            KEY idx_type (type) USING BTREE,
            KEY idx_attribute (attribute) USING BTREE,
            KEY idx_content (content) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='event pre-etl data' 
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_pre.error
  
    export sql_5="
        select type
        from codebook_cdp.events_main
    	;"
    echo ''
    echo [Get the type_id on web]
    mysql --login-path=$dest_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt

    while read type_id; 
    do
        export sql_6="  
            select column_location
            from env_config.events_analysis_base
            where type = ${type_id}
                and column_location is not null
                and column_location <> ''
            ;"
        echo ''
        echo [Get the column_index on web]
        mysql --login-path=$dest_login_path -e "$sql_6" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt
    
        while read column_index; 
        do 
            export sql_7="            
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_pre
                    select 
                        null serial, 
                        fpc,
                        domain,
			type, 
                        event, 
                        (select ${column_index} from codebook_cdp.events_function where type = ${type_id}) attribute, 
                        ${column_index} content, 
                        created_at, 
                        now() updated_at
                    from ${project_name}.${type_s}_${table_name}_${org_id}_src
                    where type = ${type_id}
                    group by 
                        fpc, 
                        domain,
			type,  
                        event, 
                        ${column_index}, 
                        created_at
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
            mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl.error

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt
    
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ####### Start the Analysis #######
    ##################################
    
    export sql_8="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',
	    type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此', 
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段', 
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (serial) USING BTREE, 
            KEY idx_fpc (fpc) USING BTREE,
            KEY idx_domain (domain) USING BTREE,
            KEY idx_event (event) USING BTREE,
            KEY idx_attribute (attribute) USING BTREE,
            KEY idx_content (content) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='event etl data' 
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_etl.error

export sql_9="
    INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl
        select 
            null serial, 
            ifnull(c.fpc, a.fpc) fpc, 
            a.domain, 
	    a.type, 
            event, 
            attribute, 
            content, 
	    ifnull(session, concat(date_format(a.created_at, '%Y%m%d'), '01')) session, 
            a.created_at, 
            now() updated_at
        from ${project_name}.${type_s}_${table_name}_${org_id}_pre a
            left join
            (
            select fpc, domain, created_at, event_type type, session
            from ${project_name}.session_both_${org_id}_src 
            where behavior = 'event'
            group by fpc, domain, created_at, event_type, session
            ) b
            on a.created_at = b.created_at
                and a.fpc = b.fpc
                and a.domain = b.domain
                and a.type = b.type
            
            left join uuid.cdp_fpc_mapping c
                on a.fpc = c.origin_fpc 
                    and c.domain = substring_index(a.domain, '/', 1)
    ;"

    #DROP TABLE ${project_name}.${type_s}_both_${org_id}_src;
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_etl.error
     

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
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, fpc, domain, event, attribute, content),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_fpc (fpc), 
            key idx_domain (domain), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='event 保留 fpc 原始表'
        ;"
    #### Export Data ####
    echo ''
    # echo $sql_10
    echo [start: date on ${vDate}]
    echo [create table if not exists ${project_name}.${type_s}/${type_p}_${table_name}_${org_id}/_fpc]
    mysql --login-path=$dest_login_path -e "$sql_10"

    export sql_11="
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
                count(*) freq,
                row_number () over (partition by domain, event, attribute order by count(*) desc) ranking, 
		null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc,
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    session
                from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                group by 
                    fpc,
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    session
                ) a
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
                count(*) freq,
                row_number () over (partition by event, attribute order by count(*) desc) ranking,
		null time_flag, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc,
                    event, 
                    attribute, 
                    content, 
                    session
                from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                group by 
                    fpc,
                    event, 
                    attribute, 
                    content, 
                    session
                ) a
            group by 
                event, 
                attribute, 
                content 
        ; 
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ; 
        #INSERT IGNORE INTO ${project_name}.${type_s}_${table_name}_${org_id}
            #(tag_date, span, start_date, end_date, domain, event, attribute, content)
        #Values ('${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', 'ALL', 'ALL', 'ALL', 'ALL')
        ; 
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ; 



	# the daily data is no needed!
        #INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
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
                count(distinct fpc) person,
                row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking, 
		null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc,
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    session
                from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                group by 
                    fpc,
                    domain, 
                    event, 
                    attribute, 
                    content, 
                    session
                ) a
            group by 
                domain,  
                event, 
                attribute, 
                content 
        ; 
        #ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ; 

        #INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
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
                count(distinct fpc) person,
                row_number () over (partition by event, attribute order by count(distinct fpc) desc) ranking, 
		null time_flag,
                now() created_at, 
                now() updated_at
            from (
                select 
                    fpc,
                    event, 
                    attribute, 
                    content, 
                    session
                from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                group by 
                    fpc,
                    event, 
                    attribute, 
                    content, 
                    session
                ) a
            group by 
                event, 
                attribute, 
                content 
        ; 
        #ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ; 
        
        INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                fpc,
                domain, 
                event, 
                attribute, 
                content, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type_s}_${table_name}_${org_id}_etl
            group by 
                fpc,
                domain, 
                event, 
                attribute, 
                content 
	;
        ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id}_fpc AUTO_INCREMENT = 1
        ;

        INSERT INTO env_config.events_menu
            select 
                null serial, 
                ${org_id} org_id, 
                '${project_name}' channel, 
		b.id event_main_id, 
		b.kind,
                b.type, 
                a.event, 
                now() created_at, 
                now() updated_at
            from (
                select event
                from ${project_name}.${type_s}_${table_name}_${org_id}
                where span = 'daily'
                    and tag_date = '${vDate}'
                    and domain = 'ALL'
                group by event
                ) a
                
                inner join codebook_cdp.events_main b
                    on a.event = b.name
                
                left join env_config.events_menu c
                on a.event = c.event and b.kind = c.kind    
                
            where c.event is null
        ;
        ALTER TABLE env_config.events_menu AUTO_INCREMENT = 1
        ;" 
    #### Export Data ####
    echo ''
    echo $sql_11
    echo [start: date on ${vDate}]
    echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error

    if [ ${vDateName} = Sun ];
    then 
        export sql_12="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
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
                    count(distinct fpc) person, 
                    row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking, 
		    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
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
                    count(distinct fpc) person, 
                    row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking,
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;

            #INSERT IGNORE INTO ${project_name}.${type_p}_${table_name}_${org_id}
                #(tag_date, span, start_date, end_date, domain, event, attribute, content)
            #Values ('${vDate}' + interval 1 day, 'weekly', '${vDate}', '${vDate}', 'ALL', 'ALL', 'ALL', 'ALL')
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 

            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo $sql_12
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    

    else 
        echo [today is ${vDateName}, not Sun. No Need to do the weekly statistics.]
    fi 
  
    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_13="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
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
            DELETE
            FROM ${project_name}.${type_s}_${table_name}_${org_id}
            where start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                and span = 'daily'
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
                    count(distinct fpc) person, 
                    row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking, 
		    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
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
                    count(distinct fpc) person, 
                    row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking,
                    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                group by 
                    event, 
                    attribute, 
                    content
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;
 
            #INSERT IGNORE INTO ${project_name}.${type_p}_${table_name}_${org_id}
                #(tag_date, span, start_date, end_date, domain, event, attribute, content)
            #Values ('${vDate}' + interval 1 day, 'monthly', '${vDate}', '${vDate}', 'ALL', 'ALL', 'ALL', 'ALL')
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 

            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        # echo $sql_13
        echo [start: date on ${vDate}. ${vDate} = ${vMonthLast}?]
        echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    

    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi 
    
    for seasonDate in $seasonEnd
    do
        if [ ${vDate} = ${seasonDate} ];
        then 
            export sql_14="
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
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
                        and span = 'monthly'
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
                        count(distinct fpc) person, 
                        row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking,
			null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
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
                        count(distinct fpc) person, 
                        row_number () over (partition by domain, event, attribute order by count(distinct fpc) desc) ranking,
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                    group by 
                        event, 
                        attribute, 
                        content
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;
 
                #INSERT IGNORE INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    #(tag_date, span, start_date, end_date, domain, event, attribute, content)
                #Values ('${vDate}' + interval 1 day, 'seasonal', '${vDate}', '${vDate}', 'ALL', 'ALL', 'ALL', 'ALL')
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ; 

                UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;

                DELETE
                FROM ${project_name}.${type_p}_${table_name}_${org_id}_fpc
                WHERE start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ;"
            echo ''
            # echo $sql_14
            echo [start: date on ${vDate}. ${vDate} = ${seasonDate}?]
            echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_14" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    
        else 
            echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
        fi 
    done 

    echo ''
    echo [at the end: DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src;"
    echo [at the end: DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_pre;"
    echo [at the end: DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl;"


done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_7.txt
#done < /root/datapool/export_file/cdp/web/web.cdp_org_id_7.txt

echo ''
echo [end the ${vDate} data on `date`]
