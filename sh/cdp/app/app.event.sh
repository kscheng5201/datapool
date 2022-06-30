#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
export type_s="session"
export type_p="person"
export table_name="event_temp" 
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
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    #### 1. Making the ETL table  ####
    ##################################

    export sql_4="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'line accu_id',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',
            type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此', 
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (accu_id, domain, event, attribute, content, created_at) USING BTREE, 
            KEY idx_token (accu_id) USING BTREE,
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
    echo [Get the type_id on ${project_name}]
    mysql --login-path=$dest_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt

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
        echo [Get the column_index on ${project_name}]
        mysql --login-path=$dest_login_path -e "$sql_6" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt
    
        while read column_index; 
        do 
            export sql_7="            
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_pre
                    select 
                        null serial, 
                        accu_id,
                        domain,
                        type, 
                        event, 
                        (select ${column_index} from codebook_cdp.events_function where type = ${type_id}) attribute, 
                        ${column_index} content, 
                        created_at, 
                        now() updated_at
                    from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                    where type = ${type_id}
                        and type not in (
                            select type
                            from codebook_cdp.events_analysis_base
                            where column_location in (null, '')
                        )
                    group by 
                        accu_id, 
                        domain,
                        type,  
                        event, 
                        ${column_index},
                        created_at
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
            echo ''
            echo $sql_7
            mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl.error

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_column_index.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_type.txt
    
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ##### 2. Start the Analysis   ####
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
            primary key (serial),
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id}_token (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日',
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日', 
            accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'line accu_id', 
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, accu_id, domain, event, attribute, content),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date),  
            key idx_token (accu_id), 
            key idx_domain (domain), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='event 保留 ${project_name} 原始表'
        ;"
    #### Export Data ####
    echo ''
    # echo $sql_10
    echo [start: date on ${vDate}]
    echo [create table if not exists ${project_name}.${type_s}/${type_p}_${table_name}_${org_id}/_${project_name}]
    mysql --login-path=$dest_login_path -e "$sql_10"

    export sql_11="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
            select *
            from (
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
                        accu_id,
                        domain, 
                        event, 
                        attribute, 
                        content
                    from ${project_name}.${type_s}_${table_name}_${org_id}_pre
                    group by 
                        accu_id,
                        domain, 
                        event, 
                        attribute, 
                        content 
                    ) a 
    
                group by 
                    domain,  
                    event, 
                    attribute, 
                    content 
                ) b
                
            where ranking <= 500
        ; 
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ; 

        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
            select *
            from (
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
                        accu_id,
                        event, 
                        attribute, 
                        content
                    from ${project_name}.${type_s}_${table_name}_${org_id}_pre
                    group by 
                        accu_id,
                        event, 
                        attribute, 
                        content 
                    ) a 
    
                group by 
                    event, 
                    attribute, 
                    content 
                ) b
                
            where ranking <= 500
        ; 
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
	;

        INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}_token
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
        ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id}_token AUTO_INCREMENT = 1
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
                select *
                from (
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
                    ) a
                where ranking <= 500
            ; 
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 
            UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select *
                from (
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
                        count(distinct token) user, 
                        row_number () over (partition by domain, event, attribute order by count(distinct accu_id) desc) ranking, 
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_token
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                    group by 
                        domain, 
                        event, 
                        attribute, 
                        content
                    ) a
                where ranking <= 500
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select *
                from (
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
                        count(distinct token) user, 
                        row_number () over (partition by event, attribute order by count(distinct token) desc) ranking,
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_token
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                        and start_date < '${vDate}' + interval 1 day
                    group by 
                        event, 
                        attribute, 
                        content
                    ) a
                where ranking <= 500
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


    export sql_weekly_count_s="
        select count(*) weekly_count_s
        from ${project_name}.${type_s}_${table_name}_${org_id}
        where tag_date = '${vDate}' + interval 1 day
            and span = 'weekly'
            and event in (
                select event
                from env_config.events_analysis_base
                where column_location is not null
                    and column_location <> ''
            )
        ;"
    echo ''
    echo [Get the weekly_count_s on ${org_id}]
    echo $sql_weekly_count_s
    mysql --login-path=$dest_login_path -e "$sql_weekly_count_s" > $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_weekly_count_s.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_weekly_count_s.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_weekly_count_s.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_weekly_count_s.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_weekly_count_s.txt

    while read weekly_count_s; 
    do 
        if [ ${vDateName} = Sun ] && [ ${weekly_count_s} = 0 ];
        then 
            export sql_insert_weekly_empty="
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), '${vDate}', 'ALL', '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), '${vDate}', (select domain from codebook_cdp.organization_domain where org_id = ${org_id} and domain_type = '${project_name}'), '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;"            
            echo ''
            echo $sql_insert_weekly_empty
            echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
            echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_insert_weekly_empty" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_insert_weekly_empty.error    
    
        else 
            echo [no need to do the zero fill-in on weekly]
        fi
    done < $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_weekly_count_s.txt


    export sql_weekly_count_p="
        select count(*) weekly_count_p
        from ${project_name}.${type_p}_${table_name}_${org_id}
        where tag_date = '${vDate}' + interval 1 day
            and span = 'weekly'
            and event in (
                select event
                from env_config.events_analysis_base
                where column_location is not null
                    and column_location <> ''
            )
        ;"
    echo ''
    echo [Get the weekly_count_p on ${org_id}]
    echo $sql_weekly_count_p
    mysql --login-path=$dest_login_path -e "$sql_weekly_count_p" > $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_weekly_count_p.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_weekly_count_p.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_weekly_count_p.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_weekly_count_p.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_weekly_count_p.txt

    while read weekly_count_p; 
    do 
        if [ ${vDateName} = Sun ] && [ ${weekly_count_p} = 0 ];
        then 
            export sql_insert_weekly_empty="
                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), '${vDate}', 'ALL', '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), '${vDate}', (select domain from codebook_cdp.organization_domain where org_id = ${org_id} and domain_type = '${project_name}'), '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;"
            echo ''
            echo $sql_insert_weekly_empty
            echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
            echo [insert into ${project_name}.${type_p}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_insert_weekly_empty" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_sql_insert_weekly_empty.error    
    
        else 
            echo [no need to do the zero fill-in on weekly]
        fi
    done < $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_weekly_count_p.txt




    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_13="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select *
                from (
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
                    ) a
                where ranking <= 500
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
                select *
                from (
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
                        count(distinct token) user, 
                        row_number () over (partition by domain, event, attribute order by count(distinct token) desc) ranking, 
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_token
                    where start_date >= date_format('${vDate}', '%Y-%m-01')
                        and start_date < '${vDate}' + interval 1 day
                    group by 
                        domain, 
                        event, 
                        attribute, 
                        content
                    ) a
                where ranking <= 500
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1  
            ;

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select *
                from (
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
                        count(distinct token) user, 
                        row_number () over (partition by event, attribute order by count(distinct token) desc) ranking,
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type_p}_${table_name}_${org_id}_token
                    where start_date >= date_format('${vDate}', '%Y-%m-01')
                        and start_date < '${vDate}' + interval 1 day
                    group by 
                        event, 
                        attribute, 
                        content
                    ) a
                where ranking <= 500
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo $sql_13
        echo [start: date on ${vDate}. ${vDate} = ${vMonthLast}?]
        echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    

    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi 


    export sql_monthly_count_s="
        select count(*) monthly_count_s
        from ${project_name}.${type_s}_${table_name}_${org_id}
        where tag_date = '${vDate}' + interval 1 day
            and span = 'monthly'
            and event in (
                select event
                from env_config.events_analysis_base
                where column_location is not null
                    and column_location <> ''
            )
        ;"
    echo ''
    echo [Get the monthly_count_s on ${org_id}]
    echo $sql_monthly_count_s
    mysql --login-path=$dest_login_path -e "$sql_monthly_count_s" > $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_monthly_count_s.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_monthly_count_s.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_monthly_count_s.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_monthly_count_s.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_monthly_count_s.txt

    while read monthly_count_s; 
    do 
        if [ ${vDate} = ${vMonthLast} ] && [ ${monthly_count_s} = 0 ];
        then 
            export sql_insert_monthly_empty="
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', date_format('${vDate}', '%Y-%m-01'), '${vDate}', 'ALL', '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', date_format('${vDate}', '%Y-%m-01'), '${vDate}', (select domain from codebook_cdp.organization_domain where org_id = ${org_id} and domain_type = '${project_name}'), '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;"            
            echo ''
            echo $sql_insert_monthly_empty
            echo [start: date on ${vDate}. Is ${vDate} = ${vMonthLast}?]
            echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_insert_monthly_empty" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_insert_monthly_empty.error    
    
        else 
            echo [no need to do the zero fill-in on monthly]
        fi
    done < $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_monthly_count_s.txt


    export sql_monthly_count_p="
        select count(*) monthly_count_p
        from ${project_name}.${type_p}_${table_name}_${org_id}
        where tag_date = '${vDate}' + interval 1 day
            and span = 'monthly'
            and event in (
                select event
                from env_config.events_analysis_base
                where column_location is not null
                    and column_location <> ''
            )
        ;"
    echo ''
    echo [Get the monthly_count_p on ${org_id}]
    echo $sql_monthly_count_p
    mysql --login-path=$dest_login_path -e "$sql_monthly_count_p" > $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_monthly_count_p.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_monthly_count_p.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_monthly_count_p.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_monthly_count_p.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_monthly_count_p.txt

    while read monthly_count_p; 
    do 
        if [ ${vDate} = ${vMonthLast} ] && [ ${monthly_count_p} = 0 ];
        then 
            export sql_insert_monthly_empty="
                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', date_format('${vDate}', '%Y-%m-01'), '${vDate}', 'ALL', '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    VALUES (null, '${vDate}' + interval 1 day, 'weekly', date_format('${vDate}', '%Y-%m-01'), '${vDate}', (select domain from codebook_cdp.organization_domain where org_id = ${org_id} and domain_type = '${project_name}'), '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                ;
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;"
            echo ''
            echo $sql_insert_monthly_empty
            echo [start: date on ${vDate}. Is ${vDate} = ${vMonthLast}?]
            echo [insert into ${project_name}.${type_p}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_insert_monthly_empty" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_sql_insert_monthly_empty.error    
    
        else 
            echo [no need to do the zero fill-in on monthly]
        fi
    done < $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_monthly_count_p.txt


    for seasonDate in $seasonEnd
    do
        if [ ${vDate} = ${seasonDate} ];
        then 
            export sql_14="
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    select *
                    from (
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
                        ) a
                    where ranking <= 500
                ; 
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;
                UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;

                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    select *
                    from (
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
                            count(distinct token) user, 
                            row_number () over (partition by domain, event, attribute order by count(distinct token) desc) ranking,
                            null time_flag, 
                            now() created_at, 
                            now() updated_at
                        from ${project_name}.${type_p}_${table_name}_${org_id}_token
                        where start_date >= '${vDate}' - interval 3 month + interval 1 day
                            and start_date < '${vDate}' + interval 1 day
                        group by 
                            domain, 
                            event, 
                            attribute, 
                            content
                        ) a
                    where ranking <= 500
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    select *
                    from (
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
                            count(distinct token) user, 
                            row_number () over (partition by event, attribute order by count(distinct token) desc) ranking,
                            null time_flag,
                            now() created_at, 
                            now() updated_at
                        from ${project_name}.${type_p}_${table_name}_${org_id}_token
                        where start_date >= '${vDate}' - interval 3 month + interval 1 day
                            and start_date < '${vDate}' + interval 1 day
                        group by 
                            event, 
                            attribute, 
                            content
                        ) a
                    where ranking <= 500
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;
 
                UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;

                DELETE
                FROM ${project_name}.${type_p}_${table_name}_${org_id}_token
                WHERE start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ;"
            echo ''
            echo $sql_14
            echo [start: date on ${vDate}. ${vDate} = ${seasonDate}?]
            echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_14" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    
        else 
            echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
        fi


        export sql_seasonal_count="
            select count(*) seasonal_count
            from ${project_name}.${type_s}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
                and span = 'seasonal'
                and event in (
                    select event
                    from env_config.events_analysis_base
                    where column_location is not null
                        and column_location <> ''
                )
            ;"
        echo ''
        echo [Get the seasonal_count on ${org_id}]
        echo $sql_seasonal_count
        mysql --login-path=$dest_login_path -e "$sql_seasonal_count" > $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count.txt
        echo $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count.txt
        cat $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count.txt
    

        export sql_seasonal_count_s="
            select count(*) seasonal_count_s
            from ${project_name}.${type_s}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
                and span = 'seasonal'
                and event in (
                    select event
                    from env_config.events_analysis_base
                    where column_location is not null
                        and column_location <> ''
                )
            ;"
        echo ''
        echo [Get the seasonal_count_s on ${org_id}]
        echo $sql_seasonal_count_s
        mysql --login-path=$dest_login_path -e "$sql_seasonal_count_s" > $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count_s.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count_s.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count_s.txt
        echo $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count_s.txt
        cat $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count_s.txt
    
        while read seasonal_count_s; 
        do 
            if [ ${vDate} = ${seasonDate} ] && [ ${seasonal_count_s} = 0 ];
            then 
                export sql_insert_seasonal_empty="
                    INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                        VALUES (null, '${vDate}' + interval 1 day, 'weekly', '${vDate}' - interval 3 month + interval 1 day, '${vDate}', 'ALL', '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                    ;
                    ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                    ;
    
                    INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                        VALUES (null, '${vDate}' + interval 1 day, 'weekly', '${vDate}' - interval 3 month + interval 1 day, '${vDate}', (select domain from codebook_cdp.organization_domain where org_id = ${org_id} and domain_type = '${project_name}'), '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                    ;
                    ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                    ;"            
                echo ''
                echo $sql_insert_seasonal_empty
                echo [start: date on ${vDate}. Is ${vDate} = ${seasonDate}?]
                echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
                mysql --login-path=$dest_login_path -e "$sql_insert_seasonal_empty" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_insert_seasonal_empty.error    
        
            else 
                echo [no need to do the zero fill-in on seasonal]
            fi
        done < $export_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_seasonal_count_s.txt


        export sql_seasonal_count_p="
            select count(*) seasonal_count_p
            from ${project_name}.${type_p}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
                and span = 'seasonal'
                and event in (
                    select event
                    from env_config.events_analysis_base
                    where column_location is not null
                        and column_location <> ''
                )
            ;"
        echo ''
        echo [Get the seasonal_count_p on ${org_id}]
        echo $sql_seasonal_count_p
        mysql --login-path=$dest_login_path -e "$sql_seasonal_count_p" > $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_seasonal_count_p.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_seasonal_count_p.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_seasonal_count_p.txt
        echo $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_seasonal_count_p.txt
        cat $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_seasonal_count_p.txt
    
        while read seasonal_count_p; 
        do 
            if [ ${vDate} = ${seasonDate} ] && [ ${seasonal_count_p} = 0 ];
            then 
                export sql_insert_seasonal_empty="
                    INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                        VALUES (null, '${vDate}' + interval 1 day, 'weekly', '${vDate}' - interval 3 month + interval 1 day, '${vDate}', 'ALL', '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                    ;
                    ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                    ;
    
                    INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                        VALUES (null, '${vDate}' + interval 1 day, 'weekly', '${vDate}' - interval 3 month + interval 1 day, '${vDate}', (select domain from codebook_cdp.organization_domain where org_id = ${org_id} and domain_type = '${project_name}'), '無事件', '無屬性', '無內容', 0, 1, 'last', now(), now())
                    ;
                    ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                    ;"            
                echo ''
                echo $sql_insert_seasonal_empty
                echo [start: date on ${vDate}. Is ${vDate} = ${seasonDate}?]
                echo [insert into ${project_name}.${type_p}_${table_name}_${org_id}]
                mysql --login-path=$dest_login_path -e "$sql_insert_seasonal_empty" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_sql_insert_seasonal_empty.error    
        
            else 
                echo [no need to do the zero fill-in on seasonal]
            fi
        done < $export_dir/$src_login_path/$project_name/$project_name.${type_p}_${table_name}_${org_id}_seasonal_count_p.txt
 
    done 

    echo ''
    #echo [at the end: DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    #mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_etl;"
    echo [at the end: DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type_s}_${table_name}_${org_id}_pre;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt

echo ''
echo [end the ${vDate} data on `date`]
