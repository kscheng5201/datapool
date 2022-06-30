
#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 事件分析
# Created_at: 2022-01-04
# Updated_at: 2022-01-04
# Note: 
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type_s="session"
export type_p="person"
export table_name="event" 
export src_login_path="cdp"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
    nvDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
    nvDate=`date -d "1 day ago" +"%Y-%m-%d"`
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
    echo [DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src;"
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre;"
   
    export sql_0="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            domain varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',            
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
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
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '原始創建時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間', 
            PRIMARY KEY (serial) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE,
            KEY idx_type (type) USING BTREE,
            KEY idx_fpc (fpc) USING BTREE, 
            KEY idx_campaign_id (campaign_id) USING BTREE,
            KEY idx_utm_id (utm_id) USING BTREE, 
            KEY idx_domain (domain) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='依照 utm 取得接觸過 event 的 fpc_raw_data 全表資訊(當天資料)' 
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_0
    echo [${vDate} at `date`] >> $error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src_sql0.error    
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src_sql0.error    
    echo '' >> $error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src_sql0.error    
    
    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
        ;"
    echo ''
    echo [Get the db_id on web of ${org_id}]
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_db_id.txt
    cat $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_db_id.txt


    while read db_id; 
    do 
        while read utm_detail; 
        do 
            export sql_2="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    domain,
                    $(echo ${utm_detail} | cut -d _ -f 1) campaign_id, 
                    $(echo ${utm_detail} | cut -d _ -f 2) utm_id, 
                    fpc,
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
                from cdp_web_${db_id}.fpc_event_raw_data a
                    left join cdp_web_${db_id}.fpc_unique b
                        on a.fpc_unique_id = b.id          
                    inner join cdp_organization.events_main c
                        on a.type = c.id
                where a.created_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                    and fpc_unique_id in 
                        (
                        select channel_id
                        from cdp_${org_id}.user_utm a
                        where a.created_at = ${vDate}
                            and a.campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                            and a.utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                            and a.db_id = ${db_id}
                            and a.created_at <= date_format(a.updated_at, '%Y%m%d')
                            and a.updated_at >= '${vDate}'
                            and a.channel_type = 1
                        )
                ;"
            #### Export Data ####
            echo ''
            echo [exporting data to ${project_name}.${type_s}_${table_name}_${org_id}_src.txt]
            echo $sql_2            
            echo [${vDate} at `date`] >> $error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src_sql2.error
            mysql --login-path=${src_login_path} -e "$sql_2" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src_sql2.error
            echo '' >> $error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src_sql2.error

            #### Import Data ####
            echo ''
            echo [import data from $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src.txt to ${project_name}.${type_s}_${table_name}_${org_id}_src]
            echo [${vDate} at `date`] >> $error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src_sql2.error
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)_src.txt' INTO TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src.error
            echo '' >> $error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src.error
            
        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt
    done < $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_db_id.txt

    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ###### Making the ETL table ######
    ##################################

    export sql_4="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',            
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此', 
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            KEY idx_fpc (fpc) USING BTREE,
            KEY idx_type (type) USING BTREE,
            KEY idx_attribute (attribute) USING BTREE,
            KEY idx_content (content) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='event pre-etl data' 
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_pre.error
  
    export sql_5="
        select type
        from env_config.events_analysis_base
        where column_location is not null
            and column_location <> ''
        group by type
    	;"
    echo ''
    echo [Get the type_id on web]
    mysql --login-path=$dest_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_type.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_type.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_type.txt

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
        mysql --login-path=$dest_login_path -e "$sql_6" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_column_index.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_column_index.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_column_index.txt
    
        while read column_index; 
        do 
            export sql_7="            
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_pre
                    select 
                        null serial, 
                        campaign_id, 
                        utm_id,
                        fpc,
                        type, 
                        event, 
                        (select ${column_index} from codebook_cdp.events_function where type = ${type_id}) attribute, 
                        ${column_index} content, 
                        created_at, 
                        now() updated_at
                    from ${project_name}.${type_s}_${table_name}_${org_id}_src
                    where type = ${type_id}
                    group by 
                        campaign_id, 
                        utm_id,
                        fpc,
                        type, 
                        event, 
                        ${column_index}, 
                        created_at
                ;"
            echo ''
            echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
            mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl.error

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_column_index.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_events_analysis_base_type.txt
    
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ####### Start the Analysis #######
    ##################################
    
    export sql_8="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',            
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            type tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此', 
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始創建時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            KEY idx_fpc (fpc) USING BTREE,
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
                campaign_id, 
                utm_id,
                ifnull(c.fpc, a.fpc) fpc, 
                a.type, 
                event, 
                attribute, 
                content, 
                a.created_at, 
                now() updated_at
            from ${project_name}.${type_s}_${table_name}_${org_id}_pre a
                left join uuid.cdp_fpc_mapping c
                    on a.fpc = c.origin_fpc 
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_etl.error

    export sql_10="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            campaign_start date DEFAULT NULL COMMENT '活動開始日期', 
            campaign_end date DEFAULT NULL COMMENT '活動結束日期',
            utm_id int(11) NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',  
            utm_start date DEFAULT NULL COMMENT 'UTM開始日期', 
            utm_end date DEFAULT NULL COMMENT 'UTM結束日期',            
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            freq int DEFAULT NULL COMMENT '事件-屬性的數量',  
            ranking int DEFAULT NULL COMMENT '依照事件-屬性的數量所做的排名', 
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, event, attribute, content),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_campaign_id (campaign_id), 
            key idx_utm_id (utm_id), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【事件深度分析】流量相關各項資料'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            campaign_start date DEFAULT NULL COMMENT '活動開始日期', 
            campaign_end date DEFAULT NULL COMMENT '活動結束日期',
            utm_id int(11) NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',  
            utm_start date DEFAULT NULL COMMENT 'UTM開始日期', 
            utm_end date DEFAULT NULL COMMENT 'UTM結束日期',            
            event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '事件名稱', 
            attribute varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '根據 events_function 所得知的 event 子項目',
            content varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'event 子項目的實際內容; 依照 events_analysis_base 指定的欄位, 放置於此',
            user int DEFAULT NULL COMMENT '事件-人數的數量',  
            ranking int DEFAULT NULL COMMENT '依照事件-人數所做的排名',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期',  
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, event, attribute, content),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_campaign_id (campaign_id), 
            key idx_utm_id (utm_id), 
            key idx_event (event), 
            key idx_attribute (attribute), 
            key idx_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【事件深度分析】人數相關各項資料'
        ;"
    #### Export Data ####
    echo ''
    echo $sql_10
    echo [start: date on ${vDate}]
    echo [create table if not exists ${project_name}.${type_s}/${type_p}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_10"

    while read campaign_utm;
    do
        export sql_11="
            DELETE 
            FROM ${project_name}.${type_s}_${table_name}_${org_id}
            WHERE utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
                and utm_end >= '${vDate}'
                and span = 'FULL'
            ;
            DELETE 
            FROM ${project_name}.${type_p}_${table_name}_${org_id}
            WHERE utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
                and utm_end >= '${vDate}'
                and span = 'FULL'
            ;
            
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_utm} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 3)' campaign_end,
                    '$(echo ${campaign_utm} | cut -d _ -f 4)' utm_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 5)' utm_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 6)' utm_end,
                    event, 
                    attribute, 
                    content, 
                    count(*) freq,
                    row_number () over (partition by campaign_id, utm_id, event, attribute order by count(*) desc) ranking, 
                    'last' time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                    where campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1)
                        and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
                        and created_at >= '$(echo ${campaign_utm} | cut -d _ -f 5)'
                        and created_at < '$(echo ${campaign_utm} | cut -d _ -f 6)' + interval 1 day
                        and content is not null
                        and content <> ''
                    group by 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    ) a
                group by  
                    event, 
                    attribute, 
                    content 
            ; 
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 
    
            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_utm} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 3)' campaign_end,
                    '$(echo ${campaign_utm} | cut -d _ -f 4)' utm_id, 
                    '$(echo ${campaign_utm} | cut -d _ -f 5)' utm_start, 
                    '$(echo ${campaign_utm} | cut -d _ -f 6)' utm_end,
                    event, 
                    attribute, 
                    content, 
                    count(distinct fpc) freq,
                    row_number () over (partition by campaign_id, utm_id, event, attribute order by count(distinct fpc) desc) ranking, 
                    'last' time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                    where campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1)
                        and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
                        and created_at >= '$(echo ${campaign_utm} | cut -d _ -f 5)'
                        and created_at < '$(echo ${campaign_utm} | cut -d _ -f 6)' + interval 1 day
                        and content is not null
                        and content <> ''
                    group by 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    ) a
                group by 
                    event, 
                    attribute, 
                    content 
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        #### Export Data ####
        echo ''
        echo $sql_11
        echo [start: date on ${vDate}]
        echo [insert into ${project_name}.${type_s}_${table_name}_${org_id} and ${project_name}.${type_p}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_11.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.txt
    
    
    while read campaign_detail; 
    do
        export sql_12="
            DELETE 
            FROM ${project_name}.${type_s}_${table_name}_${org_id}
            WHERE campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                and utm_id = 0
                and campaign_end >= '${vDate}'
                and span = 'FULL'
            ;
            DELETE 
            FROM ${project_name}.${type_p}_${table_name}_${org_id}
            WHERE campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                and utm_id = 0
                and campaign_end >= '${vDate}'
                and span = 'FULL'
            ;

            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_detail} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_detail} | cut -d _ -f 3)' campaign_end,
                    0 utm_id, 
                    null utm_start, 
                    null utm_end,
                    event, 
                    attribute, 
                    content, 
                    count(*) freq,
                    row_number () over (partition by campaign_id, utm_id, event, attribute order by count(*) desc) ranking, 
                    'last' time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                    where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                        and created_at >= '$(echo ${campaign_detail} | cut -d _ -f 2)'
                        and created_at < '$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                        and content is not null
                        and content <> ''
                    group by 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    ) a
                group by  
                    event, 
                    attribute, 
                    content 
            ; 
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ; 
    
            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span,
                    '$(echo ${campaign_detail} | cut -d _ -f 1)' campaign_id, 
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start, 
                    '$(echo ${campaign_detail} | cut -d _ -f 3)' campaign_end,
                    0 utm_id, 
                    null utm_start, 
                    null utm_end,
                    event, 
                    attribute, 
                    content, 
                    count(distinct fpc) freq,
                    row_number () over (partition by campaign_id, utm_id, event, attribute order by count(distinct fpc) desc) ranking, 
                    'last' time_flag,
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                    where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                        and created_at >= '$(echo ${campaign_detail} | cut -d _ -f 2)'
                        and created_at < '$(echo ${campaign_detail} | cut -d _ -f 3)' + interval 1 day
                        and content is not null
                        and content <> ''
                    group by 
                        fpc,
                        campaign_id, 
                        utm_id, 
                        event, 
                        attribute, 
                        content
                    ) a
                group by 
                    event, 
                    attribute, 
                    content 
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        #### Export Data ####
        echo ''
        echo $sql_12
        echo [start: date on ${vDate}]
        echo [insert into ${project_name}.${type_s}_${table_name}_${org_id} and ${project_name}.${type_p}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_12.error

        export sql_12a="
            INSERT INTO ${project_name}.events_menu
                select 
                    null serial, 
                    ${org_id} org_id, 
                    $(echo ${campaign_detail} | cut -d _ -f 1) campaign_id,
                    b.id event_main_id, 
                    b.kind,
                    b.type, 
                    a.event, 
                    now() created_at, 
                    now() updated_at
                from (
                    select event
                    from ${project_name}.${type_s}_${table_name}_${org_id}
                    where span = 'FULL'
                        and tag_date = '${vDate}' + interval 1 day
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    group by event
                    ) a
                    
                    inner join codebook_cdp.events_main b
                        on a.event = b.name
    
                    left join 
                    (
                    select *
                    from ${project_name}.events_menu 
                    where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                        and org_id = ${org_id}
                    ) c
                    on a.event = c.event and b.kind = c.kind  
    
                where c.event is null
            ;
            ALTER TABLE ${project_name}.events_menu AUTO_INCREMENT = 1
            ;" 
        echo ''
        echo [INSERT INTO ${project_name}.events_menu]
        echo $sql_12a
        mysql --login-path=$dest_login_path -e "$sql_12a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.events_menu_sql_12a.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt 
    
    
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src;"
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre;"

    while read campaign_utm;
    do 
        if [ $(echo ${campaign_utm} | cut -d _ -f 6) = ${nvDate} ];
        then 
            export sql_13="
                DELETE
                FROM ${project_name}.${type_s}_${table_name}_${org_id}_etl
                WHERE campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1)
                    and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)
                ;"
            echo ''
            echo [DELETE FROM ${project_name}.${type_s}_${table_name}_${org_id}_etl WHERE campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1) and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4)]
            echo $sql_13
            #mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_sql_13.error
        else
            echo [keep whom as campaign_id = $(echo ${campaign_utm} | cut -d _ -f 1) and utm_id = $(echo ${campaign_utm} | cut -d _ -f 4) in ${project_name}.${type_s}_${table_name}_${org_id}_etl]
        fi

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo [end ${vDate} at `date`]
