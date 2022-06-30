#!/usr/bin/bash
####################################################
# Project: APP 互動分析儀表板儀表板
# Branch: session_event 大表產製
# Author: Benson Cheng
# Created_at: 2022-04-11
# Updated_at: 2022-04-11
# Note: 
#####################################################
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
export type_p="person"
export type_s="session"
export table_name="0420_event" 
export src_login_path="cdp"
export src_login_true="cdp"

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
    echo ''
    echo [at beginning: DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src;"
    echo [at beginning: DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_pre;"
    echo [at beginning: DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl;"

    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
    	;"
    echo ''
    echo [Get the db_id on ${project_name}]
    mysql --login-path=$src_login_true -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    while read db_id; 
    do
        export sql_2="    
            CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_src (
                id int NOT NULL COMMENT '原始 id', 
                token varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '${project_name} token', 
                accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id',                 
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
                PRIMARY KEY (id, domain) USING BTREE,
                KEY idx_created_at (created_at) USING BTREE,
                KEY idx_type (type) USING BTREE,
                KEY idx_token (token) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='${project_name}_event_raw_data 全表複製' 
            ;" 
        echo ''
        echo [create table if not exists ${project_name}.${type_s}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_src.error
    
        export sql_3="    
            SET NAMES utf8mb4
            ;
            select 
                a.id, 
                ${project_name}, 
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
                from_unixtime(a.created_at) created_at, 
                now() updated_at
            from cdp_${project_name}_${db_id}.${project_name}_event_raw_data a, 
                cdp_${project_name}_${db_id}.${project_name}_unique b, 
                cdp_organization.events_main c
            where a.${project_name}_unique_id = b.id
                and a.type = c.type
                and a.created_at >= UNIX_TIMESTAMP('${vDate}')
                and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
            ;" 
        #### Export Data ####
        echo ''
        echo [start: date on ${vDate}]
        echo [exporting data to ${project_name}.${type_s}_${table_name}_${org_id}_src.txt]
        echo $sql_3
        mysql --login-path=$src_login_true -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src_sql_3.error
    
        #### Import Data ####
        echo ''
        echo [start: date on ${vDate}]
        echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.txt' INTO TABLE ${project_name}.${type_s}_${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src_sql_3.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    ##################################
    #### FURTHER WORK in DATAPOOL ####
    ###### Making the ETL table ######
    ##################################


    export sql_up="
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id}_src a
            INNER JOIN uuid.accu_mapping_${org_id}_20220420 b
            ON a.token = b.id
        SET a.accu_id = b.accu_id
        WHERE b.id_type = 'app'
        ; 
        
        UPDATE ${project_name}.${type_s}_${table_name}_${org_id}_src 
        SET accu_id = token
        WHERE accu_id = 'NULL'
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type_s}_${table_name}_${org_id}_src]
    echo $sql_up
    mysql --login-path=$dest_login_path -e "$sql_up" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src_sql_up.error


    export sql_4="    
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl (
            id int NOT NULL COMMENT '原始 id',
            token varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '${project_name} token', 
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id',                 
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
            session int(10) unsigned NOT NULL COMMENT '%Y%m%d + session',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間', 
            PRIMARY KEY (id, domain) USING BTREE,
            KEY idx_created_at (created_at) USING BTREE,
            KEY idx_type (type) USING BTREE,
            KEY idx_token (token) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='${project_name}_event_raw_data 標記 session' 
        ;" 
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl.error


    export sql_5="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl
            select 
                id, 
                token, 
                accu_id, 
                domain, 
                type, 
                event, 
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
                concat(date_format(created_at, '%Y%m%d'), LPAD(session_break, 2 ,'0')) session,
                created_at,
                now() updated_at
            from (
                select 
                    c.*,
                    IF(@accu_id = accu_id, 
                        IF(session = 1, @session := @session + 1, @session), 
                        @session := 1) session_break, 
                    @accu_id := accu_id
                from (
                    select 
                        a.*, 
                        if(timestampdiff(minute, b.created_at, a.created_at) >= 30, 1, 0) session
                    from (
                        select 
                            s.*, 
                            row_number () over (partition by accu_id order by created_at) rid
                        from ${project_name}.${type_s}_${table_name}_${org_id}_src s
                        ) a
                        
                        left join
                        (
                        select 
                            s.*, 
                            row_number () over (partition by accu_id order by created_at) rid
                        from ${project_name}.${type_s}_${table_name}_${org_id}_src s
                        ) b
                        on a.accu_id = b.accu_id and a.rid = b.rid + 1
                    ) c, 
                    (select @session := 1, @accu_id) d
                ) e
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl.error


    export sql_6="
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
                select type 
                from ${project_name}.${type_s}_${table_name}_${org_id}_etl
                group by type
                ) a, 
                codebook_cdp.events_main b
            where a.type = b.type
        ;
        ALTER TABLE env_config.events_menu AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO env_config.events_menu]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl_sql_6.error

done < $export_dir/$src_login_path/${project_name}/${project_name}.${src_login_path}_prod_org_id.txt

echo ''
echo [end the ${vDate} data on `date`]

