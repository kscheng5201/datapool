#!/usr/bin/bash
####################################################
# Project: APP 互動分析儀表板
# Branch: 行銷漏斗上游
# Author: Benson Cheng
# Created_at: 2022-04-13
# Updated_at: 2022-04-13
# Note: 主程式
#####################################################
export dest_login_path="datapool_prod"
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

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id_4.txt

echo ''
echo [end the ${vDate} data on `date`]


