#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
export type_p="person"
export type_s="session"
export table_name="event" 
export src_login_path="cdp"
export src_login_true="cdp_cms"

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
    where domain_type = '${project_name}'
    group by org_id
    limit 2, 2
    ;"    
echo ''
echo [Get the org_id]
#mysql --login-path=$src_login_true -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_8.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.error
#sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_8.txt


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
                serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
                token varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '${project_name} token', 
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
                null serial, 
                ${project_name}, 
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
            from cdp_${project_name}_${db_id}.${project_name}_event_raw_data a, 
                cdp_${project_name}_${db_id}.${project_name}_unique b, 
                cdp_organization.events_main c
            where a.${project_name}_unique_id = b.id
                and a.type = c.type
                and a.created_at >= UNIX_TIMESTAMP('${vDate}')
                and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
            ;" 
    
        echo $sql_3
        #### Export Data ####
        echo ''
        echo [start: date on ${vDate}]
        echo [exporting data to ${project_name}.${type_s}_${table_name}_${org_id}_src.txt]
        mysql --login-path=$src_login_true -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_${db_id}_src.error
    
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}_etl (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            token varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '${project_name} token', 
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
            PRIMARY KEY (serial) USING BTREE,
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
                null serial, 
                token, 
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
                    IF(@domain = domain,  
                        IF(@token = token, 
                            IF(session = 1, @session := @session + 1, @session), 
                        @session := 1), 
                    @session := 1) session_break, 
                    @token := token, 
                    @domain := domain
                from (
                    select 
                        a.*, 
                        if(timestampdiff(minute, b.created_at, a.created_at) >= 30, 1, 0) session
                    from (
                        select 
                            s.*, 
                            row_number () over (partition by domain, token order by created_at) rid
                        from ${project_name}.${type_s}_${table_name}_${org_id}_src s
                        ) a
                        
                        left join
                        (
                        select 
                            s.*, 
                            row_number () over (partition by domain, token order by created_at) rid
                        from ${project_name}.${type_s}_${table_name}_${org_id}_src s
                        ) b
                        on a.token = b.token and a.domain = b.domain and a.rid = b.rid + 1
                    ) c, 
                    (select @session := 1, @token, @domain) d
                ) e
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}_etl.error

#### app 沒有東西可以做 id mapping ####


done < /root/datapool/export_file/cdp/${project_name}/${project_name}.cdp_org_id_8.txt

echo ''
echo [end the ${vDate} data on `date`]
