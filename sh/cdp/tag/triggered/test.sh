#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tag"
export src_login_path="cdp"
export table_name="triggered"


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### loop by db_id ####
for i in $(seq 1 3)
do 
    export sql_1="
        SET NAMES utf8mb4
        ;
        select 
            null serial,
            fpc, 
            source domain, 
            (json_array(page_keyword)) keyword, 
            FROM_UNIXTIME(created_at) + interval 8 hour created_at
        from cdp_web_${i}.fpc_raw_data
        where created_at >= UNIX_TIMESTAMP('${vDate}' - interval 8 hour) 
            and created_at < UNIX_TIMESTAMP('${vDate}' - interval 8 hour + interval 1 day)
            and page_keyword is not null
            and page_keyword <> ''
        ;"
    # echo $sql_1
    # Export Data
    echo ''
    echo [exporting data from cdp_web_${i}.fpc_raw_data at ${vDate} data]
    mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$src_login_path/$project_name/${table_name}/$project_name.${src_login_path}_${i}_${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/${table_name}/$project_name.${src_login_path}_${i}_${table_name}.error


    export sql_2="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${i} (
            serial bigint(20) unsigned NOT NULL AUTO_INCREMENT unique,
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
            keyword varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面關鍵字',
            created_at datetime NOT NULL COMMENT 'the timestamp'
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDP 用戶標籤主表'   
        ;"
    # Import Data
    echo ''
    echo 'start: ' date
    echo 'importing data from cdp_'${i} 
    mysql --login-path=$dest_login_path -e "$sql_2"
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${table_name}/$project_name.${src_login_path}_${i}_${table_name}.txt' INTO TABLE ${project_name}.${table_name}_${i} IGNORE 1 LINES;" 2>>$error_dir/${project_name}/${table_name}/$project_name.${table_name}_${i}.error 
done

echo ''
echo 'end: ' date
