#!/usr/bin/bash
####################################################
# Project: Streaming Web
# Branch: 取得 table_list
# Author: Benson Cheng
# Created_at: 2022-02-16
# Updated_at: 2022-02-16
# Note: 
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="stream"
export src_login_path="cdp"

while read org_id; 
do
    while read db_list; 
    do
        while read table_list; 
        do
            export sql_1="
                select *
                from ${db_list}.${table_list}
                where created_at >= unix_timestamp('20220201')
                    and created_at < unix_timestamp('20220201' + interval 1 month)
                ;"
            echo ''
            echo [Get the table_list on web]
            echo $sql_1
            mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/${db_list}.${table_list}.csv 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_table_list.error
            sed -i '1d' $export_dir/$src_login_path/$project_name/${db_list}.${table_list}.csv
            echo ''
            echo $export_dir/$src_login_path/$project_name/${db_list}.${table_list}.csv
            tail $export_dir/$src_login_path/$project_name/${db_list}.${table_list}.csv
        
        done < $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
