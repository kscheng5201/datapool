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
        export sql_1="
            select table_name
            from information_schema.tables
            where table_schema = '${db_list}'
            ;"
        echo ''
        echo [Get the table_list on web]
        echo $sql_1
        mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt
        echo ''
        echo $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt
        cat $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt
        
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
