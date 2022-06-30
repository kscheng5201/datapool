#!/usr/bin/bash
##################################################
# Project: 資料轉移 dev to prod
# Branch: bots_lifespan
# Author: Benson Cheng
# Created_at: 2021-12-23
# Updated_at: 2021-12-23
# Note: 若正式版本有問題，則以此版本作為修改基礎
##################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="bots_lifespan"
export project="nes"
export src_login_path="nix"
export src_login_true="datapool"

export sql_1="
    select (table_name) table_list
    from information_schema.tables
    where table_schema = 'bots_lifespan'
        and table_name REGEXP 'interaction|lifespan|lifecycle_days'
        and table_name NOT REGEXP 'old|temp|origin|try'
    ;"
echo ''
echo [get the table_list from datapool dev on bots_lifespan]
mysql --login-path=${src_login_true} > $export_dir/$src_login_path/$project_name/$project_name.table_list.txt
echo $export_dir/$src_login_path/$project_name/$project_name.table_list.txt
tail $export_dir/$src_login_path/$project_name/$project_name.table_list.txt

while read table_list;
do 
    export sql_2="
        select *
        from ${project_name}.${table_list}
        ;"
    echo ''
    echo [export data from ${table_list}]
    mysql --login-path=$src_login_true > $export_dir/$src_login_path/$project_name/$project_name.${table_list}.txt
    echo $export_dir/$src_login_path/$project_name/$project_name.${table_list}.txt
    tail $export_dir/$src_login_path/$project_name/$project_name.${table_list}.txt
    
    echo ''
    echo [LOAD DATA LOCAL INFILE $export_dir/$src_login_path/$project_name/$project_name.${table_list}.txt INTO TABLE ${src_login_path}_${project}.${table_list} IGNORE 1 LINES]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${table_list}.txt' INTO TABLE ${src_login_path}_${project}.${table_list} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_list}.error
    
done < $export_dir/$src_login_path/$project_name/$project_name.table_list.txt
