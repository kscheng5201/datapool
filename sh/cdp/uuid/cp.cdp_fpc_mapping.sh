#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="datapool"
export table_name="cdp_fpc_mapping"
export fake_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

export sql_1="
    select *
    from ${project_name}.${table_name}
    where first_at >= '${vDate}'
        and first_at < '${vDate}' + interval 1 day
    ;"
echo ''
echo [Export ${vData} Data from ${src_login_path}.${table_name} at `date`]
echo $sql_1
#mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$fake_login_path/$project_name/${project_name}.${table_name}.txt 2>>$error_dir/$fake_login_path/$project_name/${project_name}.${table_name}.error

echo ''
echo [Import ${vData} Data IGNORE INTO TABLE ${project_name}.${table_name}]
#mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$fake_login_path/$project_name/${project_name}.${table_name}.txt' INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$fake_login_path/$project_name/$project_name.${table_name}.error 


echo ''
echo [end at `date`]
