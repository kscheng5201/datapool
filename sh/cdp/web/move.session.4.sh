#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export src_login_path="cdp"
export src_login_true="datapool_prod"

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


while read org_id; 
do 
    export sql_kpi="
        select *
        FROM web.session_both_4_etl
        ;"
    echo $sql_kpi
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to session_both_4_etl.txt]
#    mysql --login-path=$src_login_true -e "$sql_kpi" > $export_dir/$src_login_path/$project_name/session_both_4_etl.txt 2>>$error_dir/$src_login_path/$project_name/session_both_4_etl.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/session_both_4_etl.txt' INTO TABLE web.session_both_4_etl IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/session_both_4_etl.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt

