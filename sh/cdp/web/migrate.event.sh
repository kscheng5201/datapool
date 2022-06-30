#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type_p="person"
export type_s="session"
export table_name="event" 
export src_login_path="cdp"
export src_login_true="datapool"

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
    export sql_1="
        select null, tag_date + interval 1 month, span, start_date + interval 1 month, end_date + interval 1 month, domain, event, attribute, content, freq, ranking, time_flag, created_at, updated_at
        from ${project_name}.${type_s}_${table_name}_${org_id}
        ;"
    echo $sql_3
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.${type_s}_${table_name}_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_1" > $export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.txt' INTO TABLE ${project_name}.${type_s}_${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error


    export sql_2="
        select null, tag_date + interval 1 month, span, start_date + interval 1 month, end_date + interval 1 month, domain, event, attribute, content, user, ranking, time_flag, created_at, updated_at
        from ${project_name}.${type_p}_${table_name}_${org_id}
        ;"
    echo $sql_2
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.${type_p}_${table_name}_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_2" > $export_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_p}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}.txt' INTO TABLE ${project_name}.${type_p}_${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type_p}_${table_name}_${org_id}.error


    export sql_3="   
        select null, tag_date + interval 1 month, span, start_date + interval 1 month, end_date + interval 1 month, fpc, domain, event, attribute, content, created_at, updated_at
        from ${project_name}.${type_p}_${table_name}_${org_id}_fpc
        ;"
    echo $sql_3
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.${type_p}_${table_name}_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_fpc.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_fpc.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_p}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_fpc.txt' INTO TABLE ${project_name}.${type_p}_${table_name}_${org_id}_fpc IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type_p}_${table_name}_${org_id}_fpc.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt
#done < /root/datapool/export_file/cdp/web/web.cdp_org_id.txt

echo ''
echo [end the ${vDate} data on `date`]
