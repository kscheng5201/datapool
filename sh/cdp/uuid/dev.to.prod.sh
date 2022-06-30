

#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
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
    export sql_each="
        select *
        FROM ${project_name}.accu_mapping_${org_id}
        ;"
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.accu_mapping_${org_id}.txt]
    echo $sql_each
    mysql --login-path=$src_login_true -e "$sql_each" > $export_dir/$src_login_path/$project_name/${project_name}.accu_mapping_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.accu_mapping_${org_id}.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.accu_mapping_${org_id}.txt' INTO TABLE ${project_name}.accu_mapping_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.accu_mapping_${org_id}.error

done < /root/datapool/export_file/cdp/uuid/uuid.cdp_accu_mapping_org_id.txt


export sql_all="
    select *
    FROM ${project_name}.accu_mapping
    ;"
#### Export Data ####
echo ''
echo [start: date on ${vDate}]
echo [exporting data to ${project_name}.accu_mapping.txt]
echo $sql_all
mysql --login-path=$src_login_true -e "$sql_all" > $export_dir/$src_login_path/$project_name/${project_name}.accu_mapping.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.accu_mapping.error

#### Import Data ####
echo ''
echo [start: date on ${vDate}]
echo [import data to ${project_name}.${type_s}_${table_name}]
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.accu_mapping.txt' INTO TABLE ${project_name}.accu_mapping IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.accu_mapping.error





echo ''
echo [end the ${vDate} data on `date`]
