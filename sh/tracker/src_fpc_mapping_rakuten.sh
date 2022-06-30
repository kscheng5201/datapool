#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="fpc"
export src_login_path="tracker"
export table_name="cdp_fpc_mapping"


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

for i in $(seq 0 2)
do

export sql_query_1="
    select 
        fpc, 
        title origin_fpc, 
        metaKeyword domain, 
        min(datetime) first_time
    from tracker.landing2_mapping
    where datetime >= date('${vDate}' + interval ${i} day)
        and datetime < date('${vDate}' + interval ${i}+1 day)
	and metaKeyword = 'www.rakuten.com.tw'
    group by 
        fpc, 
        title, 
        metaKeyword
    ;"

echo $sql_query_1

# Export Data
echo ''
echo 'start: ' `date`
echo ${vDate} + ${i} day
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.error

# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.txt' IGNORE INTO TABLE ${src_login_path}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.error 
done

echo ''
echo 'end: ' `date`
