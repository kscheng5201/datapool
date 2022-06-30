#!/usr/bin/bash
PATH=/bin:/sbin:/usr/bin:/usr/sbin
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web_event_integrated"
export stakeholder="tmnewa" # 新安東京海上產險
export src_login_path_1='tracker'
export src_login_path_2='cdp'

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y%m%d"`
fi

# export partition='pMon pTue pWed pThu pFri pSat pSun'

# for i in $partition
# do

export sql_query_1="
        select now() + interval 1 day, 'I am good!' as txt
        ;"
# echo $sql_query_1

# Export Data
echo 'on partition(${i})'
echo 'start: ' `date` 
echo 'start: ' `date` > $error_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_${stakeholder}.error
mysql --login-path=$src_login_path_1 -e "$sql_query_1" > $export_dir/$src_login_path_2/$project_name/$project_name.test.txt 2>>$error_dir/$src_login_path_2/$project_name/$project_name.test.error

echo ''
echo $export_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_${stakeholder}.txt
# Import Data
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path_2/$project_name/$project_name.test.txt' INTO TABLE ${project_name}.test IGNORE 1 LINES;" 2>>$error_dir/$project_name.test.error 

# done
