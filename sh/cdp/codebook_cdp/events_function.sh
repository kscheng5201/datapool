#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="codebook_cdp"
export table_name="events_function"
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

export sql="
	SET NAMES utf8mb4
	; 
    select 
        id, 
        kind, 
        type, 
        col1, 
        col2, 
        col3, 
        col4, 
        col5, 
        col6, 
        col7, 
        col8, 
        col9, 
        col10, 
        col11, 
        col12, 
        col13, 
        col14, 
        updated_at
    from cdp_organization.events_function
	;"
# echo $sql


# Export Data
echo ''
echo 'start: ' `date`
mysql --login-path=$src_login_path -e "$sql" > $export_dir/$src_login_path/$project_name/$project_name.$table_name.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.$table_name.error

# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=$dest_login_path -e "truncate table $project_name.$table_name;"
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.$table_name.txt' INTO TABLE ${project_name}.$table_name IGNORE 1 LINES;" 2>>$error_dir/$project_name.$table_name.error 


# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=${dest_login_path}_prod -e "truncate table $project_name.$table_name;"
mysql --login-path=${dest_login_path}_prod -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.$table_name.txt' INTO TABLE ${project_name}.$table_name IGNORE 1 LINES;" 2>>$error_dir/$project_name.$table_name.error 
