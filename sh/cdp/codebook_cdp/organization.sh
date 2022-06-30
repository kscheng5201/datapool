#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="codebook_cdp"
export table_name="organization"
export src_login_path='cdp'


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

export sql="
        select 
            id, 
            name, 
            industry_id, 
            ip_filter, 
            created_at, 
            updated_at, 
	    now() logging_at
        from cdp_organization.organization
	;"
# echo $sql


# Export Data
echo ''
echo 'start: ' `date`
mysql --login-path=$src_login_path -e "$sql" > $export_dir/$src_login_path/$project_name/$project_name.$table_name.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.$table_name.error

# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=$dest_login_path -e "truncate table $project_name.$table_name; LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.$table_name.txt' INTO TABLE ${project_name}.$table_name IGNORE 1 LINES;" 2>>$error_dir/$project_name.$table_name.error 


# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=${dest_login_path}_prod -e "truncate table $project_name.$table_name; LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.$table_name.txt' INTO TABLE ${project_name}.$table_name IGNORE 1 LINES;" 2>>$error_dir/$project_name.$table_name.error 
