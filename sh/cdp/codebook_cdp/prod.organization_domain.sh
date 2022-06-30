#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="codebook_cdp"
export table_name="organization_domain"
export src_login_path='cdp'


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
            org_id, 
            db_id, 
            domain, 
            domain_type, 
            nickname, 
            created_at, 
            deleted_at
        from cdp_organization.organization_domain
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


export sql_2="
    insert into codebook_cdp.organization_domain
        select 
	    null,
            org_id, 
            db_id, 
            domain, 
            domain_type, 
            nickname, 
            created_at, 
            null
        from instagram.organization_domain
    ;"
echo ''
echo [insert into codebook_cdp.organization_domain by instagram]
mysql --login-path=$dest_login_path -e "$sql_2"
