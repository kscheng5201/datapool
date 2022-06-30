#!/usr/bin/bash
####################################################
# Project: Streaming Web
# Branch: 取得 org_id
# Author: Benson Cheng
# Created_at: 2022-02-16
# Updated_at: 2022-02-16
# Note: 
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="stream"
export src_login_path="cdp"


#### loop by org_id ####
export sql_0="
    select id
    from cdp_organization.organization
    where id = 4
    ;"    
echo ''
echo [Get the org_id]
echo $sql_0
mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
