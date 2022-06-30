#!/usr/bin/bash
####################################################
# Project: Streaming Web
# Branch: 取得 all_db
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

while read org_id; 
do 
    export sql_1="
        select concat_ws('_', 'cdp', domain_type, db_id)
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and deleted_at is null

#        UNION ALL
        
#        select concat_ws('_', 'cdp', '${org_id}')
    	;"
    echo ''
    echo [Get the all_db on web]
    echo $sql_1
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
    echo ''
    echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
