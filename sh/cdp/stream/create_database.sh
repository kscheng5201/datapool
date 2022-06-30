#!/bin/bash
####################################################
# Project: Hive practice
# Branch: 
# Author: Benson Cheng
# Created_at: 2022-02-21
# Updated_at: 2022-02-21
# Note: 
#####################################################


export_dir='s3a://accuemrs3poc/hdfs_data'
src_login_path='cdp'
project_name='web'
table_name='fpc_raw_data'
db_id='3'


while read org_id; 
do 
    while read db_id; 
    do 
        echo ''
        echo [ CREATE DATABASE IF NOT EXISTS ${src_login_path}_${project_name}_${db_id} ]
        echo [ 如果不指定 DATABASE，系統就會使用 hive default DATABASE，這 DATABASE會因為 叢集開／關導致無法使用 ]
        echo ##########
        echo "hive -e" "CREATE DATABASE IF NOT EXISTS ${src_login_path}_${project_name}_${db_id} LOCATION '${export_dir}/${src_login_path}_${project_name}_${db_id}/${project_name}'"
    
    done < /root/datapool/export_file/cdp/stream/stream.cdp_${org_id}_db_id.txt
done < /root/datapool/export_file/cdp/stream/stream.cdp_org_id.txt
