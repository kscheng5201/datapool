#!/usr/bin/bash
####################################################
# Project: Streaming Web
# Branch: 取得 table_list
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
export hostname="hadoop@ec2-3-15-214-30.us-east-2.compute.amazonaws.com"

while read org_id; 
do
    while read db_list; 
    do
        while read table_list; 
        do
            #echo ''
            echo ssh -v -i ~/.ssh/emrpoc_idrsa ${hostname} -C \"aws s3 cp stream/${db_list}.${table_list}.csv s3://accuemrs3/hdfs_data/${db_list}.db/${table_list}/\" 
            #ssh -v -i ~/.ssh/emrpoc_idrsa hadoop@ec2-18-224-73-195.us-east-2.compute.amazonaws.com -C "aws s3 cp stream/${db_list}.${table_list}.csv s3://accuemrs3/hdfs_data/${db_list}.db/${table_list}/"
        
        done < $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
