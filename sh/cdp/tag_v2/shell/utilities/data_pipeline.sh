#!/usr/bin/bash
####################################################
# Project: 標籤系統
# Branch: triggered_%_etl 大表完成狀態登陸
# Author: Benson Cheng
# Created_at: 2022-05-06
# Updated_at: 2022-05-06
# Note: 供 AWS EMR 確認何時可以開始動作
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tag_v2"
export src_login_path="cdp"
export table_name="triggered"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


while read org_id; 
do 
    export sql_0="
        CREATE TABLE IF NOT EXISTS ${project_name}.data_pipeline (
           serial int NOT NULL AUTO_INCREMENT COMMENT '流水號' unique, 
           tag_date date NOT NULL COMMENT '程式執行日期',        
           table_name varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '資料表名稱',   
           org_id int NOT NULL COMMENT '廠商編號',
           data_dt int NOT NULL COMMENT '資料原始日期',          
           entries int NOT NULL COMMENT '資料總量',
           page_view int NOT NULL COMMENT '來自網頁瀏覽的資料量', 
           event int NOT NULL COMMENT '來自事件觸發的資料量', 
           campaign int NOT NULL COMMENT '來自 campaign 的資料量', 
           api int NOT NULL COMMENT '來自 API 的資料量', 
           finished_at datetime NOT NULL COMMENT '資料表完成時間',
           created_at datetime NOT NULL COMMENT '資料建立時間',  
           updated_at datetime NOT NULL COMMENT '資料更新時間',  
           PRIMARY KEY (table_name, org_id, data_dt),      
           KEY idx_table_name (table_name),  
           KEY idx_org_id (org_id),          
           KEY idx_data_dt (data_dt),        
           KEY idx_created_at (created_at),  
           KEY idx_updated_at (updated_at)   
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='紀錄 triggered_%_etl 已完成的狀態表'           
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.data_pipeline]
    echo $sql_0
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src_sql_0.error


    export sql_1="
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                '${table_name}_${org_id}_etl' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                sum(if(origin = 'page_view', 1, 0)) page_view, 
                sum(if(origin = 'event', 1, 0)) page_view, 
                sum(if(origin = 'campaign', 1, 0)) campaign, 
                sum(if(origin = 'API', 1, 0)) api, 
                max(updated_at) finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id}_etl
            where datetime >= '${vDate}'
                and datetime < '${vDate}' + interval 1 day
        ;        
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_1.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt

echo ''
echo `date`
