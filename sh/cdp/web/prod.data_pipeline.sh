#!/usr/bin/bash
####################################################
# Project: 互動分析儀表板儀表板
# Branch: session_both 大表完成狀態登陸
# Author: Benson Cheng
# Created_at: 2022-04-07
# Updated_at: 2022-04-07
# Note: 供 AWS EMR 確認何時可以開始動作
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type="session"
export table_name="both" 
export src_login_path="cdp"


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
            serial INT AUTO_INCREMENT NOT NULL COMMENT '流水號' unique, 
            tag_date DATE NOT NULL COMMENT '程式執行日期', 
            table_name VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '資料表名稱', 
            org_id INT NOT NULL COMMENT '廠商編號', 
            data_dt INT NOT NULL COMMENT '資料原始日期', 
            entries INT NOT NULL COMMENT '資料總量', 
            page_view INT NOT NULL COMMENT '來自網頁瀏覽的資料量', 
            event INT NOT NULL COMMENT '來自事件觸發的資料量', 
            finished_at DATETIME NOT NULL COMMENT '資料表完成時間', 
            created_at DATETIME NOT NULL COMMENT '資料建立時間', 
            updated_at DATETIME NOT NULL COMMENT '資料更新時間', 
            PRIMARY KEY (table_name, org_id, data_dt), 
            KEY idx_table_name (table_name), 
            KEY idx_org_id (org_id), 
            KEY idx_data_dt (data_dt), 
            KEY idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='紀錄 ${type}_${table_name}_etl 已完成的狀態表'
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
                '${type}_${table_name}_${org_id}_etl' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                sum(if(behavior = 'page_view', 1, 0)) page_view, 
                sum(if(behavior = 'event', 1, 0)) event, 
                max(updated_at) finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_etl
        ;        
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_1.error


    export sql_2="        
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                '${type}_event_${org_id}_etl' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                0 page_view, 
                count(*) event, 
                max(updated_at) finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_event_${org_id}_etl
        ;
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_2.error


    export sql_3="
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                'event_raw_data_${org_id}' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                0 page_view, 
                count(*) event, 
                max(updated_at) finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.event_raw_data_${org_id}
            where created_at >= '${vDate}'
                and created_at < '${vDate}' + interval 1 day
        ;
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_3.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt

echo ''
echo `date`
