#!/usr/bin/bash
####################################################
# Project: 標籤系統
# Branch: mining_label 上游
# Author: Benson Cheng
# Created_at: 2022-04-20
# Updated_at: 2022-04-20
# Note: 
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="mining_label"
export table_name="rfm_streaming" 
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
        DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id} (
            serial bigint NOT NULL AUTO_INCREMENT COMMENT '流水號' unique,  
            member_id varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            r timestamp COMMENT 'recency, 最近一次消費時間', 
            f int COMMENT 'frequency, 消費頻率', 
            m int COMMENT 'Monetary, 消費總金額', 
            view_time int COMMENT '觸發事件 14: 加入購物車的次數', 
            brain_ratio float COMMENT '腦波強弱指數: f / view_time', 
            created_at DATETIME NOT NULL COMMENT '資料建立時間', 
            updated_at DATETIME NOT NULL COMMENT '資料更新時間',                           
            PRIMARY KEY (member_id) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='member rfm' 
        ;
        
        CREATE TABLE IF NOT EXISTS ${project_name}.data_pipeline (
            serial INT AUTO_INCREMENT NOT NULL COMMENT '流水號' unique, 
            tag_date DATE NOT NULL COMMENT '程式執行日期', 
            table_name VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '資料表名稱', 
            org_id INT NOT NULL COMMENT '廠商編號', 
            data_dt INT NOT NULL COMMENT '資料原始日期', 
            entries INT NOT NULL COMMENT '資料總量', 
            finished_at DATETIME NOT NULL COMMENT '資料表完成時間', 
            created_at DATETIME NOT NULL COMMENT '資料建立時間', 
            updated_at DATETIME NOT NULL COMMENT '資料更新時間', 
            PRIMARY KEY (table_name, org_id, data_dt), 
            KEY idx_table_name (table_name), 
            KEY idx_org_id (org_id), 
            KEY idx_data_dt (data_dt), 
            KEY idx_created_at (created_at),
            KEY idx_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='紀錄 ${table_name} 已完成的狀態表'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}]
    echo $sql_0
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}.error


    export sql_3="
        SET NAMES utf8mb4
        ;
        select 
            null serial,
            member_id, 
            r, 
            f, 
            m, 
            view_time, 
            round(f / view_time, 6) brain_ratio,
            now() created_at, 
            now() updated_at
        from (
            select 
                channel_id, 
                member_id, 
                max(purchased_at) r, 
                sum(if(price >= 1, 1, 0)) f, 
                sum(price) m
            from cdp_${org_id}.item_purchase
            group by 
                channel_id, 
                member_id
            ) a, 
            (
            select 
                crm_unique_id, 
                sum(if(type = 14, 1, 0)) view_time
            from cdp_crm_${org_id}.crm_event_raw_data
            group by crm_unique_id
            ) b
        where a.channel_id = b.crm_unique_id
        ;"
    echo ''
    echo #### Export Data ####
    echo [exporting data to ${project_name}.${table_name}_${org_id}.txt]
    echo $sql_3
    mysql --login-path=${src_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}.error

    echo ''
    echo #### Import Data ####
    echo [import data from ${project_name}.${table_name}_${org_id}.txt to ${project_name}.${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}.txt' REPLACE INTO TABLE ${project_name}.${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}.error 


    export sql_4="
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                '${table_name}_${org_id}' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                now() finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id}
        ;
        
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_4.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt


