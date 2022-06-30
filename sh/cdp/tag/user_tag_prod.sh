#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="summary_cdp"
export src_login_path="datapool_dev"
export table_name="tag"


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### loop by org_id ####
for org_id in $(seq 1 14)
do 
    export sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id} (
	   serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique,
           stat_date date NOT NULL COMMENT '資料統計日', 
           start_date date NOT NULL COMMENT '90 天起始日',   
           end_date date NOT NULL COMMENT '90 天最末日', 
           db_id tinyint unsigned NOT NULL DEFAULT '0', 
           channel_id int unsigned NOT NULL DEFAULT '0',
           identity int unsigned NOT NULL DEFAULT '0' COMMENT '識別是否同一人',  
           domain varchar(32) NOT NULL DEFAULT '0', 
           channel_type varchar(16) NOT NULL DEFAULT '0',   
           tag varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',  
           tag_freq int unsigned NOT NULL DEFAULT '0' COMMENT '貼標次數',   
           last_at datetime NOT NULL COMMENT '最近貼標時間', 
           ranking int NOT NULL COMMENT '標籤濃度',
           created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '創建時間',
           updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',  
           KEY idx_stat_date (stat_date), 
           KEY idx_domain (domain),
           KEY idx_channel_type (channel_type),   
           KEY idx_tag (tag),  
           KEY idx_channel_id (channel_id),   
           KEY idx_complex (tag,channel_type,domain)  
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='編號 ${org_id} 的客戶，近 90 天的貼標 summary'  
        ;"

    export sql_2="
        select 
	    null serial, 
            stat_date, 
            start_date, 
            end_date, 
            db_id, 
            channel_id, 
            identity, 
            domain, 
            channel_type, 
            tag, 
            tag_freq, 
            last_at, 
            ranking, 
	    now() created_at, 
	    now() updated_at
        from ${project_name}.${table_name}_${org_id}
        ;"
    # Export Data
    echo [DROP TABLE ${project_name}.${table_name}_${org_id} on ${dest_login_path}]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${table_name}_${org_id}; "

    echo ''
    echo [start the ${vDate} data on `date`]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id} on ${dest_login_path}]
    mysql --login-path=$dest_login_path -e "$sql_1"
    
    echo [exporting data from ${table_name}_${org_id} ]
    mysql --login-path=$src_login_path -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_${table_name}.error
    
    # Import Data
    echo ''
    echo [start the ${vDate} data on `date`]
    echo [importing data to ${project_name}.${table_name}_${org_id} on ${dest_login_path}] 
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_${table_name}.txt' INTO TABLE ${project_name}.${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${org_id}_${table_name}.error 

done

echo ''
echo [end the ${vDate} data on `date`]
