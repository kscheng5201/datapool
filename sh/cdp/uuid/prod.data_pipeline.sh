echo `date`
#!/usr/bin/bash
####################################################
# Project: 愛酷 ID
# Branch: uuid 完成狀態登錄
# Author: Benson Cheng
# Created_at: 2022-04-07
# Updated_at: 2022-04-11
# Note: 供 AWS EMR 確認何時可以開始動作
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export table_name="accu_mapping" 
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
            fpc INT DEFAULT NULL COMMENT 'fpc 資料總量', 
            line INT DEFAULT NULL COMMENT 'line 資料總量', 
            messenger INT DEFAULT NULL COMMENT 'messenger 資料總量', 
            app INT DEFAULT NULL COMMENT 'app 資料總量', 
            crn INT DEFAULT NULL COMMENT 'crn 資料總量',                         
            finished_at DATETIME NOT NULL COMMENT '資料表完成時間', 
            created_at DATETIME NOT NULL COMMENT '資料建立時間', 
            updated_at DATETIME NOT NULL COMMENT '資料更新時間', 
            PRIMARY KEY (table_name, org_id, data_dt), 
            KEY idx_table_name (table_name), 
            KEY idx_org_id (org_id), 
            KEY idx_data_dt (data_dt), 
            KEY idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='紀錄 ${table_name} 已完成的狀態表'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.data_pipeline]
    echo $sql_0
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_0.error


    export sql_1="
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                '${table_name}_${org_id}' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                sum(if(id_type = 'fpc', 1, 0)) fpc, 
                sum(if(id_type = 'line', 1, 0)) line,  
                sum(if(id_type = 'messenger', 1, 0)) messenger, 
                sum(if(id_type = 'app', 1, 0)) app, 
                sum(if(id_type = 'crm', 1, 0)) crm, 
                max(updated_at) finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id}
            where first_at >= '${vDate}'
                and first_at < '${vDate}' + interval 1 day
        ;
        
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_sql_1.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt



export sql_2="
    INSERT INTO ${project_name}.data_pipeline
        select 
            null serial,
            '${vDate}' + interval 1 day tag_date, 
            '${table_name}' table_name, 
            0 org_id, 
            '${vDate}' data_dt, 
            count(*) entries, 
            null, 
            null, 
            null, 
            null, 
            null,             
            max(updated_at) finished_at, 
            now() created_at, 
            now() updated_at
        from ${project_name}.${table_name}
        where first_at >= '${vDate}'
            and first_at < '${vDate}' + interval 1 day
    ;
    
    ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
    ;"
echo ''
echo [INSERT INTO ${project_name}.data_pipeline]
echo $sql_2
mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_2.error



echo ''
echo `date`
