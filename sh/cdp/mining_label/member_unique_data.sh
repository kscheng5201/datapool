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
export channel="crm"
export table_name="unique_data" 
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
        CREATE TABLE IF NOT EXISTS ${project_name}.member_${table_name}_${org_id} (
            serial bigint NOT NULL AUTO_INCREMENT COMMENT '流水號' unique,  
            member_id varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            sex varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '性別：男-女-無資料',
            birth date NOT NULL COMMENT '生日(ex:2020-01-01)',
            level varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員等級',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
            crm_unique_created_at int unsigned NOT NULL DEFAULT '0' COMMENT 'crm_unique 的 created_at',    
            updated_at int unsigned NOT NULL DEFAULT '0' COMMENT '資料時間',                
            PRIMARY KEY (member_id) USING BTREE,  
            KEY idx_sex (sex) USING BTREE,
            KEY idx_birth (birth) USING BTREE,
            KEY idx_level (level) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc事件(包含 fpc)' 
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


    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${channel}'
            and deleted_at is null
    	;"
    echo ''
    echo [Get the db_id on web]
    echo $sql_1
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    echo ''
    echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    

    while read db_id; 
    do 
        export sql_3="
            SET NAMES utf8mb4
            ;
            select 
                null serial,
                member_id, 
                case gender
                    when 'male'   then '男'
                    when 'female' then '女'
                    else '無資料'
                end sex, 
                birth, 
                level, 
                crm_unique_created_at,
                updated_at
            from cdp_${channel}_${db_id}.${channel}_${table_name}
            where (crm_unique_created_at >= unix_timestamp('${vDate}')
                and crm_unique_created_at < unix_timestamp('${vDate}' + interval 1 day))
                 or (updated_at >= unix_timestamp('${vDate}')
                and updated_at < unix_timestamp('${vDate}' + interval 1 day))
            ;"
        echo ''
        echo #### Export Data ####
        echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}.txt]
        echo $sql_3
        mysql --login-path=${src_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.error

        echo ''
        echo #### Import Data ####
        echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}.txt to ${project_name}.member_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt' REPLACE INTO TABLE ${project_name}.member_${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    
    
    export sql_4="
        INSERT INTO ${project_name}.data_pipeline
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date, 
                'member_${table_name}_${org_id}' table_name, 
                '${org_id}' org_id, 
                '${vDate}' data_dt, 
                count(*) entries, 
                now() finished_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.member_${table_name}_${org_id}
            where crm_unique_created_at >= unix_timestamp('${vDate}')
                and crm_unique_created_at < unix_timestamp('${vDate}' + interval 1 day)
        ;
        
        ALTER TABLE ${project_name}.data_pipeline AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.data_pipeline]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.data_pipeline_sql_4.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt


