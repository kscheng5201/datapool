#!/usr/bin/bash
####################################################
# Project: Streaming Web
# Branch: 複製 fpc_event_raw_data
# Author: Benson Cheng
# Created_at: 2022-02-16
# Updated_at: 2022-02-16
# Note: 
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="stream"
export table_name="fpc_event_raw_data"
export src_login_path="cdp"


while read org_id; 
do 
    while read db_id; 
    do 
        export sql_4="
            CREATE TABLE IF NOT EXISTS ${src_login_path}_web_${db_id}.${table_name} (
                id bigint NOT NULL AUTO_INCREMENT,
                fpc_unique_id int unsigned NOT NULL DEFAULT '0' COMMENT 'fpc_unique 的 id', 
                domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',  
                kind tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件代碼',  
                type tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
                col1 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col2 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col3 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col4 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col5 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col6 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col7 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col8 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col9 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col10 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col11 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col12 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col13 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col14 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                identity int NOT NULL DEFAULT '0' COMMENT '識別是否同一人(還沒綁定fpc_unique_id尾數+0，綁定audience_match尾數+1)',  
                created_at int unsigned NOT NULL DEFAULT '0',  
                PRIMARY KEY (id) USING BTREE,  
                KEY created_at (created_at) USING BTREE,  
                KEY fpc_unique_id (fpc_unique_id) USING BTREE,  
                KEY identity (identity) USING BTREE, 
                KEY kind_type (kind,type) USING BTREE   
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc事件'    
            ;
            CREATE TABLE IF NOT EXISTS ${src_login_path}_web_${db_id}.${table_name}_temp (
                id bigint NOT NULL AUTO_INCREMENT,
                fpc_unique_id int unsigned NOT NULL DEFAULT '0' COMMENT 'fpc_unique 的 id', 
                domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源',  
                kind tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件代碼',  
                type tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
                col1 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col2 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col3 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col4 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col5 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col6 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col7 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col8 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col9 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                col10 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col11 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col12 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col13 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                col14 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,  
                identity int NOT NULL DEFAULT '0' COMMENT '識別是否同一人(還沒綁定fpc_unique_id尾數+0，綁定audience_match尾數+1)',  
                created_at int unsigned NOT NULL DEFAULT '0',  
                PRIMARY KEY (id) USING BTREE,  
                KEY created_at (created_at) USING BTREE,  
                KEY fpc_unique_id (fpc_unique_id) USING BTREE,  
                KEY identity (identity) USING BTREE, 
                KEY kind_type (kind,type) USING BTREE   
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc事件'    
            ;"
        echo ''
        echo [CREATE TABLE IF NOT EXISTS ${src_login_path}_web_${db_id}.${table_name}]
        echo $sql_4
        mysql --login-path=${src_login_path} -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}.error
        

        export sql_5="
            SET NAMES utf8mb4
            ;
            select 
                id, 
                fpc_unique_id, 
                domain, 
                kind, 
                type, 
                col1, 
                col2, 
                col3, 
                col4, 
                col5, 
                col6, 
                col7, 
                col8, 
                col9, 
                col10, 
                col11, 
                col12, 
                col13, 
                col14, 
                identity, 
                created_at
            from cdp_web_${db_id}.${table_name}
                where created_at >= UNIX_TIMESTAMP((now() - interval 1 minute)) 
                and created_at < UNIX_TIMESTAMP((now() - interval 1 minute) + interval 1 minute)
            ;"
        echo ''
        echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}_src.txt]
        echo $sql_5
        mysql --login-path=${src_login_path} -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src.error
        echo ''
        echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}_src.txt to ${project_name}.${type}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src.txt' INTO TABLE cdp_web_${db_id}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_src.error 
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_src.txt' INTO TABLE cdp_web_${db_id}.${table_name}_temp IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_src.error 



    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
