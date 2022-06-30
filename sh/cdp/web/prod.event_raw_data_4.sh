#!/usr/bin/bash
####################################################
# Project: Web 互動分析儀表板
# Branch: fpc_event_raw_data 標記 session
# Author: Benson Cheng
# Created_at: 2022-04-18
# Updated_at: 2022-04-18
# Note: 主程式
#####################################################
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type_s="session"
export table_name="event" 
export src_login_path="cdp"


while read org_id; 
do
    export sql_0="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_raw_data_${org_id} (
           id int NOT NULL COMMENT '原始 id',
           token varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'token', 
           token_type varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'token 類型。web-fpc; line-token; app-token 等',
           accu_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id', 
           domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源', 
           type tinyint unsigned NOT NULL DEFAULT '0' COMMENT '事件功能代碼', 
           event varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '事件名稱',
           col1 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col2 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col3 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col4 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col5 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col6 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col7 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col8 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col9 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
           col10 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,   
           col11 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,   
           col12 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,   
           col13 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,   
           col14 varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,   
           session int unsigned NOT NULL COMMENT '%Y%m%d + session',
           created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間', 
           updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間', 
           PRIMARY KEY (id, domain) USING BTREE,
           KEY idx_created_at (created_at) USING BTREE, 
           KEY idx_type (type) USING BTREE,
           KEY idx_token (token) USING BTREE   
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='fpc_event_raw_data 標記 session'   
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_raw_data_${org_id}]
    echo $sql_0
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_raw_data_${org_id}_sql_0.error



    export sql_1="
        INSERT INTO ${project_name}.${table_name}_raw_data_${org_id}
            select 
                id, 
                fpc token,
                'fpc' token_type,
                accu_id, 
                domain, 
                type, 
                event, 
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
                session, 
                created_at, 
                updated_at
            from ${project_name}.${type_s}_${table_name}_${org_id}_etl
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${table_name}_raw_data_${org_id}]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_raw_data_${org_id}_sql_1.error
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id_4.txt

echo ''
echo [end the ${vDate} data on `date`]


    
    
