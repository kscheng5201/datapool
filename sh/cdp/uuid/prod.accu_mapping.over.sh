#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export table_name="accu_mapping" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### Get DateName (Mon to Sun)
if [ -n "$1" ]; 
then
    vDateName=`date -d $1 '+%a'`
else
    vDateName=`date -d "1 day ago" '+%a'`
fi

#### Get the First and Last Date of Month ####
if [ -n "$1" ]; 
then 
    vMonthFirst=`date -d $1 +"%Y-%m-01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y-%m-01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi



echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_mid]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_mid;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_bfpc_f]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_bfpc_f;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_dump]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_dump;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_bfpc_f]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_bfpc_f;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_dump]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_dump;"

export sql_2="
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_mid (
        serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
        accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
        browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
        first_at datetime DEFAULT NULL COMMENT '此 fpc 首次出現時間',
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
        KEY idx_complex (accu_id, browser_fpc),
        KEY idx_accu_id (accu_id), 
        KEY idx_browser_fpc (browser_fpc)        
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_id vs browser_fpc 跨組織 mapping （中繼）表'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_mid_bfpc_f (
        accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
        browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
        rid int NOT NULL COMMENT 'ranking id',
        KEY idx_rid (rid),
        KEY idx_accu_id (accu_id), 
        KEY idx_browser_fpc (browser_fpc)        
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='當日時間最早 browser fpc 暫存表'
    ;   
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_mid_dump (
        serial int NOT NULL COMMENT 'auto_increment Serial Number',
        accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
        browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
        count int NOT NULL COMMENT 'count number',
        KEY idx_serial (serial),
        KEY idx_accu_id (accu_id), 
        KEY idx_browser_fpc (browser_fpc), 
        KEY idx_both (accu_id, browser_fpc)         
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_id 與 browser fpc 同時重複出現的暫存'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_bfpc_f (
        accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
        browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
        rid int NOT NULL COMMENT 'ranking id',
        KEY idx_rid (rid),
        KEY idx_accu_id (accu_id), 
        KEY idx_browser_fpc (browser_fpc)        
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='當日時間最早 browser fpc 暫存表'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_dump (
        serial int NOT NULL COMMENT 'auto_increment Serial Number',
        accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
        browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
        count int NOT NULL COMMENT 'count number',
        KEY idx_serial (serial),
        KEY idx_accu_id (accu_id), 
        KEY idx_browser_fpc (browser_fpc), 
        KEY idx_both (accu_id, browser_fpc)         
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_id 與 browser fpc 同時重複出現的暫存'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name} (
        serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
        accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
        browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',
        first_at datetime DEFAULT NULL COMMENT '此 fpc 首次出現時間',
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
        KEY idx_complex (accu_id, browser_fpc),
        KEY idx_accu_id (accu_id), 
        KEY idx_browser_fpc (browser_fpc)        
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_id vs browser_fpc 跨組織 mapping （匯整）表'
    ;"
echo ''
echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_src]
echo $sql_2
mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_src.error


while read org_id; 
do 
    export sql_3="
        INSERT INTO ${project_name}.${table_name}_mid
            select 
                null serial, 
                accu_id, 
                browser_fpc, 
                min(first_at) first_at,
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id}
            where browser_fpc is not null
                and browser_fpc <> ''
                and updated_at >= '${vDate}' + interval 1 day
            group by accu_id, browser_fpc
        ;
        ALTER TABLE ${project_name}.${table_name}_mid AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [寫入 ${project_name}.${table_name}_mid 表內的當天資料]
    echo [INSERT INTO ${vDate} data on ${project_name}.${table_name}_${org_id}_mid]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_mid.error        

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt


export sql_4="            
    INSERT INTO ${project_name}.${table_name}_mid_bfpc_f
        select accu_id, browser_fpc, row_number() over (partition by browser_fpc order by first_at) rid
        from ${project_name}.${table_name}_mid
    ;
    
    DELETE
    FROM ${project_name}.${table_name}_mid_bfpc_f
    WHERE rid >= 2
    ;

    UPDATE ${project_name}.${table_name}_mid a
        INNER JOIN ${project_name}.${table_name}_mid_bfpc_f b
        ON a.browser_fpc = b.browser_fpc      
    SET a.accu_id = b.accu_id
    ;"
echo ''
echo [整理 ${project_name}.${table_name}_mid：同個 browser_fpc，取時間最早的 accu_id]
echo [UPDATE ${project_name}.${table_name}_mid]
echo $sql_4
mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_mid.error        


export sql_4a="            
    INSERT INTO ${project_name}.${table_name}_mid_dump
        select min(serial) serial, accu_id, browser_fpc, count(*)
        from ${project_name}.${table_name}_mid
        group by accu_id, browser_fpc
        having count(*) >= 2    
    ;

    USE ${project_name}
    DELETE a
    FROM ${project_name}.${table_name}_mid a
        INNER JOIN ${project_name}.${table_name}_mid_dump b
        ON a.accu_id = b.accu_id
            AND a.browser_fpc = b.browser_fpc
    WHERE a.serial <> b.serial
    ;
    ALTER TABLE ${project_name}.${table_name}_mid AUTO_INCREMENT = 1
    ;"
echo ''
echo [刪除 ${project_name}.${table_name}_mid：同個 browser_fpc，accu_id]
echo [DELETE ${project_name}.${table_name}_mid]
echo $sql_4a
mysql --login-path=$dest_login_path -e "$sql_4a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_mid_delete.error        


export sql_5="            
     INSERT INTO ${project_name}.${table_name}
        select 
            null serial, 
            b.accu_id, 
            b.browser_fpc, 
            b.first_at, 
            b.created_at, 
            b.updated_at
        from ${project_name}.${table_name} a
            RIGHT JOIN ${project_name}.${table_name}_mid b
            ON a.browser_fpc = b.browser_fpc
        where a.browser_fpc is null
            or a.browser_fpc = ''
    ;
    ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1
    ;"
echo ''
echo [未存在 ${project_name}.${table_name} 表內的資料：寫入]
echo [INSERT ${vDate} data INTO ${project_name}.${table_name}]
echo $sql_5
mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}.error        


export sql_6="            
    INSERT INTO ${project_name}.${table_name}_bfpc_f
        select accu_id, browser_fpc, row_number() over (partition by browser_fpc order by first_at) rid
        from ${project_name}.${table_name}    
    ;
    
    DELETE
    FROM ${project_name}.${table_name}_bfpc_f
    WHERE rid >= 2
    ;
    
    UPDATE ${project_name}.${table_name} a
        INNER JOIN ${project_name}.${table_name}_bfpc_f b
        ON a.browser_fpc = b.browser_fpc      
    SET a.accu_id = b.accu_id
    ;
    ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1
    ;"
echo ''
echo [已存在 ${project_name}.${table_name} 表內的資料：更新]
echo [UPDATE ${vDate} data INTO ${project_name}.${table_name}]
echo $sql_6
mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}.error        


export sql_6a="            
    INSERT INTO ${project_name}.${table_name}_dump
        select min(serial) serial, accu_id, browser_fpc, count(*)
        from ${project_name}.${table_name}
        group by accu_id, browser_fpc
        having count(*) >= 2   
    ;

    USE ${project_name}
    DELETE a
    FROM ${project_name}.${table_name} a
        INNER JOIN ${project_name}.${table_name}_dump b
        ON a.accu_id = b.accu_id
            AND a.browser_fpc = b.browser_fpc
    WHERE a.serial <> b.serial
    ;
    ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1
    ;"
echo ''
echo [刪除 ${project_name}.${table_name}：同個 browser_fpc，accu_id]
echo [DELETE ${project_name}.${table_name}]
echo $sql_6a
mysql --login-path=$dest_login_path -e "$sql_6a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_delete.error        

while read org_id; 
do 
    export sql_7="            
        UPDATE ${project_name}.${table_name}_${org_id} a
            INNER JOIN ${project_name}.${table_name} b
            ON a.browser_fpc = b.browser_fpc      
        SET a.accu_id = b.accu_id
        ;
        ALTER TABLE ${project_name}.${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [將跨客戶整合後的 browser_fpc，其 accu_id 再整合回 ${project_name}.${table_name}_${org_id} 表內：更新]
    echo [UPDATE ${vDate} data INTO ${project_name}.${table_name}_${org_id}]
    echo $sql_7
    mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}.error        

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt


echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_mid]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_mid;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_bfpc_f]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_bfpc_f;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_dump]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_mid_dump;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_bfpc_f]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_bfpc_f;"
echo ''
echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_dump]
mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_dump;"
