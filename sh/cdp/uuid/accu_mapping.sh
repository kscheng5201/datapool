#!/usr/bin/bash
export dest_login_path="datapool"
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
echo [beforehand: DROP TABLE ${project_name}.${src_login_path}_${table_name}_src]
mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${src_login_path}_src;"

while read org_id; 
do 
    export sql_2="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_src (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 mysql uuid() 機制所產生的 id',
            org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)',
            db_id tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '資料來自哪個DB', 
            channel_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'unique_id/channel_id',
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc/channel_id 首次出現時間',
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間',
            PRIMARY KEY (serial),
            KEY idx_fpc (fpc),
            KEY idx_org_id (org_id),
            KEY idx_channel (channel),
            KEY idx_channel_id (channel_id),
            KEY idx_db_id (db_id),
            KEY idx_member_id (member_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='fpc_unique_data 每日新建或更新的資料'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_src]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_src.error
        
        
    while read db_id; 
    do 
        export sql_3="
            select 
                null serial, 
                uuid() accu_id, 
                ${org_id} org_id,
                'web' channel,
                ${db_id} db_id, 
                b.id unique_id, 
                fpc,
                a.member_id, 
                from_unixtime(a.fpc_unique_created_at) first_at,
                from_unixtime(if(c.registered_at >= 1 and c.registered_at < c.created_at, c.registered_at, c.created_at)) registered_at,
                from_unixtime(a.fpc_unique_created_at) created_at, 
                from_unixtime(a.updated_at) updated_at
            from cdp_web_${db_id}.fpc_unique_data a
            
                left join cdp_web_${db_id}.fpc_unique b
                on a.fpc_unique_id = b.id
                
                left join 
                (
                select *
                from cdp_${org_id}.audience_data 
                where channel_type = 1
                    and db_id = ${db_id}
                ) c
                on b.id = c.channel_id
                
            where (a.fpc_unique_created_at >= unix_timestamp('${vDate}')
                and a.fpc_unique_created_at < unix_timestamp('${vDate}' + interval 1 day)
                 or a.updated_at >= unix_timestamp('${vDate}')
                and a.updated_at < unix_timestamp('${vDate}' + interval 1 day)
                )
            ;"
            
        #### Export Data ####
        echo ''
        echo [Export ${vDate} Data FROM cdp_web_${db_id}.fpc_unique_data and cdp_web_${db_id}.fpc_unique]
        echo $sql_3
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_${org_id}_${db_id}_src.error
   
        #### Import Data ####
        echo ''
        echo [INSERT ${vDate} data INTO ${project_name}.${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_${org_id}_${db_id}_src.txt' INTO TABLE ${project_name}.${table_name}_src IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${src_login_path}_${table_name}_src.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_db_id.txt
    #done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_accu_mapping_${org_id}_db_id.txt


    export sql_4="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_mid (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 mysql uuid() 機制所產生的 id',  
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)', 
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            member_id varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc/token 首次出現時間', 
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則為綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',  
            PRIMARY KEY (accu_id, channel, id, id_type),
            KEY idx_accu_id (accu_id),
            KEY idx_channel (channel),
            KEY idx_id (id),
            KEY idx_id_type (id_type),
            KEY idx_first_at (first_at),
            KEY idx_registered_at (registered_at) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_mapping 當日總表（distinct 前）' 
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_pre (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 mysql uuid() 機制所產生的 id',  
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)', 
            tracker_id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '來自 tracker mapping 後所得的 fpc',
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            member_id varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc/token 首次出現時間', 
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則為綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',  
            PRIMARY KEY (accu_id, channel, id, id_type),
            KEY idx_accu_id (accu_id),
            KEY idx_channel (channel),
            KEY idx_id (id),
            KEY idx_id_type (id_type),
            KEY idx_first_at (first_at),
            KEY idx_registered_at (registered_at) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_mapping 當日總表（distinct 後）' 
	;
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name} (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 mysql uuid() 機制所產生的 id',  
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)', 
            tracker_id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '來自 tracker mapping 後所得的 fpc',
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            member_id varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc/token 首次出現時間', 
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則為綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',  
            PRIMARY KEY (accu_id, channel, id, id_type),
            KEY idx_accu_id (accu_id),
            KEY idx_channel (channel),
            KEY idx_id (id),
            KEY idx_id_type (id_type),
            KEY idx_first_at (first_at),
            KEY idx_registered_at (registered_at) 
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='accu_mapping 總表' 
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_pre]
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_pre.error

    export sql_5="
        INSERT INTO ${project_name}.${table_name}_mid
            select 
                null serial,
                max(accu_id) accu_id, 
                channel, 
                fpc id,
                'fpc' id_type,
                max(member_id) member_id, 
                min(first_at) first_at, 
                min(registered_at) registered_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_src
            group by 
                channel, 
                fpc
        ;"
    echo ''
    echo [INSERT ${vDate} data INTO ${project_name}.${table_name}_mid]    
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$project_name/${project_name}.${table_name}_mid.error 

    export sql_6="
        ## tracker 與 CDP 都有資料
        INSERT INTO ${project_name}.${table_name}_pre
            select 
                null serial,
                accu_id, 
                'web' channel,
                tracker_id, 
                id, 
                id_type, 
                member_id, 
                if(a.first_at < b.first_at, a.first_at, b.first_at) first_at, 
                registered_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.tracker_mapping a
                inner join ${project_name}.${table_name}_mid b
                on a.fpc = b.id
            where b.id_type = 'fpc'
        ;
        ALTER TABLE ${project_name}.${table_name}_pre AUTO_INCREMENT = 1
        ;
        
        ## 僅 CDP 有資料
        INSERT INTO ${project_name}.${table_name}_pre
            select 
                null serial,
                accu_id, 
                'web' channel,
                null tracker_id, 
                id, 
                id_type, 
                member_id, 
                b.first_at, 
                registered_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.tracker_mapping a
                right join ${project_name}.${table_name}_mid b
                on a.fpc = b.id
            where b.id_type = 'fpc'
                and a.fpc is null
        ;
        ALTER TABLE ${project_name}.${table_name}_pre AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [tracker 與 CDP 都有資料] 或 [僅 CDP 有資料]
    echo [INSERT ${vDate} data INTO ${project_name}.${table_name}_pre]    
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$project_name/${project_name}.${table_name}_pre.error 

    echo ''
    echo [DROP TABLE ${project_name}.${src_login_path}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${src_login_path}_${table_name}_${org_id}_src;"

#done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_accu_mapping_org_id.txt
done < /root/datapool/export_file/cdp/uuid/uuid.cdp_accu_mapping_org_id.txt

export sql_7="
    UPDATE ${project_name}.${table_name}_pre a
        INNER JOIN
        (
        select accu_id, tracker_id
        from (
            select accu_id, tracker_id, row_number() over (partition by tracker_id order by first_at) rid
            from ${project_name}.${table_name}_pre
            where tracker_id is not null
            ) bb
        where rid = 1
        ) b
        ON a.tracker_id = b.tracker_id

        INNER JOIN
        (
        select *
        from ${project_name}.${table_name}_pre
        where tracker_id is not null
        ) c
        ON a.tracker_id = c.tracker_id
            and a.id = c.id
            and a.channel = c.channel
            and a.id_type = c.id_type                
    SET a.accu_id = b.accu_id, 
        a.member_id = c.member_id, 
        a.registered_at = c.registered_at
    ;"
echo ''
echo [整理同個 tracker_id 的資料]
echo [UPDATE ${project_name}.${table_name}_pre]
echo ''
echo $sql_7
mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$project_name/${project_name}.${table_name}.error 

export sql_8="
    ## 刪除重複綁定的 fpc 資料 ##
    DELETE 
    FROM ${project_name}.${table_name} a
        INNER JOIN
        (
        select 
        from ${project_name}.${table_name}_pre
        where member_id is not null
        ) b
        ON a.tracker_id = b.tracker_id
            and a.id = b.id
            and a.id_type = b.id_type
            and a.channel = b.channel
            and a.member_id <> b.member_id
    WHERE a.member_id is not null
    ;
    ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1
    ;"
echo ''
echo [刪除重複綁定的 fpc 資料]
echo [UPDATE ${vDate} data INTO ${project_name}.${table_name}]
echo $sql_8
mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$project_name/${project_name}.${table_name}.error 

export sql_9="
    ## 未存在表內的資料：寫入 ##
    INSERT INTO ${project_name}.${table_name} 
        select b.*
        from ${project_name}.${table_name} a
            RIGHT JOIN
            (
            select *
            from ${project_name}.${table_name}_pre
            ) b
            ON a.id = b.id
                and a.channel = b.channel
                and a.id_type = b.id_type
        where a.id is null
    ;"
echo ''
echo [未存在表內的資料：寫入]
echo [INSERT ${vDate} data INTO ${project_name}.${table_name}]
echo $sql_9
mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$project_name/${project_name}.${table_name}.error 

export sql_10="
    ## 未存在表內的資料：寫入 ##
    UPDATE ${project_name}.${table_name} a
        INNER JOIN
        (
        select *
        from ${project_name}.${table_name}_pre
        where member_id is not null
        ) b
        ON a.id = b.id
            and a.channel = b.channel
            and a.id_type = b.id_type
    SET a.member_id = b.member_id, 
        a.registered_at = b.registered_at, 
        a.tracker_id = b.tracker_id
    WHERE a.member_id is null
    ;"
echo ''
echo [已存在表內的資料：更新]
echo [INSERT ${vDate} data INTO ${project_name}.${table_name}]
echo $sql_10
mysql --login-path=$dest_login_path -e "$sql_10" 2>>$error_dir/$project_name/${project_name}.${table_name}.error 



echo ''
echo [DROP TABLE ${project_name}.${table_name}_mid]
echo [DROP TABLE ${project_name}.${table_name}_pre]
mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${table_name}_mid;"
mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${table_name}_pre;"

echo ''
echo [End ${table_name} job on ${vDate} at `date`]
