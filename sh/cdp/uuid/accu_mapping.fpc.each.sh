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


while read org_id; 
do 
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_src;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_bfpc_f]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_bfpc_f;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f;"




    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'web'
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path}_master -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_db_id.txt 2>> $error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_db_id.txt


    export sql_2="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_src (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
            org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)',
            db_id tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '資料來自哪個DB', 
            channel_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'unique_id/channel_id',
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc/channel_id 首次出現時間',
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間',
            PRIMARY KEY (serial),
            KEY idx_id (id),
            KEY idx_id_type (id_type),
            KEY idx_org_id (org_id),
            KEY idx_channel (channel),
            KEY idx_channel_id (channel_id),
            KEY idx_db_id (db_id),
            KEY idx_member_id (member_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='fpc_unique_data 每日新建或更新的資料(原始表)'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_mid (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)',
            org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',
            db_id tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '資料來自哪個DB', 
            channel_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'unique_id/channel_id',
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc 首次出現時間',
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (serial),
            KEY idx_id (id),
            KEY idx_id_type (id_type),
            KEY idx_org_id (org_id),
            KEY idx_channel (channel),
            KEY idx_channel_id (channel_id),
            KEY idx_db_id (db_id),
            KEY idx_member_id (member_id), 
            KEY idx_browser_fpc (browser_fpc)        
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='fpc_unique_data 每日新建或更新的資料（中繼表）'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_mid_bfpc_f (
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
            browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
            rid int NOT NULL COMMENT 'ranking id',
            KEY idx_rid (rid),
            KEY idx_accu_id (accu_id), 
            KEY idx_browser_fpc (browser_fpc)        
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='當日時間最早 browser fpc 暫存表'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f (
            browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            rid int NOT NULL COMMENT 'ranking id',
            KEY idx_rid (rid),
            KEY idx_member_id (member_id), 
            KEY idx_browser_fpc (browser_fpc)        
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='當日時間最早 member_id 暫存表'
        ;        
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id} (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 ##mysql uuid() 機制所產生的 id',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),app(4),crm(5)',
            org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',
            db_id tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '資料來自哪個DB', 
            channel_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'unique_id/channel_id',
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            first_at datetime DEFAULT NULL COMMENT '此 fpc 首次出現時間',
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
            PRIMARY KEY (serial),
            KEY idx_id (id),
            KEY idx_id_type (id_type),
            KEY idx_org_id (org_id),
            KEY idx_channel (channel),
            KEY idx_channel_id (channel_id),
            KEY idx_db_id (db_id),
            KEY idx_member_id (member_id), 
            KEY idx_browser_fpc (browser_fpc)        
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='fpc_unique_data 每日新建或更新的資料（彙整表）'
    	;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_src]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_sql_2.error


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
                fpc id,
                'fpc' id_type,
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
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_${org_id}_${db_id}_sql_3_pre.error
   
        #### Import Data ####
        echo ''
        echo [INSERT ${vDate} data INTO ${project_name}.${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_${org_id}_${db_id}_src.txt' INTO TABLE ${project_name}.${table_name}_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_sql_3_after.error 


        export sql_4="
            INSERT INTO ${project_name}.${table_name}_${org_id}_mid
                select 
                    null serial, 
                    accu_id, 
                    channel, 
                    org_id, 
                    db_id, 
                    channel_id, 
                    a.id,
                    id_type, 
                    b.fpc browser_fpc,
                    member_id, 
                    a.first_at, 
                    registered_at, 
                    now() created_at,
                    now() updated_at
                from (
                    select *
                    from uuid.accu_mapping_${org_id}_src 
                    where db_id = ${db_id}
                        and id_type = 'fpc'
                    ) a
                    
                    left join uuid.cdp_fpc_mapping b
                    on a.id = b.origin_fpc 

                group by 
                    accu_id, 
                    channel, 
                    org_id, 
                    db_id, 
                    channel_id, 
                    a.id,
                    id_type, 
                    b.fpc,
                    member_id, 
                    a.first_at, 
                    registered_at   
            ;"            
        echo ''
        echo [INSERT INTO ${project_name}.${table_name}_${org_id}_mid]
        echo $sql_4
        mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_sql_4.error        

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_db_id.txt


    export sql_5a="
        INSERT INTO ${project_name}.${table_name}_${org_id}_mid_bfpc_f
            select accu_id, browser_fpc, row_number() over (partition by browser_fpc order by first_at) rid
            from ${project_name}.${table_name}_${org_id}_mid
            where browser_fpc is not null
                and browser_fpc <> ''
                and id_type = 'fpc'
        ;
        
        DELETE
        FROM ${project_name}.${table_name}_${org_id}_mid_bfpc_f
        WHERE rid >= 2
        ;
        
        UPDATE ${project_name}.${table_name}_${org_id}_mid a
            INNER JOIN ${project_name}.${table_name}_${org_id}_mid_bfpc_f b
            ON a.browser_fpc = b.browser_fpc      
        SET a.accu_id = b.accu_id
        WHERE a.id_type = 'fpc'
        ;"
    echo ''
    echo [整理 ${project_name}.${table_name}_${org_id}_mid：同個 browser_fpc，取時間最早的 accu_id]
    echo [UPDATE ${project_name}.${table_name}_${org_id}_mid]
    echo $sql_5a
    mysql --login-path=$dest_login_path -e "$sql_5a" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_mid.error        


    export sql_5b="          
        INSERT INTO ${project_name}.${table_name}_${org_id}_mid_member_f
            select browser_fpc, member_id, registered_at, row_number () over (partition by browser_fpc order by registered_at desc) rid
            from ${project_name}.${table_name}_${org_id}_mid
            where member_id is not null
                and member_id <> ''
                and id_type = 'fpc'
        ;
        
        DELETE
        FROM ${project_name}.${table_name}_${org_id}_mid_member_f
        WHERE rid >= 2
        ;

        UPDATE ${project_name}.${table_name}_${org_id}_mid a
            INNER JOIN ${project_name}.${table_name}_${org_id}_mid_member_f c
            ON a.browser_fpc = c.browser_fpc
        SET a.member_id = if(a.member_id is null or a.member_id = '', c.member_id, a.member_id), 
            a.registered_at = if(a.registered_at < c.registered_at, c.registered_at, a.registered_at)
        WHERE a.id_type = 'fpc'  
        ;"
    echo ''
    echo [整理 ${project_name}.${table_name}_${org_id}_mid：同個 browser_fpc，將 member_id 與 registered_at 為空者，以最晚時間的資料補入]
    echo [UPDATE ${project_name}.${table_name}_${org_id}_mid]
    echo $sql_5b
    mysql --login-path=$dest_login_path -e "$sql_5b" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_mid.error        


    export sql_6="
        ## 刪除重複綁定的 fpc 資料 ##
        ###############################################################
        # 根據小張的說明，同一 fpc/channel_id 有重複註冊的話，
        # 在註冊的那個瞬間，就會再送新的 fpc/channel_id 給那個 user，所以 member_id 會不同。
        # 在 CDP 系統中，identity_data 會直接以新資料取代舊資料
        # 但是 fpc_unique_data 與 audience_data 則是多筆紀錄都保留。
        # 下面這段 DELETE，實際上不會有符合的案例。
        ###############################################################

        USE ${project_name}
        DELETE a
        FROM ${project_name}.${table_name}_${org_id}_mid a
            INNER JOIN
            (
            select browser_fpc, id, id_type, channel, member_id, registered_at
            from (
                select 
                    browser_fpc, 
                    id, 
                    id_type, 
                    channel, 
                    member_id, 
                    registered_at, 
                    row_number () over (partition by browser_fpc order by registered_at desc) rid
                from ${project_name}.${table_name}_${org_id}_mid
                where member_id is not null
                    and member_id <> ''
                    and id_type = 'fpc'
                    and channel = 'web'
                ) bb
            where rid = 1
            ) b
            ON a.browser_fpc = b.browser_fpc
                and a.id = b.id
                and a.id_type = b.id_type
                and a.channel = b.channel
                and a.member_id <> b.member_id
        WHERE a.member_id is not null
            and a.member_id <> ''
        ;
        ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [刪除先前已有綁定 member_id 的 accu_id 資料]
    echo [UPDATE ${vDate} data INTO ${project_name}.${table_name}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}.error 


    export sql_7="
        INSERT INTO ${project_name}.${table_name}_${org_id}
            select 
                null serial, 
                b.accu_id, 
                b.channel, 
                b.org_id, 
                b.db_id, 
                b.channel_id, 
                b.id,
                b.id_type,
                b.browser_fpc, 
                b.member_id, 
                b.first_at, 
                b.registered_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_${org_id} a
                RIGHT JOIN
                (
                select *
                from ${project_name}.${table_name}_${org_id}_mid
                ) b
                ON a.id = b.id
                    and a.channel_id = b.channel_id
                    and a.db_id = b.db_id
                    and a.channel = b.channel
                    and a.id_type = b.id_type
            where a.id is null    
                and b.id_type = 'fpc'
        ;"
    echo ''
    echo [未存在 ${project_name}.${table_name}_${org_id} 表內的資料：寫入]
    echo [INSERT ${vDate} data INTO ${project_name}.${table_name}_${org_id}]
    echo $sql_7
    mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}.error        

    
    export sql_8="
        UPDATE ${project_name}.${table_name}_${org_id} a
            INNER JOIN
            (
            select *
            from ${project_name}.${table_name}_${org_id}_mid
            where member_id is not null
                and member_id <> ''
            ) b
            ON a.id = b.id
                and a.channel_id = b.channel_id
                and a.db_id = b.db_id
                and a.channel = b.channel
                and a.id_type = b.id_type
        SET a.member_id = if(a.member_id is null or a.member_id = '', b.member_id, a.member_id), 
            a.registered_at = if(a.registered_at < b.registered_at, b.registered_at, a.registered_at)
        WHERE a.id_type = 'fpc'
        ;"
    echo ''
    echo [已存在 ${project_name}.${table_name}_${org_id}_mid 表內的資料：更新]
    echo [UPDATE ${vDate} data on ${project_name}.${table_name}_${org_id}]
    echo $sql_8
    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}.error        


    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_src;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_bfpc_f]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_bfpc_f;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt
