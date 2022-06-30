
#!/usr/bin/bash
####################################################
# Project: 愛酷 ID 整合
# Branch: crm 部分
# Author: Benson Cheng
# Created_at: 2022-04-08
# Updated_at: 2022-04-08
# Note: 只用 member_id 作 mapping
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
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_src;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_mid]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_mid;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_mid_member_f;"


    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = 'crm'
    	;"
    echo ''
    echo [Get the db_id on web]
    echo $sql_1
    mysql --login-path=${src_login_path}_master -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_crm_${org_id}_db_id.txt 2>> $error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_crm_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_crm_${org_id}_db_id.txt


    export sql_2="
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_crm_${org_id}_src (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 #mysql uuid() 機制所產生的 id',
            org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),crm(4),crm(5)',
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_crm_${org_id}_mid (
            serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number',
            accu_id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '由 #mysql uuid() 機制所產生的 id',
            channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web(1),line(2),messenger(3),crm(4),crm(5)',
            org_id int NOT NULL DEFAULT '0' COMMENT '組織/客戶 id',
            db_id tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '資料來自哪個DB', 
            channel_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'unique_id/channel_id',
            id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '各種 id',
            id_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'id 欄位內容的說明',
            browser_fpc varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '透過 tracker 比對後的指紋碼 fingerprint code',            
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            new_first_at datetime DEFAULT NULL COMMENT '此 fpc/channel_id 首次出現時間',            
            old_first_at datetime DEFAULT NULL COMMENT '此 fpc/channel_id 首次出現時間',
            new_registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            old_registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
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
        CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_crm_${org_id}_mid_member_f (
            accu_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '愛酷ID',
            member_id varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '會員ID',
            registered_at datetime DEFAULT NULL COMMENT '會員註冊時間(若無,則取綁定時間)',
            rid int NOT NULL COMMENT 'ranking id',
            KEY idx_rid (rid),
            KEY idx_member_id (member_id), 
            KEY idx_registered_at (registered_at),
            KEY idx_accu_id (accu_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='時間最早 member_id 暫存表'
        ;"        
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_crm_${org_id}_src]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_crm_${org_id}_src.error


    while read db_id; 
    do 
        export sql_3="
            select 
                null serial, 
                uuid() accu_id, 
                ${org_id} org_id,
                'crm' channel,
                ${db_id} org_id,
                b.id unique_id, 
                crm id,
                'crm' id_type,
                a.member_id, 
                from_unixtime(a.crm_unique_created_at) first_at,
                from_unixtime(if(c.registered_at >= 1 and c.registered_at < c.created_at, c.registered_at, c.created_at)) registered_at,
                from_unixtime(a.crm_unique_created_at) created_at, 
                from_unixtime(a.updated_at) updated_at
            from cdp_crm_${db_id}.crm_unique_data a
            
                inner join cdp_crm_${db_id}.crm_unique b
                on a.crm_unique_id = b.id
                
                left join 
                (
                select *
                from cdp_${org_id}.audience_data 
                where channel_type = 5
                    and db_id = ${db_id}
                
                # web(1),line(2),messenger(3),app(4),crm(5)
                ) c
                on b.id = c.channel_id

            where (a.crm_unique_created_at >= unix_timestamp('${vDate}')
                and a.crm_unique_created_at < unix_timestamp('${vDate}' + interval 1 day))
                 or (a.updated_at >= unix_timestamp('${vDate}')
                and a.updated_at < unix_timestamp('${vDate}' + interval 1 day)
                )
            ;"
            
        #### Export Data ####
        echo ''
        echo [Export ${vDate} Data FROM cdp_crm_${db_id}.fpc_unique_data and cdp_crm_${db_id}.crm_unique]
        echo $sql_3
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_crm_${org_id}_${db_id}_src.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_crm_${org_id}_${db_id}_src.error
   
        #### Import Data ####
        echo ''
        echo [INSERT ${vDate} data INTO ${project_name}.${table_name}_crm_${org_id}_src]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${table_name}_crm_${org_id}_${db_id}_src.txt' INTO TABLE ${project_name}.${table_name}_crm_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name${project_name}.${table_name}_crm_${org_id}_src.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_crm_${org_id}_db_id.txt


    export sql_u1="
        UPDATE ${project_name}.${table_name}_crm_${org_id}_src
        SET member_id = id
        WHERE member_id is null
            or member_id = ''
        ;"
    echo ''
    echo [UPDATE ${project_name}.${table_name}_crm_${org_id}_src]
    echo $sql_u1
    mysql --login-path=$dest_login_path -e "$sql_u1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_crm_${org_id}_src_sql_u1.error        



    export sql_4a="            
        INSERT IGNORE INTO ${project_name}.${table_name}_crm_${org_id}_mid
            select 
                null, 
                a.accu_id, 
                a.channel, 
                a.org_id, 
                a.db_id, 
                a.channel_id, 
                a.id, 
                a.id_type, 
                browser_fpc, 
                a.member_id, 
                a.first_at new_first_at, 
                b.first_at old_first_at,
                a.registered_at new_registered_at, 
                b.registered_at old_registered_at,
                now() created_at,
                now() updated_at
            from ${project_name}.${table_name}_crm_${org_id}_src a
                INNER JOIN ${project_name}.${table_name}_${org_id} b
                ON a.member_id = b.member_id
        ;
        ALTER TABLE ${project_name}.${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${table_name}_crm_${org_id}_mid]
    echo $sql_4a
    mysql --login-path=$dest_login_path -e "$sql_4a" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_crm_${org_id}_mid_4a.error        


    export sql_5a="            
        INSERT INTO ${project_name}.${table_name}_${org_id}
            select 
                null serial,
                accu_id, 
                channel, 
                org_id, 
                db_id, 
                channel_id, 
                id, 
                id_type, 
                browser_fpc, 
                member_id, 
                if(old_first_at is not null, 
                    if(new_first_at is not null, 
                        if(old_first_at < new_first_at, 
                            old_first_at, 
                                new_first_at
                            ), ifnull(old_first_at, new_first_at) 
                        ), ifnull(old_first_at, new_first_at)
                    ) first_at, 
                if(old_registered_at is not null, 
                    if(new_registered_at is not null, 
                        if(old_registered_at < new_registered_at, 
                            old_registered_at, 
                                new_registered_at
                            ), ifnull(old_registered_at, new_registered_at) 
                        ), ifnull(old_registered_at, new_registered_at)
                    ) registered_at, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_crm_${org_id}_mid
        ;
        ALTER TABLE ${project_name}.${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${table_name}_${org_id}]
    echo $sql_5a
    mysql --login-path=$dest_login_path -e "$sql_5a" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_${org_id}_4a.error        


    export sql_6="     
        INSERT IGNORE INTO ${project_name}.${table_name}_crm_${org_id}_mid_member_f
            select 
                accu_id, 
                member_id, 
                registered_at, 
                row_number () over (partition by member_id order by registered_at) rid
            from ${project_name}.${table_name}_${org_id}
            where member_id is not null
                and member_id <> ''
        ;
        
        DELETE
        FROM ${project_name}.${table_name}_crm_${org_id}_mid_member_f
        WHERE rid >= 2
        ;

        UPDATE ${project_name}.${table_name}_${org_id} a
            INNER JOIN ${project_name}.${table_name}_crm_${org_id}_mid_member_f c
            ON a.member_id = c.member_id
        SET a.accu_id = c.accu_id, 
            a.registered_at = 
                if(a.registered_at is not null, 
                    if(c.registered_at is not null, 
                        if(a.registered_at < c.registered_at, 
                            a.registered_at, 
                                c.registered_at
                            ), ifnull(a.registered_at, c.registered_at) 
                        ), ifnull(a.registered_at, c.registered_at)
                    ) 
        ;"
    echo ''
    echo [UPDATE ${project_name}.${table_name}_${org_id}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_crm_${org_id}_mid_sql_6.error 


    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_src;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_mid]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_mid;"
    echo ''
    echo [beforehand: DROP TABLE IF EXISTS ${project_name}.${table_name}_${org_id}_mid_member_f]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${table_name}_crm_${org_id}_mid_member_f;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt


echo ''
echo `date`
