#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="tracker"
export table_name="cdp_fpc_mapping"
export fake_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

while read org_id;
do
    export sql_0="
        select group_concat(quote(substring_index(domain, '/', 1))) domain_list
        from ${fake_login_path}_organization.organization_domain
        where domain_type = 'web'
            and org_id = ${org_id}
        ;"
    echo ''
    echo [Get the domain_list of organization ${org_id}]
    echo $sql_0
    mysql --login-path=$fake_login_path -e "$sql_0" > $export_dir/$fake_login_path/$project_name/$project_name.${table_name}_${org_id}_domain_list.txt
    sed -i '1d' $export_dir/$fake_login_path/$project_name/$project_name.${table_name}_${org_id}_domain_list.txt
    

    while read domain_list; 
    do 
        export sql_1="
            select 
                fpc, 
                title origin_fpc, 
                metaKeyword domain, 
                min(datetime) first_time
            from ${src_login_path}.landing2_mapping
            where datetime >= date('${vDate}' + interval 0 day)
                and datetime < date('${vDate}' + interval 0+1 day)  
                and metakeyword in (${domain_list})
            group by 
                fpc, 
                title, 
                metaKeyword
            ;"
        echo ''
        echo [Export ${vData} Data from ${src_login_path}.landing2_mapping at `date`]
        echo $sql_1
        mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$fake_login_path/$project_name/$project_name.${table_name}_${org_id}.txt 2>>$error_dir/$fake_login_path/$project_name/$project_name.${table_name}_${org_id}.error
        
        echo ''
        echo [Import ${vData} Data IGNORE INTO TABLE ${project_name}.${table_name}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$fake_login_path/$project_name/$project_name.${table_name}_${org_id}.txt' IGNORE INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$fake_login_path/$project_name/$project_name.${table_name}.error 
    
    done < $export_dir/$fake_login_path/$project_name/$project_name.${table_name}_${org_id}_domain_list.txt
done < /root/datapool/export_file/cdp/uuid/uuid.cdp_accu_mapping_org_id.txt


export sql_2="
    CREATE TABLE IF NOT EXISTS ${project_name}.${src_login_path}_mapping (
        serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
        tracker_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來自 tracker mapping 後所得的 id',
        fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '原始 fpc',
        first_at datetime DEFAULT NULL COMMENT '首次 mapping 時間', 
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',    
        PRIMARY KEY (fpc), 
        key idx_tracker_id (tracker_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='tracker_id 與 fpc mapping 表'
    ;"
echo ''
echo [CREATE TABLE IF NOT EXISTS ${project_name}.${src_login_path}_mapping]
echo $sql_2
mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_mapping.error 

export sql_3="
    INSERT IGNORE INTO ${project_name}.${src_login_path}_mapping
        select 
            null serial, 
            fpc tracker_id, 
            origin_fpc fpc, 
            min(first_at) first_at, 
            now() created_at, 
            now() updated_at
        from ${project_name}.${table_name}
        where first_at >= '${vDate}'
            and first_at < '${vDate}' + interval 1 day
        group by 
            fpc, 
            origin_fpc
    ;
    ALTER TABLE ${project_name}.${src_login_path}_mapping AUTO_INCREMENT = 1
    ;"
echo ''
echo [INSERT INTO ${project_name}.${src_login_path}_mapping]
echo $sql_3
mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$fake_login_path/$project_name/${project_name}.${src_login_path}_mapping.error 



export sql_4="
    CREATE TABLE IF NOT EXISTS ${project_name}.${src_login_path}_mapping_new (
        serial int NOT NULL AUTO_INCREMENT COMMENT 'auto_increment Serial Number' unique,
        tracker_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來自 tracker mapping 後所得的 id',
        fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '原始 fpc',
        first_at datetime DEFAULT NULL COMMENT '首次 mapping 時間', 
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',    
        PRIMARY KEY (fpc), 
        key idx_tracker_id (tracker_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='tracker_id 與 fpc mapping 表'
    ;
    
    INSERT INTO uuid.tracker_mapping_new
        select null, tracker_id, fpc, first_at, created_at, now()
        from uuid.tracker_mapping
        where fpc in (
            select fpc
            from (
                select fpc, count(*)
                from uuid.tracker_mapping
                group by fpc
                having count(*) = 1
                ) a
            ) 
        order by first_at
    ;
    ALTER TABLE uuid.tracker_mapping_new AUTO_INCREMENT = 1
    ;
    
    INSERT INTO uuid.tracker_mapping_new
        select null, tracker_id, fpc, first_at, created_at, now()
        from (
            select tracker_id, fpc, first_at, created_at, row_number() over (partition by fpc order by first_at, serial) rid
            from uuid.tracker_mapping
            where fpc in (
                select fpc
                from (
                    select fpc, count(*)
                    from uuid.tracker_mapping
                    group by fpc
                    having count(*) >= 2
                    ) aa
                ) 
        ) a
        where rid = 1
    ;
    ALTER TABLE uuid.tracker_mapping_new AUTO_INCREMENT = 1
    ;
    
    DROP TABLE uuid.tracker_mapping
    ;
    RENAME TABLE uuid.tracker_mapping_new to uuid.tracker_mapping
    ;"
    
echo ''
echo [去重複用的預備語法]
echo $sql_4
#mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$fake_login_path/$project_name/${project_name}.${src_login_path}_mapping.error 

echo ''
echo [end at `date`]
