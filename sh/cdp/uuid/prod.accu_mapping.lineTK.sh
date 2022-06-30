#!/usr/bin/bash
####################################################
# Project: 愛酷 ID 建立
# Branch: Line Token 與 browser_fpc 基礎資料取得
# Author: Benson Cheng
# Created_at: 2022-04-18
# Updated_at: 2022-04-19
# Note: 
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="cdp"
export src_login_true="tracker"
export table_name="lineTK_mapping"


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


export sql_0a="
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name} (
        serial int NOT NULL AUTO_INCREMENT unique,
        lineTK varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Line token',    
        domain varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,     
        domain_fpc varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, 
        browser_fpc varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,      
        first_at datetime NOT NULL,    
        created_at datetime NOT NULL,   
        updated_at datetime NOT NULL,   
        PRIMARY KEY (lineTK, domain, domain_fpc) USING BTREE,
        KEY idx_domain_fpc (domain_fpc) USING BTREE,     
        KEY idx_domain (domain) USING BTREE,   
        KEY idx_browser_fpc (browser_fpc) USING BTREE,    
        KEY idx_first_at (first_at) USING BTREE,           
        KEY idx_lineTK (lineTK) USING BTREE,        
        KEY idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Line token 與 fpc 對照表'     
    ;"
echo ''
echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}]
echo $sql_0a
mysql --login-path=${dest_login_path} -e "$sql_0a" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_0a.error


#### Get the web domain ####
export sql_0b="
    select group_concat(domain separator '|') source
    from cdp_organization.organization_domain
    where domain_type = 'web'
        and deleted_at is null
    ;"
echo ''
echo [Get the web domain of all org ]
echo $sql_0b
mysql --login-path=$src_login_path -e "$sql_0b" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt
echo ''
echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt


while read source;
do 
    export sql_2="
        select concat_ws('=', replace(table_name, 'mapping_', ''), table_name)
        from information_schema.tables
        where table_schema = '${src_login_true}'
            and table_name REGEXP '^mapping_'
            and table_name NOT REGEXP 'translate'
            and table_name REGEXP '${source}'
        ;"
    echo ''
    echo [Get the current table_name of all org ]
    echo $sql_2
    mysql --login-path=${src_login_true}2 -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_LineTK_domain.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_LineTK_domain.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_LineTK_domain.txt
    echo ''
    echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_LineTK_domain.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_LineTK_domain.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt


while read LineTK_domain; 
do 
    export sql_3="
        select 
            a.lineTK, 
            a.source, 
            a.fpc domain_fpc, 
            b.fpc browser_fpc, 
            min(a.datetime) datetime, 
            now() created_at, 
            now() updated_at
        from ${src_login_true}.\`$(echo ${LineTK_domain} | cut -d = -f 1)\` partition (p${vDateName}) a
            LEFT JOIN ${src_login_true}.\`$(echo ${LineTK_domain} | cut -d = -f 2)\` b
            ON a.fpc = b.title
        where a.lineTK <> '?' 
        group by a.lineTK, a.source, a.fpc
        ;"
    echo ''
    echo #### Export Data ####
    echo [exporting data to ${project_name}.${table_name}_$(echo $(echo ${LineTK_domain} | cut -d = -f 1) | cut -d / -f 1)_p${vDateName}.txt]
    echo $sql_3
    mysql --login-path=${src_login_true}2 -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_$(echo $(echo ${LineTK_domain} | cut -d = -f 1) | cut -d / -f 1)_p${vDateName}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_$(echo $(echo ${LineTK_domain} | cut -d = -f 1) | cut -d / -f 1)_p${vDateName}.error
    
    echo ''
    echo #### Import Data ####
    echo [import data from ${project_name}.${table_name}_$(echo $(echo ${LineTK_domain} | cut -d = -f 1) | cut -d / -f 1)_p${vDateName}.txt to ${project_name}.${table_name}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_$(echo $(echo ${LineTK_domain} | cut -d = -f 1) | cut -d / -f 1)_p${vDateName}.txt' IGNORE INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES (lineTK, domain, domain_fpc, browser_fpc, first_at, created_at, updated_at);" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_$(echo $(echo ${LineTK_domain} | cut -d = -f 1) | cut -d / -f 1)_p${vDateName}.error

    echo ''
    echo [ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1]
    mysql --login-path=$dest_login_path -e "ALTER TABLE ${project_name}.${table_name} AUTO_INCREMENT = 1;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_auto_increment.error


done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_LineTK_domain.txt


while read org_id;
do
    export sql_4="
        ## 先讓 lineTK 的 browser_fpc 與現有 browser_fpc 一致
        UPDATE ${project_name}.${table_name} a
            INNER JOIN ${project_name}.accu_mapping_${org_id} b
            ON b.id = a.domain_fpc
        SET a.browser_fpc = b.browser_fpc
        WHERE b.id_type = 'fpc'
            and b.browser_fpc <> 'NULL'
            and b.browser_fpc is not null
            and b.browser_fpc <> ''
            and a.created_at >= '${vDate}' + interval 1 day
        ;"
    echo ''
    echo [先讓 lineTK 的 browser_fpc 與現有 browser_fpc 一致]
    echo [UPDATE uuid.lineTK_mapping]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_sql_4.error


    export sql_5="
        INSERT IGNORE INTO ${project_name}.accu_mapping_${org_id}
            select 
                null serial, 
                uuid() accu_id, 
                'line' channel, 
                ${org_id} org_id, 
                0 db_id, 
                0 channel_id, 
                LineTK id, 
                'lineTK' id_type, 
                a.browser_fpc, 
                null member_id, 
                a.first_at, 
                null registered_at, 
                now() created_at, 
                now() updated_at
            from (
                select aa.*
                from ${project_name}.${table_name} aa, 
                    codebook_cdp.organization_domain bb
                where aa.domain = bb.domain
                    and domain_type = 'web'
                    and org_id = ${org_id}
                ) a
                
                LEFT JOIN ${project_name}.accu_mapping_${org_id} b
                ON a.domain_fpc = b.id
            where id_type = 'fpc'
                and a.created_at >= '${vDate}' + interval 1 day
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.accu_mapping_${org_id}]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.accu_mapping_${org_id}_sql_5.error


    export sql_6="
        UPDATE ${project_name}.accu_mapping_${org_id} a
            INNER JOIN ${project_name}.accu_mapping_${org_id} b
            ON a.browser_fpc = b.browser_fpc
        SET a.accu_id = b.accu_id, 
            a.member_id = b.member_id, 
            a.registered_at = b.registered_at
        WHERE a.id_type = 'lineTK'
            and b.id_type <> 'lineTK'
        ;"
    echo ''
    echo [UPDATE ${project_name}.accu_mapping_${org_id}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.accu_mapping_${org_id}_sql_6.error
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_accu_mapping_org_id.txt
