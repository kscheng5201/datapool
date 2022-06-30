#!/usr/bin/bash
##############################################################
# Project: 給遠東集團查詢 browser_fpc 在跨組織的表現
# Branch: 
# Author: Benson Cheng
# Created_at: 2022-05-13
# Updated_at: 2022-05-13
# Note: 此寫法為固定一個 org 一個 web domain，若有變動則可能出錯
###############################################################

src_login_path="cdp"
dest_login_path="datapool_prod"
project_name="far_east"
export_dir="/root/datapool/export_file"
error_dir="/root/datapool/error_log"
table_name="fpc_unique"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


sql_0="
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name} (
        id int unsigned NOT NULL COMMENT '原始流水號',
        fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'domain 指紋碼', 
        browser_fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'browser 指紋碼', 
        org_id int unsigned NOT NULL COMMENT '組織 id',
        domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '網域',
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間', 
        PRIMARY KEY (id, fpc),
        KEY idx_browser_fpc (browser_fpc), 
        KEY idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='${table_name} 原表擴充'
    ;"
echo ''
echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}]
echo $sql_0
mysql --login-path=${src_login_path} -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}.error



for db_id in $(seq 21 26)
do
    sql_1="
        select 
            id, 
            fpc, 
            null browser_fpc, 
            (select org_id from cdp_organization.organization_domain where domain_type = 'web' and db_id = ${db_id}) org_id, 
            (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = ${db_id}) domain, 
            from_unixtime(created_at), 
            updated_at
        from cdp_web_${db_id}.${table_name}
        #where created_at >= unix_timestamp('${vDate}')
        where created_at < unix_timestamp('20220401')
        ;"
    echo ''
    echo [from cdp_web_${db_id}.${table_name}]
    echo $sql_1
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${db_id}_${table_name}.txt
    mysql --login-path=${dest_login_path} -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${db_id}_${table_name}.txt' INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES;"
done


for org_id in $(seq 10 13)
do 
    sql_2="
        UPDATE ${project_name}.${table_name} a
            INNER JOIN uuid.accu_mapping_${org_id} b
            ON a.fpc = b.id
        SET a.browser_fpc = b.browser_fpc
        WHERE a.org_id = ${org_id}
            and b.id_type = 'fpc'
            and (a.browser_fpc = ''
             or a.browser_fpc = 'NULL')
        ;"
    echo ''
    echo [UPDATE ${project_name}.${table_name}]
    echo $sql_2
    mysql --login-path=${dest_login_path} -e "$sql_2" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_2.error
done 
