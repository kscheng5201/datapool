#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tag"
export src_login_path="cdp"
export table_name="fpc_unique"

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### loop by db_id ####
for i in $(seq 1 14)
do 
    export sql_query_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${i}
	    and domain_type = 'web'
	;"
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}.txt

while read p ;
do
    export sql_query_2="
        select 
            ${p} db_id, 
            id channel_id, 
            fpc, 
            life_cycle, 
            FROM_UNIXTIME(created_at) created_at, 
            updated_at
        from cdp_web_${p}.fpc_unique
	where (created_at >= UNIX_TIMESTAMP('${vDate}')
	    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
	    or (updated_at >= '${vDate}'
	    and updated_at < '${vDate}' + interval 1 day)
        ;"
# echo $sql_query_2

    export sql_query_3="
        CREATE TABLE IF NOT EXISTS ${project_name}.${src_login_path}_${table_name}_${i} (
            db_id int unsigned NOT NULL,  
            channel_id int unsigned NOT NULL,
            fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, 
            life_cycle tinyint NOT NULL DEFAULT '0' COMMENT '新用戶(1),積極(2),消極(3),沈睡(4),無回應(5)',  
            created_at timestamp NOT NULL,
            updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
            PRIMARY KEY (channel_id,fpc),  
            KEY idx_db_id (db_id),
            KEY idx_channel_id (channel_id), 
            KEY idx_fpc (fpc), 
            KEY created_at (created_at) 
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='fpc 對照表 with db_id and channel_id'
        ;"
# echo $sql_query_3

# Export Data
echo ''
echo 'start: ' `date`
echo 'exporting data from cdp_web_'${p} 
mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${p}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${p}.error

# Import Data
echo ''
echo 'start: ' `date`
echo 'importing data from cdp_web_'${p} 
mysql --login-path=$dest_login_path -e "$sql_query_3"
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${p}.txt' REPLACE INTO TABLE ${project_name}.${src_login_path}_${table_name}_${i} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${table_name}_${p}.error 

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}.txt
done 

echo ''
echo 'end: ' `date`
