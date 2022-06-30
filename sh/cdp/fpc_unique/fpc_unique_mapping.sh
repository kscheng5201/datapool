#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="fpc_unique"
export src_login_path="cdp"
export table_name="mapping"

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
        SET NAMES utf8mb4
        ;
        select 
            null uuid, 
            a.fpc_unique_id, 
            a.member_id, 
            a.identity,
            b.fpc, 
            'web' channel_type, 
            FROM_UNIXTIME(b.created_at) first_time
        from cdp_web_${i}.fpc_unique_data a,
            cdp_web_${i}.fpc_unique b
        where a.fpc_unique_id = b.id
            and b.created_at >= UNIX_TIMESTAMP(${vDate}))
#            and b.created_at < UNIX_TIMESTAMP(${vDate} + interval 1 day))
        ;"
# echo $sql_query_1

# Export Data
echo ''
echo 'start: ' `date`
echo 'exporting data from cdp_web_'${i} 
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${project_name}_${table_name}_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${project_name}_${table_name}_${i}.error

# Import Data
echo ''
echo 'start: ' `date`
echo 'importing data from cdp_web_'${i} 
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${project_name}_${table_name}_${i}.txt' INTO TABLE ${project_name}.${src_login_path}_${project_name}_${table_name}_${i} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${project_name}_${table_name}_${i}.error 
done

echo ''
echo 'end: ' `date`
