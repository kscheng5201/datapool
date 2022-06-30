#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="landing3"
export src_login_path="tracker"
export table_name="web_keyword"

tables="
landing2_coway
landing2_ebo
landing2_gewei
landing2_philips
landing2_syntrend
"

for table in $tables
do 
    for i in $(seq 0 2)
    do
        export sql_1="
            select 
                null serial, 
                fpc, 
                datetime, 
                source, 
                pathname, 
                metaKeyword, 
                metaDesc, 
                now() created_at, 
                now() updated_at
            from ${src_login_path}.${table}
            where datetime >= '20210701' + interval ${i} month
                and datetime < '20210701' + interval ${i}+1 month
            ;"
        
        # Export Data
        echo ''
        echo [start export data at `date`]
        echo $sql_1
        mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.error
        
        # Import Data
        echo ''
        echo [start import data at `date`]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.txt' INTO TABLE ${src_login_path}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.error 
    
    done
done

echo ''
echo [end at `date`]
