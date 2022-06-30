#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export table_name="funnel" 
export src_login_path="cdp"

export sql_1="
    SET NAMES utf8mb4
    ;
    select *
    from cdp_data_team.${table_name}
    ;"
echo ''
echo [Get the latest funnel]
mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}.error
echo ''
echo [TRUNCATE TABLE codebook_cdp.${table_name}]
mysql --login-path=$dest_login_path -e "TRUNCATE TABLE codebook_cdp.${table_name};" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.${table_name}.error
echo ''
echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt' INTO TABLE codebook_cdp.${table_name}]
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt' INTO TABLE codebook_cdp.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.${table_name}.error


export sql_2="
    SET NAMES utf8mb4
    ;
    select *
    from cdp_data_team.${table_name}_config
    ;"    
echo ''
echo [Get the latest funnel_config]
mysql --login-path=${src_login_path} -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}_config.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}_config.error
echo ''
echo [TRUNCATE TABLE codebook_cdp.${table_name}_config]
mysql --login-path=$dest_login_path -e "TRUNCATE TABLE codebook_cdp.${table_name}_config;" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.${table_name}_config.error
echo ''
echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${table_name}_config.txt' INTO TABLE codebook_cdp.${table_name}_config]
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${table_name}_config.txt' INTO TABLE codebook_cdp.${table_name}_config IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.${table_name}_config.error


echo ''
echo 'end: ' `date`
