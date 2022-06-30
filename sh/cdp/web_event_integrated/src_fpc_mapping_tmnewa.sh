#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web_event_integrated"
export stakeholder="tmnewa" # 新安東京海上產險
export src_login_path_1='tracker'
export src_login_path_2='cdp'

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y%m%d"`
fi

export sql_query_1="
        select 
            fpc, 
            title origin_fpc, 
            metaKeyword domain, 
            max(datetime) last_time
        from tracker.landing2_mapping
        where metaKeyword in (
            'b2c.tmnewa.com.tw',
            'www.tmnewa.com.tw',
            'yahoo.ebo.tmnewa.com.tw',
            'www.rakuten.com.tw/insurance'
            )
            and datetime >= '${vDate}' 
            and datetime < '${vDate}' + interval 1 day
        group by 
            fpc, 
            title, 
            metaKeyword
        ;"
# echo $sql_query_1

export sql_query_2="
        select 
            fpc, 
            title origin_fpc, 
            metaKeyword domain, 
            min(datetime) first_time
        from tracker.landing2_mapping
        where metaKeyword in (
            'b2c.tmnewa.com.tw',
            'www.tmnewa.com.tw',
            'yahoo.ebo.tmnewa.com.tw',
            'www.rakuten.com.tw/insurance'
            )
            and datetime >= '${vDate}' 
            and datetime < '${vDate}' + interval 1 day
        group by 
            fpc, 
            title, 
            metaKeyword
        ;"
# echo $sql_query_2


# Export Data
echo ''
echo 'start: ' `date` 
echo 'export data for src_fpc_mapping_last_'${stakeholder}
mysql --login-path=$src_login_path_1 -e "$sql_query_1" > $export_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_last_${stakeholder}.txt 2>>$error_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_last_${stakeholder}.error

echo ''
echo 'start: ' `date` 
echo 'export data for src_fpc_mapping_first_'${stakeholder}
mysql --login-path=$src_login_path_1 -e "$sql_query_2" > $export_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_first_${stakeholder}.txt 2>>$error_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_first_${stakeholder}.error


# Import Data
echo ''
echo 'on partition '${i}
echo 'start: ' `date` 
echo 'import data for src_fpc_mapping_last_'${stakeholder}
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_last_${stakeholder}.txt' REPLACE INTO TABLE ${project_name}.src_fpc_mapping_last_${stakeholder} IGNORE 1 LINES;" 2>>$error_dir/$project_name.src_fpc_mapping_last_${stakeholder}.error 

echo ''
echo 'on partition '${i}
echo 'start: ' `date` 
echo 'export data for src_fpc_mapping_first_'${stakeholder}
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path_2/$project_name/$project_name.src_fpc_mapping_first_${stakeholder}.txt' IGNORE INTO TABLE ${project_name}.src_fpc_mapping_first_${stakeholder} IGNORE 1 LINES;" 2>>$error_dir/$project_name.src_fpc_mapping_first_${stakeholder}.error 

