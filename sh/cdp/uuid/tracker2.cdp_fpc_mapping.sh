#!/usr/bin/bash
####################################################
# Project: 愛酷 ID 建立
# Branch: browser fpc 基礎資料取得
# Author: Benson Cheng
# Created_at: 2022-01-05
# Updated_at: 2022-02-09
# Note: 
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="cdp"
export src_login_true="tracker"
export table_name="cdp_fpc_mapping"


#### Get Date ####
if [ -n "$1" ]; then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

echo ''
echo [Get Date ${vDate}]

#### Get the web domain ####
export sql_0="
    select group_concat(domain separator '|') source
    from cdp_organization.organization_domain
    where domain_type = 'web'
        and deleted_at is null
    ;"
echo ''
echo [Get the web domain of all org ]
mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt
echo ''
echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt


while read source;
do 
    export sql_2="
        select table_name
        from information_schema.tables
        where table_schema = 'tracker'
            and table_name REGEXP '^mapping_'
            and table_name NOT REGEXP 'translate'
            and table_name REGEXP '${source}'
        ;"
    echo ''
    echo [Get the current table_name of all org ]
    mysql --login-path=${src_login_true}2 -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_domain.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_domain.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_domain.txt
    echo ''
    echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_domain.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_domain.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_web_domain.txt


while read domain;
do
    export sql_1="
        select 
            fpc, 
            title origin_fpc, 
            metaKeyword domain, 
            min(datetime) first_time
        from tracker.\`${domain}\`
        where datetime >= '${vDate}'
            and datetime < '${vDate}' + interval 1 day
        group by 
            fpc, 
            title, 
            metaKeyword
        ;"
    echo ''
    echo [Get the new mapping fpc at ${domain} on ${vDate}]
    echo $sql_1
    mysql --login-path=${src_login_true}2 -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${domain}_fpc.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${domain}_fpc.error
    tail $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${domain}_fpc.txt

    echo ''
    echo [LOAD DATA LOCAL INFILE $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${domain}_fpc.txt IGNORE INTO TABLE uuid.${table_name} ]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${domain}_fpc.txt' IGNORE INTO TABLE uuid.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/uuid.${table_name}_${domain}.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_domain.txt

echo ''
echo 'end: ' `date`
