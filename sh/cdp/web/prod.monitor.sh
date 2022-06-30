#!/usr/bin/bash
####################################################
# Project: 每日資料量監控
# Branch: 一切都是為了監控
# Author: Benson Cheng
# Created_at: 2022-02-10
# Updated_at: 2022-02-10
####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type="session"
export table_name="monitor" 
export src_login_path="cdp"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


export sql_0="    
    CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name} (
        serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
        tag_date date NOT NULL COMMENT '資料願算日',
        stat_date date NOT NULL COMMENT '資料統計區間',
        org_id tinyint(4) unsigned NOT NULL NOT NULL COMMENT '組織編號', 
        org_name varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '組織名稱',
        domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
        entry int NOT NULL COMMENT 'fpc_raw_data 資料筆數',
        created_at datetime NOT NULL COMMENT '寫入資料庫的時間戳記', 
        updated_at timestamp not null default current_timestamp on update current_timestamp COMMENT '更新資料庫的時間戳記',
        primary key (tag_date, stat_date, org_id, org_name, domain),
        key idx_created_at (created_at), 
        key idx_domain (domain), 
        key idx_tag_date (tag_date),
        key idx_org_id (org_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每日 fpc_raw_data 資料筆數'
    ;" 
echo ''
echo [create table if not exists ${project_name}.${type}_${table_name}_${org_id}]
echo $sql_0
mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_0.error


while read org_id;
do
    export sql_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
            and deleted_at is null
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    export sql_2="
        select max(nickname) name
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
        ;"
    echo ''
    echo [Get the org_name on web]
    mysql --login-path=${src_login_path} -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_name.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_name.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_name.txt


    while read org_name; 
    do 
        while read db_id; 
        do            
            export sql_3="
                SET NAMES utf8mb4
                ;
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    '${vDate}' stat_date,
                    ${org_id} org_id,
                    '${org_name}' org_name,
                    source domain,
                    count(*) entry,
                    now() created_at, 
                    now() updated_at
                from cdp_web_${db_id}.fpc_raw_data
                where created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)
                group by domain    
                ;"
            echo ''
            echo [exporting data to ${project_name}.${table_name}_${org_id}_${db_id}.txt]
            echo $sql_3
            mysql --login-path=${src_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.error
            echo ''
            echo [import data from ${project_name}.${table_name}_${org_id}_${db_id}.txt to ${project_name}.${type}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt' INTO TABLE ${project_name}.${type}_${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}.error 

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_name.txt
    
    
    export sql_4="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}
            select 
                null,
                tag_date, 
                stat_date, 
                org_id, 
                org_name, 
                'ALL' domain, 
                sum(entry) entry, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}
            where tag_date = '${vDate}' + interval 1 day
            group by 
                tag_date, 
                stat_date, 
                org_id, 
                org_name
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql_4.error 


    export sql_5="
        select concat('組織', org_id, '：', org_name, '。昨天(', '${vDate}', ')資料量：', ifnull(yesterday, 0), '；前天(', '${vDate}' - interval 1 day, ')資料量：', ifnull(last_2nd_day, 0), '（fpc_raw_data）')
        from (
            select 
                org_id, 
                org_name,
                max(if(tag_date = '${vDate}' + interval 1 day and domain = 'ALL', entry, null)) yesterday, 
                max(if(tag_date = '${vDate}' and domain = 'ALL', entry, null)) last_2nd_day
            from web.session_monitor
            where org_id = ${org_id}
            group by 
                org_id, 
                org_name
            ) a
        ;"
    echo ''
    echo [making the output messenge]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_output.txt
    sed -i '1d' $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_output.txt

    while read output; 
    do
        # 小房間
        /root/anaconda3/bin/python /root/LogReport.py 'info' ${output} '' "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
        # public room
        /root/anaconda3/bin/python /root/LogReport.py 'info' ${output} '' "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
    done < $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_output.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt
