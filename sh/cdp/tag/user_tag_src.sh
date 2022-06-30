#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tag"
export src_login_path="cdp"
export table_name="src"


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
            date(a.updated_at) stat_date,
            a.db_id, 
            channel_id, 
	    identity, 
            domain,
            max(a.updated_at) datetime, 
            case channel_type
                when 1 then 'web'
                when 2 then 'line'
                when 3 then 'messenger'
                when 4 then 'app'
                when 5 then 'crm'
            else null
            end channel_type, 
            b.name tag, 
            count(*) tagging_freq
        from cdp_${i}.user_tag a
            inner join cdp_${i}.tag b on a.tag_id = b.id
            inner join 
            (
            select db_id, domain
            from cdp_organization.organization_domain 
            where org_id = ${i}
		and domain_type = 'web'
            ) d
	     on a.db_id = d.db_id
        where a.created_at >= date_format('${vDate}', '%Y%m%d')
            and a.created_at < date_format('${vDate}' + interval 1 day, '%Y%m%d')
        group by 
            stat_date, 
            a.db_id, 
            channel_id,
	    identity,  
            domain,
            channel_type, 
            tag
        ;"
# echo $sql_query_1

    export sql_query_2="
        CREATE TABLE IF NOT EXISTS ${project_name}.${src_login_path}_${i}_${table_name} (
           stat_date date NOT NULL COMMENT 'statistics date',
           db_id tinyint unsigned NOT NULL DEFAULT '0' COMMENT '資料來源的 db_id',
           channel_id int unsigned NOT NULL DEFAULT '0' COMMENT '各 domain 中的 unique id',     
	   identity int(11) unsigned NOT NULL DEFAULT '0' COMMENT '識別是否同一人',
           domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'domain url',
           last_at datetime NOT NULL COMMENT 'the lastest timestamp at the day',
           channel_type varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web,line,messenger,app,crm', 
           tag varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',  
           tag_freq int unsigned NOT NULL DEFAULT '0' COMMENT 'frequency of tag',  
           PRIMARY KEY (stat_date, db_id, channel_id),     
           KEY idx_tag (tag),  
           KEY idx_last_at (last_at),  
           KEY idx_stat_date (stat_date) 
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDP 用戶標籤主表'   
        ;"


# Export Data
echo ''
echo 'start: ' `date`
echo 'exporting data from cdp_'${i} 
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${i}_${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${i}_${table_name}.error

# Import Data
echo ''
echo 'start: ' `date`
echo 'importing data from cdp_'${i} 
mysql --login-path=$dest_login_path -e "$sql_query_2"
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${i}_${table_name}.txt' INTO TABLE ${project_name}.${src_login_path}_${i}_${table_name} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${i}_${table_name}.error 
done

echo ''
echo 'end: ' `date`
