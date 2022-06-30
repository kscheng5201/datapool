#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tag"
export src_login_path="cdp"
export origin_table_name="src"
export dest_table_db="summary"


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

for i in $(seq 1 14)
do
    export sql_query_1="
        delete from 
        ${project_name}.${src_login_path}_${i}_${origin_table_name}
        where stat_date < date('${vDate}' - interval 89 day)
        ; 

	drop table if exists ${project_name}.temp_${src_login_path}_${i};     
        create table ${project_name}.temp_${src_login_path}_${i} as
            select 
                db_id, 
                channel_id, 
                identity, 
                domain, 
                max(last_at) last_at, 
                channel_type, 
                tag, 
                sum(tag_freq) tag_freq
            from ${project_name}.${src_login_path}_${i}_${origin_table_name}
            group by 
                db_id, 
                channel_id, 
                identity, 
                domain,  
                channel_type, 
                tag
        ; 

        drop table if exists ${dest_table_db}_${src_login_path}.${project_name}_${i}; 
        create table if not exists ${dest_table_db}_${src_login_path}.${project_name}_${i} as
        select 
            date('${vDate}') stat_date, 
            date('${vDate}' - interval 89 day) start_date, 
            date('${vDate}') end_date,
            db_id, 
            channel_id, 
            identity,
            domain, 
            channel_type, 
            tag, 
            tag_freq, 
            last_at, 
            floor((1 - rid / sum_over) * 100) ranking
        from (
            select
                *, 
                sum(1) over (partition by domain, tag) sum_over, 
                row_number () over (partition by domain, tag order by tag_freq desc, last_at desc) rid
            from ${project_name}.temp_${src_login_path}_${i}
            ) a
        ; 
    
    
        create index idx_stat_date on ${dest_table_db}_${src_login_path}.${project_name}_${i} (stat_date); 
        create index idx_domain on ${dest_table_db}_${src_login_path}.${project_name}_${i} (domain); 
        create index idx_channel_type on ${dest_table_db}_${src_login_path}.${project_name}_${i} (channel_type); 
        create index idx_tag on ${dest_table_db}_${src_login_path}.${project_name}_${i} (tag);
        create index idx_channel_id on ${dest_table_db}_${src_login_path}.${project_name}_${i} (channel_id); 
        create index idx_complex on ${dest_table_db}_${src_login_path}.${project_name}_${i} (tag, channel_type, domain);  
    
        alter table ${dest_table_db}_${src_login_path}.${project_name}_${i}
            modify column stat_date date not null comment '資料統計日', 
            modify column start_date date not null comment '90 天起始日', 
            modify column end_date date not null comment '90 天最末日',  
            modify column db_id tinyint(3) unsigned NOT NULL DEFAULT '0',
            modify column channel_id int(11) unsigned NOT NULL DEFAULT '0',
            modify column identity int(11) unsigned NOT NULL DEFAULT '0' COMMENT '識別是否同一人', 
            modify column domain varchar(32) NOT NULL DEFAULT '0',
            modify column channel_type varchar(16) NOT NULL DEFAULT '0',
            modify column tag varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
            modify column tag_freq int(11) unsigned NOT NULL DEFAULT '0' COMMENT '貼標次數',
            modify column last_at datetime not null comment '最近貼標時間',
            modify column ranking int not null comment '標籤濃度'
        ; 
        alter table ${dest_table_db}_${src_login_path}.${project_name}_${i}
            COMMENT = '編號 ${i} 的客戶，近 90 天的貼標 summary'
        ; 
	drop table if exists ${project_name}.temp_${src_login_path}_${i}
	; 
        "
        
# echo $sql_query_1

# Import Data
echo ''
echo 'start: ' `date`
echo 'importing data from '${project_name}.${src_login_path}_${i}_${origin_table_name}
mysql --login-path=$dest_login_path -e "$sql_query_1"
done

echo ''
echo 'end: ' `date`
