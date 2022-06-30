#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export src_login_path="nix"
export project_name="tag"
export table_name="src"

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

# IM: Instant Messenger
IM="fbmessenger line"


for cur_im in $IM;
do
    export sql_query_1="
        select id
        from accunix_v2.${cur_im}bots
        ;"
    mysql --login-path=$src_login_path -e "$sql_query_1" > ${export_dir}/${src_login_path}/${project_name}/${project_name}.${src_login_path}_${cur_im}bots_id.txt 2>> $error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${cur_im}bots_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${cur_im}bots_id.txt
        
    while read p ;
    do
        export sql_query_2="
            SET NAMES utf8mb4
	    ;
	    select 
                date(a.created_at) stat_date, 
                user_token, 
                b.name tag, 
                count(*) tag_freq, 
                max(a.created_at) last_at
            from accunix_v2_log.${cur_im}bot_${p}_tag_histories a
                left join accunix_v2_log.${cur_im}bot_${p}_tags b
                    on a.tag_id = b.id
                left join accunix_v2_log.${cur_im}bot_${p}_users c
                    on a.user_id = c.id
            where a.created_at >= date('${vDate}')
                and a.created_at < date('${vDate}' + interval 1 day)
                and action < 3
                and b.deleted_at is null
            group by 
                user_token, 
                b.name
            ;"
        # echo $sql_query_2
    
        export sql_query_3="
            create table if not exists ${project_name}.${src_login_path}_${cur_im}bot_${p}_${table_name} (
                stat_date date COMMENT '報表資料計算日', 
                user_token varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '用戶Token',
                tag varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱', 
                tag_freq int NOT NULL COMMENT '標籤累積次數', 
                last_at datetime COMMENT '最後一次貼標時間', 
                primary key (user_token, tag), 
                key idx_stat_date (stat_date), 
                key idx_tag (tag), 
                key idx_last_at (last_at)
            ) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='近 90 天的 user tag 每日紀錄情形'                                
            ;"
        # echo $sql_query_2   
    
        # Export Data
        echo ''
        echo 'start: ' `date` 
	echo 'export data to '$export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${cur_im}bot_${table_name}_90d_${p}.txt
        mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/${project_name}.${src_login_path}_${cur_im}bot_${table_name}_90d_${p}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${cur_im}bot_${table_name}_90d_${p}.error
    
        echo ''
        echo 'start: ' `date` 
        # Import Data
	echo 'create table if not exists ' ${project_name}.${src_login_path}_${cur_im}bot_${p}_${table_name}
        mysql --login-path=$dest_login_path -e "$sql_query_3"

	#echo 'truncate table '${project_name}.${src_login_path}_${cur_im}bot_${p}_${table_name}
        #mysql --login-path=$dest_login_path -e "truncate table ${project_name}.${src_login_path}_${cur_im}bot_${p}_${table_name};"

	echo 'import data to '${project_name}.${src_login_path}_${cur_im}bot_${table_name}_90d_${p}
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${cur_im}bot_${table_name}_90d_${p}.txt' INTO TABLE ${project_name}.${src_login_path}_${cur_im}bot_${p}_${table_name} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${project_name}.${src_login_path}_${cur_im}bot_${p}_${table_name}.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${cur_im}bots_id.txt
done

echo 'end: ' `date`


