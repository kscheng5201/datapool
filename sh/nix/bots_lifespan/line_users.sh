#!/usr/bin/bash

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="bots_lifespan"
export src_login_path='nix'


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi
echo ''
echo ${vDate}


# IM: Instant Messenger
IM="line"

src_login_path='nix'
for cur_im in $IM;
do
    export sql_query_0="
        select id
        from accunix_v2.${cur_im}bots
        where created_at < date(now())
        order by id
       ;"
    mysql --login-path=$src_login_path -e "$sql_query_0" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.txt

    while read p ;
    do
        export sql_query_1="
            CREATE TABLE IF NOT EXISTS ${project_name}.${cur_im}bot_${p}_users (  
               id int(10) unsigned NOT NULL AUTO_INCREMENT, 
               LINEBot_id int(10) unsigned NOT NULL COMMENT 'Line機器人ID',   
               audience_id int(10) unsigned DEFAULT NULL COMMENT '受眾ID',    
               richmenu_id int(10) unsigned DEFAULT NULL COMMENT '主選單ID',  
               script_id int(11) DEFAULT NULL COMMENT '目前腳本 ID', 
               node_id int(11) DEFAULT NULL COMMENT '目前節點 ID',   
               data varchar(5000) DEFAULT NULL COMMENT '好友資料',    
               tmp varchar(5000) DEFAULT NULL COMMENT '好友暫時資料', 
               name varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '用戶名稱',  
               user_token varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '用戶Token', 
               reply_token varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '回覆Token', 
               status text COLLATE utf8mb4_unicode_ci COMMENT 'Line狀態訊息',  
               picture text COLLATE utf8mb4_unicode_ci COMMENT '頭貼',   
               is_follow tinyint(1) DEFAULT '1' COMMENT '追蹤狀態',  
               chatroom_status varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT 'finish' COMMENT '聊天狀態',
               last_active_at timestamp NULL DEFAULT NULL COMMENT '上次活躍時間',   
               created_at timestamp NULL DEFAULT NULL, 
               updated_at timestamp NULL DEFAULT NULL, 
               deleted_at timestamp NULL DEFAULT NULL, 
               lifecycle int(11) NOT NULL DEFAULT '1', 
               freq_hour int(11) NOT NULL DEFAULT '-1',
               last_messaged_at timestamp NULL DEFAULT NULL,
               is_block tinyint(1) NOT NULL DEFAULT '0',    
               lifecycle_v2 int(10) unsigned DEFAULT NULL,  
               PRIMARY KEY (id) USING BTREE, 
               KEY ${cur_im}bot_${p}_users_linebot_id_index (LINEBot_id),
               KEY ${cur_im}bot_${p}_users_audience_id_index (audience_id),   
               KEY ${cur_im}bot_${p}_users_richmenu_id_index (richmenu_id),   
               KEY ${cur_im}bot_${p}_users_script_id_index (script_id),  
               KEY ${cur_im}bot_${p}_users_node_id_index (node_id), 
               KEY ${cur_im}bot_${p}_users_name_index (name),    
               KEY ${cur_im}bot_${p}_users_user_token_index (user_token),
               KEY ${cur_im}bot_${p}_users_lifecycle_index (lifecycle)   
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci  
            ;"       
        echo ''
        echo 'start: ' `date` 
        echo [ CREATE TABLE IF NOT EXISTS ${project_name}.${cur_im}_${p}_users ]
        mysql --login-path=$dest_login_path -e "$sql_query_1"

        export sql_query_2="
            SELECT *
            FROM accunix_v2_log.${cur_im}bot_${p}_users 
            ;"
        echo ''
        echo $sql_query_2

        # Export Data
        echo ''
        echo 'start: ' `date` 
        echo ${project_name}.${cur_im}bot_${p}_users
        mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_${p}_users.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_${p}_users.error
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_${p}_users.txt' INTO TABLE ${project_name}.${cur_im}bot_${p}_users IGNORE 1 LINES;" 2>>$error_dir/$project_name.${cur_im}_${p}_users.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.txt
done

echo 'end: ' `date`
