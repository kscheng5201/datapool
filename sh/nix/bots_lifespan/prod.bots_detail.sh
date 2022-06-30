#!/usr/bin/bash
##################################################
# Project: 更新機器人的基本資料
# Branch: bots_lifespan
# Author: Benson Cheng
# Created_at: 2021-12-23
# Updated_at: 2021-12-23
# Note: 若正式版本有問題，則以此版本作為修改基礎
##################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="bots_lifespan"
export project="nes"
export src_login_path="nix"


# IM: Instant Messenger
IM="fbmessenger line"
echo ''
echo [Create IM: Instant Messenger]
echo [IM = $IM]

for cur_im in $IM;
do
    case $cur_im in
    
        fbmessenger)	
        export sql_query_1="
            SET NAMES utf8mb4
            ;
            select 
                id, 
                name, 
                description, 
                organization_id, 
                product_type_id, 
                app_id, 
                app_secret, 
                access_token, 
                page_id, 
                pending_schedule_count, 
                GUID, 
                users_count, 
                auto_redirect_params, 
                chatroom_welcome_id, 
                chatroom_welcome_messages, 
                chatroom_finish_id, 
                chatroom_finish_messages, 
                chatroom_auto_finish, 
                data, 
                default_persistent_menu_id, 
                expires_at, 
                created_at, 
                updated_at, 
                deleted_at, 
                picture
            from accunix_v2.fbmessengerbots
            ;";;
    
        line)
        export sql_query_1="
            SET NAMES utf8mb4
            ;
            select 
                id, 
                name, 
                description, 
                organization_id, 
                product_type_id, 
                channel_id, 
                channel_secret, 
                channel_access_token, 
                product_token, 
                basic_id, 
                liff_id, 
                default_richmenu_id, 
                users_count, 
                GUID, 
                auto_redirect_params, 
                data, 
                chatroom_welcome_id, 
                chatroom_welcome_messages, 
                chatroom_finish_id, 
                chatroom_finish_messages, 
                chatroom_auto_finish, 
                created_at, 
                updated_at, 
                deleted_at, 
                is_remind, 
                picture, 
                liff_path, 
                liff_url, 
                user_id
            from accunix_v2.linebots
           ;";;
    esac
    
    echo ''
    echo [Export data from accunix_v2.${cur_im}bots]
    echo $sql_query_1
    mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.error
    cat $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.txt
    tail $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.txt -n 1

    echo ''
    echo [TRUNCATE TABLE ${src_login_path}_${project}.${cur_im}bots]
    echo [LOAD DATA LOCAL INFILE $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.txt INTO TABLE ${src_login_path}_${project}.${cur_im}bots IGNORE 1 LINES]
    mysql --login-path=$dest_login_path -e "TRUNCATE TABLE ${src_login_path}_${project}.${cur_im}bots;"
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.txt' INTO TABLE ${src_login_path}_${project}.${cur_im}bots IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_prod.error

done


for cur_im in $IM;
do
    case $cur_im in
    
        fbmessenger)	
        export sql_query_2="
            SET NAMES utf8mb4
            ;
            INSERT INTO ${src_login_path}_${project}.${cur_im}bots_lifecycle_days
                select 
                    b.id, 
                    organization_id, 
                    0 active_days, 
                    0 passive_days, 
                    1 new_days, 
                    date(now()) last_update, 
                    0 all_months
                from ${src_login_path}_${project}.${cur_im}bots_lifecycle_days a
                    RIGHT JOIN ${src_login_path}_${project}.${cur_im}bots b
                    ON a.id = b.id
                WHERE a.id is null
                ;";;
    
        line)
        export sql_query_2="
            SET NAMES utf8mb4
            ;
            INSERT INTO ${src_login_path}_${project}.${cur_im}bots_lifecycle_days
                select 
                    b.id, 
                    organization_id, 
                    0 active_days, 
                    0 passive_days, 
                    1 new_days, 
                    date(now()) last_update, 
                    0 all_months
                from ${src_login_path}_${project}.${cur_im}bots_lifecycle_days a
                    RIGHT JOIN ${src_login_path}_${project}.${cur_im}bots b
                    ON a.id = b.id
                WHERE a.id is null
               ;";;
    esac

    echo ''
    echo [INSERT INTO ${src_login_path}_${project}.${cur_im}bots_lifecycle_days for new bots]
    echo $sql_query_2
    mysql --login-path=$dest_login_path -e "$sql_query_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_lifecycle_days_prod.error

done
