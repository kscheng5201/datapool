#!/usr/bin/bash

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="bots_lifespan"


# IM: Instant Messenger
IM="fbmessenger line"

src_login_path='nix'
for cur_im in $IM;
do
  # check the id number of each bots
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
#            where updated_at >= date_format(now(), '%Y-%m-01' - interval 1 month)
#		and updated_at < date_format(now(), '%Y-%m-01' - interval 1 month + interval 1 month)
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
#	    where updated_at >= date_format(now(), '%Y-%m-01' - interval 1 month)
#		and updated_at < date_format(now(), '%Y-%m-01' - interval 1 month) + interval 1 month
           ;";;
esac


echo ''
echo [Export data]
echo $cur_im
echo $sql_query_1
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bots.error

echo ''
echo [Import data]
echo [TRUNCATE TABLE bots_lifespan.${cur_im}bots]
echo [LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots.txt' INTO TABLE bots_lifespan.${cur_im}bots IGNORE 1 LINES]
mysql --login-path=$dest_login_path -e "TRUNCATE TABLE bots_lifespan.${cur_im}bots;"
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${cur_im}bots.txt' INTO TABLE bots_lifespan.${cur_im}bots IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bots.error

done


for cur_im in $IM;
do
  # check the id number of each bots
case $cur_im in
	fbmessenger)	
	export sql_query_2="
	    SET NAMES utf8mb4
        ;
        INSERT INTO ${project_name}.${cur_im}bots_lifecycle_days
            select 
                b.id, 
                organization_id, 
                0 active_days, 
                0 passive_days, 
                1 new_days, 
                date(now()) last_update, 
                0 all_months
            from ${project_name}.${cur_im}bots_lifecycle_days a
                RIGHT JOIN ${project_name}.${cur_im}bots b
                ON a.id = b.id
            WHERE a.id is null
       	    ;";;

        line)
    export sql_query_2="
        SET NAMES utf8mb4
        ;
        INSERT INTO ${project_name}.${cur_im}bots_lifecycle_days
            select 
                b.id, 
                organization_id, 
                0 active_days, 
                0 passive_days, 
                1 new_days, 
                date(now()) last_update, 
                0 all_months
            from ${project_name}.${cur_im}bots_lifecycle_days a
                RIGHT JOIN ${project_name}.${cur_im}bots b
                ON a.id = b.id
            WHERE a.id is null
           ;";;
esac


echo ''
echo [Export data]
echo $cur_im
echo $sql_query_2
mysql --login-path=$dest_login_path -e "$sql_query_2" 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bots_lifecycle_days.error

done
