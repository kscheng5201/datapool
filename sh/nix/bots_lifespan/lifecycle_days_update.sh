#!/usr/bin/bash

export dest_login_path="nix"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="bots_lifespan"


# IM: Instant Messenger
IM="fbmessenger line"

src_login_path='datapool'
for cur_im in $IM;
do
  # check the id number of each bots
case $cur_im in
	fbmessenger)	
	export sql_query_1="
            select 
                id fbbot_id, 
                ifnull(active_days, 0) active_days,  
                ifnull(passive_days, 0) passive_days, 
                new_days, 
                last_update, 
                ifnull(all_months, 0) all_months
            from bots_lifespan.fbmessengerbots_lifecycle_days
	    order by id
       	    ;";;

        line)
	export sql_query_1="
            select 
                id linebot_id, 
                ifnull(active_days, 0) active_days,  
                ifnull(passive_days, 0) passive_days, 
                new_days, 
                last_update, 
                ifnull(all_months, 0) all_months
            from bots_lifespan.linebots_lifecycle_days
	    order by id
           ;";;
esac

# echo $cur_im
# echo $sql_query_1

# Export data
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$dest_login_path/$project_name/$project_name.${cur_im}bots_lifecycle_days.txt 2>>$error_dir/$dest_login_path/$project_name/$project_name.${cur_im}bots_lifecycle_days.error

# Import data
mysql --login-path=${dest_login_path}_master -e "DELETE FROM accunix_data.${cur_im}bots_lifecycle_days;"
mysql --login-path=${dest_login_path}_master -e "LOAD DATA LOCAL INFILE '$export_dir/$dest_login_path/$project_name/$project_name.${cur_im}bots_lifecycle_days.txt' REPLACE INTO TABLE accunix_data.${cur_im}bots_lifecycle_days IGNORE 1 LINES;" 2>>$error_dir/$dest_login_path/$project_name/$project_name.${cur_im}bots_lifecycle_days.error


done


# mysql --login-path=$dest_login_path -e "UPDATE accunix_data.linebots_lifecycle_days SET last_update = date(now()); "
# mysql --login-path=$dest_login_path -e "UPDATE accunix_data.fbmessengerbots_lifecycle_days SET last_update = date(now()); "

#### 例外處理 ####
export sql_x="
    delete
    from accunix_data.${cur_im}bots_lifecycle_days
    where ${cur_im}bot_id in
        (
        select ${cur_im}bot_id
        from (
            select ${cur_im}bot_id, count(*)
            from accunix_data.${cur_im}bots_lifecycle_days
            group by ${cur_im}bot_id
            having count(*) > 1
            ) a
        )
        and active_days = 0
    ;"
