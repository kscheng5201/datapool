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
    while read missing; 
    do
        export sql_query_0="
            select id
            from accunix_v2.${cur_im}bots
            where created_at < date(now())
                and id = $(echo ${missing} | cut -d _ -f 1)
    #            and deleted_at is null
            order by id
           ;"
        mysql --login-path=$src_login_path -e "$sql_query_0" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${cur_im}_table_list.txt
    

            export sql_query_1="
                CREATE TABLE IF NOT EXISTS ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction (   
                    serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique,
                    user_id int NOT NULL COMMENT 'user id',
                    interaction_date date NOT NULL COMMENT '用戶有與機器人互動的日期',
                    first_date date NOT NULL COMMENT '用戶首次與機器人互動的日期',      
                    created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
                    updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',          
                    PRIMARY KEY (user_id, interaction_date), 
                    KEY IDX_interaction_date (interaction_date),   
                    KEY IDX_user_id (user_id)        
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用戶與機器人互動的每日紀錄'
                ;
                CREATE TABLE IF NOT EXISTS ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan (  
                    serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique,
                    user_id int NOT NULL COMMENT 'user id',
                    start_date date DEFAULT NULL COMMENT '用戶首次與機器人互動的日期',          
                    last_date date DEFAULT NULL COMMENT '用戶最後一次與機器人互動的日期',            
                    freq int DEFAULT NULL COMMENT '互動次數',
                    life_cycle int DEFAULT NULL COMMENT '生命週期; 單位：日',  
                    cycle_time int DEFAULT NULL COMMENT '首次互動日與最近互動日的平均間隔日數',
                    created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
                    updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',          
                    PRIMARY KEY (user_id),
                    KEY idx_last_date (last_date)    
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用戶與機器人互動的統整紀錄'
                ;"                
            # Create Table
            echo ''
            echo 'start: ' `date` 
            echo create table if not exists ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction and interaction
            mysql --login-path=$dest_login_path -e "$sql_query_1"
    
            export sql_query_2="
                SELECT 
                   id user_id, 
                   DATE(created_at) interaction_date,
                   DATE(created_at) first_date, 
                   now() created_at
                FROM accunix_v2_log.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_users
                where id in ($(echo ${missing} | cut -d _ -f 2))
                   AND created_at < '${vDate}' + interval 1 day
                ;"
        echo ''
        echo $sql_query_2
    
            export sql_query_3="        
                SELECT 
                   user_id, interaction_date, now() created_at
                FROM (
                   SELECT 
                       DISTINCT user_id, 
                       DATE(created_at) interaction_date
                   FROM accunix_v2_log.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_chatroom_logs
                    where user_id in ($(echo ${missing} | cut -d _ -f 2))
                       AND created_at < '${vDate}' + interval 1 day
                       AND sender = 'User'
                
                   UNION ALL
                            
                   SELECT 
                       DISTINCT user_id, 
                       DATE(created_at) interaction_date
                   FROM accunix_v2_log.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_redirect_logs
                   where user_id in ($(echo ${missing} | cut -d _ -f 2))
                        AND created_at < '${vDate}' + interval 1 day        
                   ) a
                GROUP BY user_id, interaction_date
                ;"
        echo ''
        echo $sql_query_3
    
            # Export Data
            echo ''
            echo 'start: ' `date` 
            echo ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction_first
            mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction_first.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction_first.error
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction_first.txt' INTO TABLE ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction IGNORE 1 LINES (user_id, interaction_date, first_date, created_at);" 2>>$error_dir/$project_name.${cur_im}_$(echo ${missing} | cut -d _ -f 1)_interaction_first.error 
    
            # Export Data
            echo ''
            echo 'start: ' `date` 
            echo [INSERT INTO ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction]
            mysql --login-path=$src_login_path -e "$sql_query_3" > $export_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction.error
            mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction.txt' IGNORE INTO TABLE ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction IGNORE 1 LINES (user_id, interaction_date, created_at);" 2>>$error_dir/$project_name.${cur_im}_$(echo ${missing} | cut -d _ -f 1)_interaction.error 


        export sql_query_4="        
            UPDATE ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction a
                LEFT JOIN
                (
                select user_id, min(interaction_date) first_date
                from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction
                where user_id in (
                    select user_id
                    from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction
                    where first_date is null
                    ) 
                group by user_id
                ) b 
                ON a.user_id = b.user_id
            SET a.first_date = b.first_date,
               a.created_at = now()
            WHERE a.first_date is null
            ;
            delete
            from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction
            where interaction_date is null 
                and interaction_date < 'S{vDate}' - interval 180 day
            ;"
        # Update Data
        echo ''
	echo [start ${vDate} date at `date`] 
        echo [update ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction]
        mysql --login-path=$dest_login_path -e "$sql_query_4"


        export sql_5="
            UPDATE ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan a
                LEFT JOIN
                (
                select user_id, max(interaction_date) last_date, count(*) freq
                from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction
                group by user_id
                ) b
                ON a.user_id = b.user_id
            SET a.last_date = if(b.last_date > a.last_date, b.last_date, a.last_date), 
                a.freq = a.freq + if(b.last_date > a.last_date, b.freq, 0),
                a.life_cycle = datediff(if(b.last_date > a.last_date, b.last_date, a.last_date), start_date), 
                a.cycle_time = ifnull(round(datediff(if(b.last_date > a.last_date, b.last_date, a.last_date), start_date) / (a.freq + if(b.last_date > a.last_date, b.freq, 0) - 1)), 0)
            ;"
        echo ''
        echo [start ${vDate} date at `date`] 
        echo [UPDATE ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan]
        mysql --login-path=$dest_login_path -e "$sql_5"

        export sql_6="
            INSERT INTO ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan
                select 
                    null serial, 
                    b.*, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan a
                    RIGHT JOIN
                    (
                    select 
                        user_id, 
                        first_date start_date, 
                        max(interaction_date) last_date, 
                        count(*) freq, 
                        datediff(max(interaction_date), first_date) life_cycle, 
                        ifnull(round(datediff(max(interaction_date), first_date) / (count(*) - 1)), 0) cycle_time
                    from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_interaction
                    group by user_id, first_date
                    ) b
                    ON a.user_id = b.user_id
                where a.user_id is null
            ;"
        echo ''
        echo [start ${vDate} date at `date`] 
        echo [INSERT INTO ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan for new user_id]
        mysql --login-path=$dest_login_path -e "$sql_6"


        export sql_7="
            select group_concat(active_days)
            from (
                select ifnull(round(avg(cycle_time)), 0) active_days
                from (
                    select cycle_time, row_number () over (order by cycle_time) rid
                    from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan
                    where cycle_time > 0 
                        and cycle_time is not null
                        and cycle_time <> ''
                    ) aa
                where rid in (
                    floor(((
                        select count(*)
                        from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan 
                        where cycle_time > 0 
                            and cycle_time is not null
                            and cycle_time <> ''
                        ) + 1) / 2), 
                    ceiling((((
                        select count(*)
                        from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan 
                        where cycle_time > 0 
                            and cycle_time is not null
                            and cycle_time <> ''
                        ) + 1) / 2))
                    )
                
                UNION ALL

                select ifnull(round(avg(life_cycle)), 0) passive_days
                from (
                    select life_cycle, row_number () over (order by life_cycle) rid
                    from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan
                    where life_cycle > 0 
                        and life_cycle is not null
                        and life_cycle <> ''
                    ) aa
                where rid in (
                    floor(((
                        select count(*)
                        from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan 
                        where life_cycle > 0 
                            and life_cycle is not null
                            and life_cycle <> ''
                        ) + 1) / 2), 
                    ceiling((((
                        select count(*)
                        from ${project_name}.${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)_lifespan 
                        where life_cycle > 0 
                            and life_cycle is not null
                            and life_cycle <> ''
                        ) + 1) / 2))
                    )
                ) n
		;"
            echo ''
            echo [Get the active_days and passive_days value on ${cur_im}bot_$(echo ${missing} | cut -d _ -f 1)]
            mysql --login-path=$dest_login_path -e "$sql_7" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_$(echo ${missing} | cut -d _ -f 1)_active_days.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_$(echo ${missing} | cut -d _ -f 1)_active_days.error
            sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_$(echo ${missing} | cut -d _ -f 1)_active_days.txt
            
            while read active_days; 
                do 
                export sql_8="
                    UPDATE ${project_name}.${cur_im}bots_lifecycle_days
                    SET active_days = $(echo ${active_days} | cut -d , -f 1),
                        passive_days = $(echo ${active_days} | cut -d , -f 2),
                        new_days = 1, 
                        last_update = date(now()), 
                        all_months = ifnull(all_months, 0)
                    WHERE id = $(echo ${missing} | cut -d _ -f 1)
                    ;"
		echo ''
		echo [UPDATE ${project_name}.${cur_im}bot_lifecycle_days]
		mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/$project_name.${project_name}.${cur_im}bot_lifecycle_days.error

            done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_$(echo ${missing} | cut -d _ -f 1)_active_days.txt
    done < /root/datapool/sh/nix/bots_lifespan/missing_line_bots.txt
done

echo 'end: ' `date`
