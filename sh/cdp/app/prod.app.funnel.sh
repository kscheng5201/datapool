echo ''
echo `date`
#!/usr/bin/bash
####################################################
# Project: App 互動分析儀表板
# Branch: 行銷漏斗下游整段
# Author: Benson Cheng
# Created_at: 2022-04-14
# Updated_at: 2022-04-14
# Note: 主程式
#####################################################

export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
export type="session"
export table_name="funnel" 
export src_login_path="cdp"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


while read org_id; 
do  
    export sql_0="
        DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            accu_id varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'accu_id',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1 最高10', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_domain (funnel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='客戶自行設定條件的行銷漏斗 level 暫存展開表(long)'
        ;
        
        DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_src;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1-大多至5-最高10', 
            user int DEFAULT NULL COMMENT '行銷漏斗各 level 的人數',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, funnel_id, layer_id),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date), 
            key idx_span (span), 
            key idx_domain (funnel_id), 
            key idx_layer_id (layer_id), 
            key idx_funnel_id (funnel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【行銷漏斗】近 90 天各層轉換率(原始表)'
        ;

        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            funnel_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '行銷漏斗的名稱',
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1 最高10', 
            layer_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '行銷漏斗的階層名稱',
            user int DEFAULT NULL COMMENT '各行銷漏斗 level 的數量',
            conversion_rate float DEFAULT NULL COMMENT '行銷漏斗各層轉換率(由低至高)',
            conversion_overall float DEFAULT NULL COMMENT '行銷漏斗整體轉換率(由低至高)',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, funnel_id, layer_id),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_domain (funnel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【行銷漏斗】近 90 天各層轉換率(完成表)'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp]
    echo $sql_0
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_temp_sql_0.error

    ## 1. 寫入第一層為「全流量」的漏斗資料
    export sql_1a="
        select funnel_id
        from codebook_cdp.funnel_config
        where channel = '${project_name}'
            and org_id = ${org_id}
            and layer_id = 1
            and logic = 'page_url'
        ;"
    echo ''
    echo [SELECT funnel_id as page_url]
    echo $sql_1a
    mysql --login-path=$dest_login_path -e "$sql_1a" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_page_1.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_page_1_sql_1a.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_page_1.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_page_1.txt


    while read page_url_funnel; 
    do 
        export sql_1b="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date,
                    accu_id, 
                    session, 
                    ${page_url_funnel} funnel_id, 
                    1 layer_id, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_event_${org_id}_etl 
                group by accu_id, session
            ;"
        echo ''
        echo [INSERT INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_temp]
        echo $sql_1b
        mysql --login-path=$dest_login_path -e "$sql_1b" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp_sql_1b.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_page_1.txt


    ## 2. 寫入各層為「事件」，但是未設定詳細條件的漏斗資料   
    export sql_2a="
        select concat_ws('-', funnel_id, layer_id, logic_content)
        from codebook_cdp.funnel_config
        where channel = '${project_name}'
            and org_id = ${org_id}
            and logic = 'event'
            and (ev_function is null 
             or ev_function = ''
             or ev_function = 'NULL')
        ;"
    echo ''
    echo [SELECT WHAT logic is event only]
    echo $sql_2a
    mysql --login-path=$dest_login_path -e "$sql_2a" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_only.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_only_sql_2a.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_only.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_only.txt


    while read event_only; 
    do 
        export sql_2b="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date,
                    accu_id, 
                    session, 
                    $(echo ${event_only} | cut -d - -f 1) funnel_id, 
                    $(echo ${event_only} | cut -d - -f 2) layer_id, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_event_${org_id}_etl 
                where type = $(echo ${event_only} | cut -d - -f 3)
                group by accu_id, session
            ;"
        echo ''
        echo [INSERT INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_temp]
        echo $sql_2b
        mysql --login-path=$dest_login_path -e "$sql_2b" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp_sql_2b.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_only.txt


    ## 3. 寫入各層為「事件」，但是有設定詳細條件的漏斗資料   
    export sql_3a="
        select concat_ws('-', funnel_id, layer_id, logic_content, ev_function, attribute)
        from codebook_cdp.funnel_config
        where channel = '${project_name}'
            and org_id = ${org_id}
            and logic = 'event'
            and (ev_function is not null 
             or ev_function <> '')
            and ev_function <> 'NULL'
        ;"
    echo ''
    echo [SELECT WHAT logic is event more]
    echo $sql_3a
    mysql --login-path=$dest_login_path -e "$sql_3a" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_more.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_more_sql_3a.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_more.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_more.txt


    while read event_more; 
    do 
        export sql_3b="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date,
                    accu_id, 
                    session, 
                    $(echo ${event_more} | cut -d - -f 1) funnel_id, 
                    $(echo ${event_more} | cut -d - -f 2) layer_id, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_event_${org_id}_etl 
                where type = $(echo ${event_more} | cut -d - -f 3)
                    and $(echo ${event_more} | cut -d - -f 4) REGEXP '$(echo ${event_more} | cut -d - -f 5)'
                group by accu_id, session
            ;"
        echo ''
        echo [INSERT INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_temp]
        echo $sql_3b
        mysql --login-path=$dest_login_path -e "$sql_3b" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp_sql_3b.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_event_more.txt



    export sql_4="    
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                funnel_id, 
                layer_id,
                count(*) user, 
                now() created_at, 
                now() updated_at
            from (
                select
                    accu_id, 
                    session, 
                    funnel_id, 
                    layer_id,
                    row_number () over (partition by accu_id, session, funnel_id order by layer_id) rid
                from ${project_name}.${type}_${table_name}_${org_id}_temp
                group by
                    accu_id, 
                    session, 
                    funnel_id, 
                    layer_id
                ) a
            where layer_id = rid
            group by funnel_id, layer_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src_sql_4.error
 
    
    export sql_5="    
        delete
        from ${project_name}.${type}_${table_name}_${org_id}_src
        where start_date < '${vDate}' - interval 89 day
            and span = 'daily'
        ; 

        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                funnel_id, 
                layer_id, 
                sum(user) user, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_src
            where span = 'daily'
	    group by funnel_id, layer_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src_sql_5.error 


    export sql_6="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date,     
                c.funnel_id, 
                funnel_name, 
                c.layer_id, 
                layer_name, 
                ifnull(a.user, 0) user, 
                ifnull(100 * round(a.user / ifnull(b.user, a.user), 2), 0) conversion_rate, 
                null conversion_overall, 
                now() created_at, 
                now() updated_at
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_src
                where tag_date = '${vDate}' + interval 1 day
                    and span = '90 days'
                ) a
            
                left join 
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_src
                where tag_date = '${vDate}' + interval 1 day
                    and span = '90 days'
                ) b
                on a.tag_date = b.tag_date
                    and a.span = b.span
                    and a.funnel_id = b.funnel_id 
                    and a.layer_id = b.layer_id + 1
            
                right join
                (
                select funnel_id, funnel_name, layer_id, layer_name
                from codebook_cdp.funnel_config
                where org_id = ${org_id}
                    and channel = '${project_name}'
                group by funnel_id, funnel_name, layer_id, layer_name
                ) c
                on a.funnel_id = c.funnel_id 
                    and a.layer_id = c.layer_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_6.error


    export sql_7="
        UPDATE ${project_name}.${type}_${table_name}_${org_id} a
            INNER JOIN 
            (
            select 
                tag_date, 
                funnel_id, 
                user
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
                and layer_id = 1
            ) b
            ON a.tag_date = b.tag_date
                and a.funnel_id = b.funnel_id
            
            INNER JOIN
            (
            select                 
                tag_date, 
                funnel_id, 
                user
            from (
                select 
                    tag_date, 
                    funnel_id, 
                    user, 
                    row_number () over (partition by funnel_id order by layer_id desc) rid
                from ${project_name}.${type}_${table_name}_${org_id}
                where tag_date = '${vDate}' + interval 1 day
                ) cc
            where rid = 1
            ) c
            ON c.tag_date = b.tag_date
                and c.funnel_id = b.funnel_id
        SET conversion_overall = ifnull(100 * round(c.user / b.user, 5), 0)
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_7
    mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_7.error


    export sql_8="
        UPDATE codebook_cdp.funnel a
            INNER JOIN
            (
            select funnel_id 
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
            group by funnel_id
            ) b
            ON a.id = b.funnel_id
        SET a.status = if(b.funnel_id is null, -1, 1)
        WHERE a.channel = '${project_name}'
        ;"
    echo ''
    echo [UPDATE the funnel status at codebook_cdp.funnel]
    echo $sql_8
    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.funnel_sql_8.error


    export sql_21="
        select concat_ws('_', org_id, id, status) funnel_status
        from codebook_cdp.funnel
        where org_id = ${org_id}
            and channel = '${project_name}'
        ;"
    echo ''
    echo [Get the funnel_status FROM codebook_cdp.funnel]
    echo $sql_21
    mysql --login-path=$dest_login_path -e "$sql_21" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt


    while read funnel_status; 
    do 
        export sql_22="
            UPDATE cdp_data_team.funnel
            SET status = $(echo ${funnel_status} | cut -d _ -f 3)
            WHERE org_id = $(echo ${funnel_status} | cut -d _ -f 1)
                and id = $(echo ${funnel_status} | cut -d _ -f 2)        
            ;"
        echo ''
        echo [UPDATE cdp_data_team.funnel at CDP prod]
        echo $sql_22
        mysql --login-path=${src_login_path}_master -e "$sql_22" 2>>$error_dir/$src_login_path/$project_name/cdp_data_team.funnel.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt


echo ''
echo 'end: ' `date`
