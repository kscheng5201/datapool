#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 行銷漏斗
# Author: Benson Cheng
# Created_at: 2022-01-07
# Updated_at: 2022-01-07
# Note: 包含預設漏斗與客製化漏斗
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
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
    export delete_old="
        DELETE 
        FROM ${project_name}.${type}_${table_name}_${org_id}
        WHERE funnel_id in 
            (
            select id
            from ${project_name}.funnel
            where org_id = ${org_id}
                and status = 0
                and channel = 'web'
            )
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"    
    echo ''
    echo [DELETE the funnel which was resetting]
    echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id}]    
    mysql --login-path=$dest_login_path -e "$delete_old" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error 

    ########################################
    #### the default funnel layer model ####
    ########################################
    
    export sql_1="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            campaign_start date DEFAULT NULL COMMENT '活動開始日期', 
            campaign_end date DEFAULT NULL COMMENT '活動結束日期',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1-大多至5-最高10', 
            user int DEFAULT NULL COMMENT '行銷漏斗各 level 的人次',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, funnel_id, layer_id),
            key idx_tag_date (tag_date),  
            key idx_span (span), 
            key idx_campaign_id (campaign_id), 
            key idx_layer_id (layer_id), 
            key idx_funnel_id (funnel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析【行銷漏斗】活動期間各層轉換率(原始表)'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            campaign_start date DEFAULT NULL COMMENT '活動開始日期', 
            campaign_end date DEFAULT NULL COMMENT '活動結束日期',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            funnel_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '行銷漏斗的名稱',
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1 最高10', 
            layer_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '行銷漏斗的階層名稱',
            user int DEFAULT NULL COMMENT '各行銷漏斗 level 的人次',
            conversion_rate float DEFAULT NULL COMMENT '行銷漏斗各層轉換率(由低至高)',
            conversion_overall float DEFAULT NULL COMMENT '行銷漏斗整體轉換率(由低至高)',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, funnel_id, layer_id),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_campaign_id (campaign_id), 
            key idx_layer_id (layer_id), 
            key idx_funnel_id (funnel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='廣告成效分析【行銷漏斗】活動期間各層轉換率(完成表)'
        ;" 
    echo ''
    echo $sql_1
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src and ${project_name}.${type}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_1.error

    while read campaign_detail; 
    do
    	echo [campaign_detail]
        cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt

        export sql_2=" 
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    $(echo ${campaign_detail} | cut -d _ -f 1) campaign_id, 
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start,
                    '$(echo ${campaign_detail} | cut -d _ -f 3)' campaign_end,
                    (select id from ${project_name}.funnel where org_id = ${org_id} and channel = 'web' and is_default = 1) funnel_id, 
                    layer_id,
                    count(*) user, 
                    now() created_at, 
                    now() updated_at
                from (
                    select
                        campaign_id, 
                        fpc, 
                        session, 
                        ifnull(layer_id, 1) layer_id, 
                        row_number () over (partition by campaign_id, fpc, session order by layer_id) rid
                    from ${project_name}.${type}_both_${org_id}_etl_log a
                        left join
                        (
                        select *
                        from ${project_name}.funnel_config
                        where org_id = ${org_id} 
                            and funnel_name = '消費'
                            and logic = 'event'
                            and channel = 'web'
                        ) b
                        on a.event_type = b.logic_content
                    where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                        and a.created_at >= '$(echo ${campaign_detail} | cut -d _ -f 2)'
                        and a.created_at < if('${vDate}' <= '$(echo ${campaign_detail} | cut -d _ -f 3)', '${vDate}', '$(echo ${campaign_detail} | cut -d _ -f 3)') + interval 1 day
                    group by 
                        campaign_id, 
                        fpc, 
                        session, 
                        layer_id
                    ) c
                where layer_id = rid
            group by layer_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
            ;"
        echo ''
        echo $sql_2
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
        mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_src_sql_2.error   

        export sql_5="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    $(echo ${campaign_detail} | cut -d _ -f 1) campaign_id, 
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start,
                    '$(echo ${campaign_detail} | cut -d _ -f 3)' campaign_end,
                    c.funnel_id, 
                    funnel_name, 
                    c.layer_id, 
                    layer_name, 
                    ifnull(a.user, 0) user, 
                    ifnull(100 * round(a.user / ifnull(b.user, a.user), 3), 0) conversion_rate, 
                    null conversion_overall, 
                    now() created_at, 
                    now() updated_at
                from (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id}_src
                    where tag_date = '${vDate}' + interval 1 day
                        and span = 'FULL'
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    ) a
                
                    left join 
                    (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id}_src
                    where tag_date = '${vDate}' + interval 1 day
                        and span = 'FULL'
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    ) b
                    on a.tag_date = b.tag_date
                        and a.span = b.span
                        and a.funnel_id = b.funnel_id 
                        and a.layer_id = b.layer_id + 1
                        and a.campaign_id = b.campaign_id
                
                    right join
                    (
                    select funnel_id, funnel_name, layer_id, layer_name
                    from ${project_name}.funnel_config
                    where org_id = ${org_id}
                        and funnel_name = '消費'
                        and channel = 'web'
                    ) c
                    on a.funnel_id = c.funnel_id 
                        and a.layer_id = c.layer_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} with funnel_name and layer_name]
        echo $sql_5
        mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_5.error

        export sql_6="
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET conversion_overall = 
                (
                select 100 * round(layer_5 / layer_1, 5)
                from (
                    select 
                        max(if(layer_id = 5, user, null)) layer_5, 
                        max(if(layer_id = 1, user, null)) layer_1
                    from ${project_name}.${type}_${table_name}_${org_id}
                    where tag_date = '${vDate}' + interval 1 day 
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    ) a
                )
            WHERE tag_date = '${vDate}' + interval 1 day
                and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
            ;"        
        echo ''
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on conversion_overall]
        echo $sql_6
        mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_after_sql_6.error

        echo ''
        echo [Is ${vDate} = $(echo ${campaign_detail} | cut -d _ -f 3)?]
        if [ ${vDate} = $(echo ${campaign_detail} | cut -d _ -f 3) ];
            then 
                echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id} WHERE campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1) and tag_date <= '$(echo ${campaign_detail} | cut -d _ -f 3)' - interval 1 day]
                mysql --login-path=$dest_login_path -e "DELETE FROM ${project_name}.${type}_${table_name}_${org_id} WHERE campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1) and tag_date <= '$(echo ${campaign_detail} | cut -d _ -f 3)' - interval 1 day;" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_6.error
            else 
                echo [the campaign $(echo ${campaign_detail} | cut -d _ -f 1) is going on since $(echo ${campaign_detail} | cut -d _ -f 2) to $(echo ${campaign_detail} | cut -d _ -f 3)]
        fi
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
    
    echo ''
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_src;"

    ###########################################
    #### the customized funnel layer model ####
    ###########################################

    export sql_9="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1 最高10', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_campaign_id (campaign_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='客戶自行設定條件的行銷漏斗 level 暫存展開表(long)'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp]
    mysql --login-path=$dest_login_path -e "$sql_9" 


    #### loop by funnel_id ####
    export sql_10="
        select funnel_id
        from ${project_name}.funnel_config
        where org_id = ${org_id}
            and channel = 'web'
        group by funnel_id
        order by funnel_id
        ;"    
    echo ''
    echo [Get the funnel_id]
    mysql --login-path=$dest_login_path -e "$sql_10" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt


    while read funnel_id; 
    do 
        export sql_11="
            select concat(layer_id, '_', group_concat(logic_content separator '|')) logic_content
            from ${project_name}.funnel_config
            where org_id = ${org_id}
                and funnel_id = ${funnel_id}
                and layer_id >= 2
                and logic = 'page_url'
            group by layer_id
            ;"
        echo ''
        echo [Get the logic_content]
        mysql --login-path=$dest_login_path -e "$sql_11" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_logic_content.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_logic_content.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_logic_content.txt

        
        while read logic_content; 
        do 
            export sql_12="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        campaign_id, 
                        fpc, 
                        session, 
                        ${funnel_id} funnel_id,
                        $(echo ${logic_content} | cut -d _ -f 1) layer_id, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_both_${org_id}_etl_log
                    where behavior = 'page_view' 
                        and page_url REGEXP '$(echo ${logic_content} | cut -d _ -f 2)'
                    group by 
                        campaign_id, 
                        fpc, 
                        session
                ;"
            echo ''
            echo $sql_12
            echo [Get the layer_id by page_url 1 by 1]
            mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_logic_content.error

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_logic_content.txt


        export sql_13="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    campaign_id, 
                    fpc, 
                    session, 
                    ${funnel_id} funnel_id,
                    1 layer_id, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_both_${org_id}_etl_log
                where behavior = 'page_view' 
                    and page_url NOT REGEXP 
                        (
                        select group_concat(logic_content separator '|') logic_content
                        from ${project_name}.funnel_config
                        where org_id = ${org_id}
                            and funnel_id = ${funnel_id}
                            and layer_id >= 2
                            and logic = 'page_url'                    
                        )
                group by 
                    campaign_id, 
                    fpc, 
                    session        
            ;"
        echo ''
        echo $sql_13
        echo [Get the layer_id by page_url which is not included any logic_content]
        mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_logic_content.error

        
        for layer_id in $(seq 2 10); 
        do
            export sql_14="
                select concat_ws('_', logic_content, ev_function, attribute) ev_layer
                from ${project_name}.funnel_config
                where org_id = ${org_id}
                    and funnel_id = ${funnel_id}
                    and logic = 'event'
                    and layer_id = ${layer_id}
                ;"
            echo ''
            echo $sql_14
            echo [Get the event_layer with the function and attribute]
            mysql --login-path=$dest_login_path -e "$sql_14" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.error
            sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt
            
            
            while read ev_layer; 
            do 
                export sql_15="
                    INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp    
                        select 
                            null serial, 
                            '${vDate}' + interval 1 day tag_date, 
                            campaign_id, 
                            fpc, 
                            session,
                            ${funnel_id} funnel_id, 
                            if($(echo ${ev_layer} | cut -d _ -f 2) like '%$(echo ${ev_layer} | cut -d _ -f 3)%', 
                                ${layer_id}, 1) layer_id,  
                            now() created_at, 
                            now() updated_at
                        from ${project_name}.${type}_both_${org_id}_etl_log
                        where behavior = 'event' 
                            and event_type = $(echo ${ev_layer} | cut -d _ -f 1)
                        group by 
                            campaign_id, 
                            fpc, 
                            session
                    ;"
                echo ''
                echo $sql_15
                echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp on ${ev_layer}]
                mysql --login-path=$dest_login_path -e "$sql_15" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.error

            done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt 
        done
        
        export sql_16="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp    
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    campaign_id, 
                    fpc, 
                    session,
                    ${funnel_id} funnel_id, 
                    1 layer_id,  
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_both_${org_id}_etl_log
                where behavior = 'event' 
                    and event_type not in
                        (         
                        select group_concat(distinct logic_content)
                        from ${project_name}.funnel_config
                        where org_id = ${org_id}
                            and funnel_id = ${funnel_id}
                            and logic = 'event'
                        )
                group by 
                    campaign_id, 
                    fpc, 
                    session
            ;"
        echo ''
        echo $sql_16
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp on others]
        mysql --login-path=$dest_login_path -e "$sql_16" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_temp.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt


    while read campaign_detail; 
    do
        campaign_start=`(date -d $(echo ${campaign_detail} | cut -d _ -f 2) +"%Y%m%d")`
        campaign_end=`(date -d $(echo ${campaign_detail} | cut -d _ -f 3) +"%Y%m%d")`  
    
        export sql_17="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    '${campaign_start}' start_date, 
                    if('${vDate}' <= '${campaign_end}', '${vDate}', '${campaign_end}') end_date, 
                    $(echo ${campaign_detail} | cut -d _ -f 1) campaign_id,
                    funnel_id, 
                    null funnel_name, 
                    layer_id,
                    null layer_name, 
                    count(*) user, 
                    null conversion_rate, 
                    null conversion_overall, 
                    now() created_at, 
                    now() updated_at
                from (
                    select
                        campaign_id, 
                        fpc, 
                        session, 
                        funnel_id, 
                        layer_id,
                        row_number () over (partition by campaign_id, fpc, session, funnel_id order by layer_id) rid
                    from ${project_name}.${type}_${table_name}_${org_id}_temp
                    where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                        and created_at >= '${campaign_start}'
                        and created_at < '${campaign_end}' + interval 1 day
                    group by
                        campaign_id, 
                        fpc, 
                        session, 
                        funnel_id, 
                        layer_id
                    ) a
                where layer_id = rid
                group by funnel_id, layer_id
                order by funnel_id, layer_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
            #;
            #DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp
            ;"
        echo ''
        echo $sql_17
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
        echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp]    
        mysql --login-path=$dest_login_path -e "$sql_17" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_src.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
    
    
        export sql_18="    
            delete
            from ${project_name}.${type}_${table_name}_${org_id}_src
            where start_date < '${vDate}' - interval 89 day
                and funnel_id >= ${org_id}
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
                    and funnel_id >= ${org_id}
            group by funnel_id, layer_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
        #mysql --login-path=$dest_login_path -e "$sql_18" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error 
    
        export sql_19="
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
                    from ${project_name}.funnel_config
                    where org_id = ${org_id}
                    group by funnel_id, funnel_name, layer_id, layer_name
                    ) c
                    on a.funnel_id = c.funnel_id 
                        and a.layer_id = c.layer_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;
    
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
        echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
        #mysql --login-path=$dest_login_path -e "$sql_19" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error


    export sql_20="
        UPDATE ${project_name}.funnel a
            INNER JOIN
            (
            select 
                ${org_id} org_id,
                funnel_id 
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
            group by funnel_id
            ) b
            ON a.org_id = b.org_id
                and a.id = b.funnel_id
        SET a.status = if(b.funnel_id is null, -1, 1)
        WHERE a.channel = 'web'
            and a.status <> 1
        ;"
    echo ''
    echo [UPDATE the funnel status at ${project_name}.funnel]
    #mysql --login-path=$dest_login_path -e "$sql_20" 2>>$error_dir/$src_login_path/$project_name/${project_name}.funnel.error


    export sql_21="
        select concat_ws('_', org_id, id, status) funnel_status
        from ${project_name}.funnel
        where org_id = ${org_id}
        ;"
    echo ''
    echo [Get the funnel_status FROM ${project_name}.funnel]
    #mysql --login-path=$dest_login_path -e "$sql_21" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.error
    #sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt


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
        #mysql --login-path=${src_login_path}_master -e "$sql_22" 2>>$error_dir/$src_login_path/$project_name/cdp_data_team.funnel.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
#done < /root/datapool/export_file/cdp/web/web.cdp_prod_org_id.txt
echo ''
echo 'end: ' `date`
