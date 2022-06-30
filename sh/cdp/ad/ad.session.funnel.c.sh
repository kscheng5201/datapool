#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 行銷漏斗
# Author: Benson Cheng
# Created_at: 2022-01-07
# Updated_at: 2022-01-13
# Note: 客製化漏斗
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
    ###########################################
    #### the customized funnel layer model ####
    ###########################################
    
    export sql_1="   
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
        ;
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
            #primary key (tag_date, span, campaign_id, funnel_id, layer_id),
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
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_sql_1.error


    while read campaign_detail; 
    do
    	echo [campaign_detail]
        cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt

        #### loop by funnel_id ####
        export sql_2="
            select id funnel_id
            from ${project_name}.funnel
            where org_id = ${org_id}
                and name <> '消費'
                and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
            ;"    
        echo ''
        echo [Get the funnel_id]
        echo $sql_2
        mysql --login-path=$dest_login_path -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt
        echo ''
        echo [campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1) and its funnel_id list as]
        cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt
        

        while read funnel_id; 
        do 
            export sql_3="
                select 
                    concat(logic, 
                        group_concat(if(logic_content is null or logic_content in ('', 'NULL'), '', logic_content) separator '|')
                        ) layer1                
                from ${project_name}.funnel_config
                where org_id = ${org_id}
                    and funnel_id = ${funnel_id}
                    and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    and logic = 'page_url'
                    and layer_id = 1
                group by logic
                ;"
            echo ''
            echo [Get the layer1]
            echo $sql_3
            mysql --login-path=$dest_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.error
            sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
            echo ''
            echo [campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1) and funnel_id = ${funnel_id} and layer_id = 1 as ]            
            cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
            
            
            while read layer1; 
            do 
                if [ ${layer1} = page_url ]; 
                then 
                    export sql_4="
                        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
                            select 
                                null serial, 
                                '${vDate}' + interval 1 day tag_date, 
                                'FULL' span, 
                                campaign_id, 
                                null campaign_start, 
                                null campaign_end, 
                                ${funnel_id} funnel_id, 
                                1 layer_id,
                                count(distinct fpc, session) user, 
                                now() created_at, 
                                now() updated_at
                            from ${project_name}.${type}_both_${org_id}_etl_log
                            where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                            group by campaign_id
                        ;
                        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                        ;"
                    echo ''
                    echo [假如第一層是全流量（沒有條件）]
                    echo [寫入第一層資料]
                    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
                    echo $sql_4
                    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error

                else
                    export sql_4="
                        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
                            select 
                                null serial, 
                                '${vDate}' + interval 1 day tag_date, 
                                'FULL' span, 
                                campaign_id, 
                                null campaign_start, 
                                null campaign_end, 
                                ${funnel_id} funnel_id, 
                                1 layer_id,
                                count(distinct fpc, session) user, 
                                now() created_at, 
                                now() updated_at
                            from ${project_name}.${type}_both_${org_id}_etl_log
                            where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                                and behavior = 'page_view'
                                and page_url REGEXP '$(echo ${layer1} | cut -d page_url -f 2)'
                            group by campaign_id
                        ;
                        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                        ;"
                    echo ''
                    echo [假如第一層是 page_url（有條件）]
                    echo [寫入第一層資料]
                    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
                    echo $sql_4
                    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error                   

                fi
            done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt            

            
            export sql_5="
                select concat(layer_id, '_', group_concat(logic_content separator '|')) page_content
                from ${project_name}.funnel_config
                where org_id = ${org_id}
                    and funnel_id = ${funnel_id}
                    and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    and logic = 'page_url'
                    and layer_id >= 2
                group by layer_id
                ;"
            echo ''
            echo [Get the page_content]
            echo $sql_5
            mysql --login-path=$dest_login_path -e "$sql_5" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_page_content.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_page_content.error
            sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_page_content.txt
    
            while read page_content; 
            do 
                export sql_6="
                    INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp  
                        select 
                            null serial, 
                            '${vDate}' + interval 1 day tag_date, 
                            campaign_id, 
                            fpc, 
                            session,
                            ${funnel_id} funnel_id, 
                            $(echo ${page_content} | cut -d _ -f 1) layer_id,
                            now() created_at, 
                            now() updated_at
                        from ${project_name}.${type}_both_${org_id}_etl_log
                        where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                            and behavior = 'page_view' 
                            and page_url REGEXP '$(echo ${page_content} | cut -d _ -f 2)'
                    ;"
                echo ''
                echo [Get the layer_id by page_url 1 by 1]
                echo [假如第二層以上是 page_url]
                echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp]
                echo $sql_6   
                mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_temp.error
    
            done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_page_content.txt    

            
            for layer_id in $(seq 7); 
                # 每個客戶最多 10 個漏斗，最多 7 層，每層最多有 10 個條件
            do
                export sql_7="
                    select concat_ws('-', ifnull(logic_content, ''), ifnull(ev_function, ''), ifnull(attribute, '')) ev_layer
                    from ${project_name}.funnel_config
                    where org_id = ${org_id}
                        and funnel_id = ${funnel_id}
                        and logic = 'event'
                        and layer_id = ${layer_id}
                    ;"
                echo ''
                echo [Get the event_layer with the function and attribute]
                echo $sql_7
                mysql --login-path=$dest_login_path -e "$sql_7" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.error
                sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt
                echo ''
                cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt
                
                while read ev_layer;
                do 
                    export sql_8="
                        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp
                            select 
                                null serial, 
                                '${vDate}' + interval 1 day tag_date, 
                                campaign_id, 
                                fpc, 
                                session,
                                ${funnel_id} funnel_id, 
                                if($(echo ${ev_layer} | cut -d - -f 2) REGEXP '$(echo ${ev_layer} | cut -d - -f 3)', 
                                    ${layer_id}, 1) layer_id,  
                                now() created_at,
                                now() updated_at
                            from ${project_name}.${type}_event_${org_id}_src_log
                            where campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                                and type = $(echo ${ev_layer} | cut -d - -f 1) 
                            group by 
                                campaign_id, 
                                fpc, 
                                session
                        ;"
                    echo ''
                    echo [任何一層是事件的全部計算]
                    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp on ${ev_layer}]                    
                    echo $sql_8                    
                    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.error

                done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt
            done
        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt
        
        export sql_9="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    $(echo ${campaign_detail} | cut -d _ -f 1) campaign_id,
                    '$(echo ${campaign_detail} | cut -d _ -f 2)' campaign_start, 
                    if('${vDate}' <= '$(echo ${campaign_detail} | cut -d _ -f 3)', '${vDate}', '$(echo ${campaign_detail} | cut -d _ -f 3)') campaign_end, 
                    funnel_id, 
                    layer_id,
                    count(*) user, 
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
                    group by
                        campaign_id, 
                        fpc, 
                        session, 
                        funnel_id, 
                        layer_id
                    ) a
                where layer_id = rid
                group by funnel_id, layer_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
        echo $sql_9        
        mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_src.error
        
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
    
    
    export sql_10="    
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '-FULL-' span, 
                campaign_id, 
                max(campaign_start) campaign_start, 
                max(campaign_end) campaign_end,
                funnel_id, 
                layer_id, 
                sum(user) user, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_src
            group by campaign_id, funnel_id, layer_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_10
    mysql --login-path=$dest_login_path -e "$sql_10" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error 

    
    while read campaign_detail; 
    do 
        export sql_11="
            INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}
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
                    ifnull(100 * round(a.user / ifnull(b.user, a.user), 2), 0) conversion_rate, 
                    null conversion_overall, 
                    now() created_at, 
                    now() updated_at
                from (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id}_src
                    where tag_date = '${vDate}' + interval 1 day
                        and span = '-FULL-'
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    ) a
                
                    left join 
                    (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id}_src
                    where tag_date = '${vDate}' + interval 1 day
                        and span = '-FULL-'
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    ) b
                    on a.tag_date = b.tag_date
                        and a.span = b.span
                        and a.campaign_id = b.campaign_id
                        and a.funnel_id = b.funnel_id 
                        and a.layer_id = b.layer_id + 1
                
                    right join
                    (
                    select campaign_id, funnel_id, funnel_name, layer_id, layer_name
                    from ${project_name}.funnel_config
                    where org_id = ${org_id}
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                    group by campaign_id, funnel_id, funnel_name, layer_id, layer_name
                    ) c
                    on a.funnel_id = c.funnel_id 
                        and a.layer_id = c.layer_id
                        and a.campaign_id = c.campaign_id
            ; 
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}]
        echo $sql_11
        mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt


    export sql_12="
        UPDATE ${project_name}.${type}_${table_name}_${org_id} a
            INNER JOIN 
            (
            select 
                tag_date, 
                campaign_id,
                funnel_id, 
                user
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
                and layer_id = 1
            ) b
            ON a.tag_date = b.tag_date
                and a.funnel_id = b.funnel_id
                and a.campaign_id = b.campaign_id
            
            INNER JOIN
            (
            select                 
                campaign_id,
                tag_date, 
                funnel_id, 
                user
            from (
                select 
                    campaign_id,
                    tag_date, 
                    funnel_id, 
                    user, 
                    row_number () over (partition by campaign_id, funnel_id order by layer_id desc) rid
                from ${project_name}.${type}_${table_name}_${org_id}
                where tag_date = '${vDate}' + interval 1 day
                ) cc
            where rid = 1
            ) c
            ON c.tag_date = b.tag_date
                and c.funnel_id = b.funnel_id
                and c.campaign_id = b.campaign_id
        SET conversion_overall = ifnull(100 * round(c.user / b.user, 5), 0)
        ;"
    echo ''
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_12
    mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error


    export sql_13="
        UPDATE ${project_name}.funnel a
            INNER JOIN
            (
            select 
                ${org_id} org_id,
                campaign_id,
                funnel_id 
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
            group by campaign_id, funnel_id
            ) b
            ON a.org_id = b.org_id
                and a.id = b.funnel_id
                and a.campaign_id = b.campaign_id
        SET a.status = if(b.funnel_id is null, -1, 1)
        WHERE a.status <> 1
        ;"
    echo ''
    echo [UPDATE the funnel status at ${project_name}.funnel]
    echo $sql_13
    mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/${project_name}.funnel.error


    export sql_14="
        select concat_ws('_', org_id, id, status) funnel_status
        from ${project_name}.funnel
        where org_id = ${org_id}
        ;"
    echo ''
    echo [Get the funnel_status FROM ${project_name}.funnel]
    echo $sql_14
    mysql --login-path=$dest_login_path -e "$sql_14" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt

    while read funnel_status; 
    do 
        export sql_15="
            UPDATE cdp_data_team.${project_name}_funnel
            SET status = $(echo ${funnel_status} | cut -d _ -f 3)
            WHERE org_id = $(echo ${funnel_status} | cut -d _ -f 1)
                and id = $(echo ${funnel_status} | cut -d _ -f 2)        
            ;"
        echo ''
        echo [UPDATE cdp_data_team.funnel at CDP on funnel_id = $(echo ${funnel_status} | cut -d _ -f 2)]
        echo $sql_15
        mysql --login-path=${src_login_path}_dev -e "$sql_15" 2>>$error_dir/$src_login_path/$project_name/cdp_data_team.funnel.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt


    echo ''
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp]   
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo [the shell end at `date`]
