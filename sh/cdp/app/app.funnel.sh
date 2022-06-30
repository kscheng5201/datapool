#!/usr/bin/bash
export dest_login_path="datapool"
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
    export delete_old="
        DELETE 
        FROM ${project_name}.${type}_${table_name}_${org_id}_src
        WHERE funnel_id in 
            (
            select id
            from codebook_cdp.funnel
            where org_id = ${org_id}
                and status = 0
                and channel = '${project_name}'
            )
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;
    
        DELETE 
        FROM ${project_name}.${type}_${table_name}_${org_id}
        WHERE funnel_id in 
            (
            select id
            from codebook_cdp.funnel
            where org_id = ${org_id}
                and status = 0
                and channel = '${project_name}'
            )
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"    
    echo ''
    echo [DELETE the funnel which was resetting]
    echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id}_src]
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
        ;" 
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error

    export sql_2=" 
        select concat_ws('-', funnel_id, logic, ifnull(logic_content, '')) layer1
        from codebook_cdp.funnel_config
        where org_id = ${org_id}
            and channel = '${project_name}'
            and funnel_name = '消費'
            and layer_id = 1
        ;"
    echo ''
    echo [Get the layer1]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt

    while read layer1; 
    do 
        if [ $(echo ${layer1} | cut -d - -f 2) = page_url ]; 
        then 
            export sql_3="
                INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'daily' span, 
                        '${vDate}' start_date, 
                        '${vDate}' end_date, 
                        $(echo ${layer1} | cut -d - -f 1) funnel_id, 
                        1 layer_id,
                        count(*) user, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select token, session
                        from ${project_name}.${type}_event_${org_id}_etl            
                        group by token, session
                        ) a      
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                ;
                
                INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'daily' span, 
                        '${vDate}' start_date, 
                        '${vDate}' end_date, 
                        $(echo ${layer1} | cut -d - -f 1) funnel_id, 
                        layer_id,
                        count(*) user, 
                        now() created_at, 
                        now() updated_at
                    from (
                        select
                            domain, 
                            token, 
                            session, 
                            layer_id, 
                            row_number () over (partition by domain, token, session order by layer_id) + 1 rid
                        from ${project_name}.${type}_event_${org_id}_etl a
                            left join
                            (
                            select *
                            from codebook_cdp.funnel_config
                            where org_id = ${org_id} 
                                and channel = '${project_name}'
                                and funnel_id = $(echo ${layer1} | cut -d - -f 1)
                                and logic = 'event'
                            ) b
                            on a.type = b.logic_content
                        
                        where layer_id is not null
                        group by 
                            domain, 
                            token, 
                            session, 
                            layer_id                    
                        ) c
                    where layer_id = rid
                        # 如果不設定這個 WHERE 條件，數字會好看許多，但不會是同一個人走過所有歷程
                    group by layer_id
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                ;"
            echo ''
            echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
            echo $sql_3
            mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error           

        else
            echo ''
            echo [codebook_cdp.funnel_config where org_id = ${org_id} and and channel = '${project_name}' and funnel_id = $(echo ${layer1} | cut -d - -f 1) 是預設漏斗]
        fi
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
    
    
    export sql_4="    
        DELETE
        from ${project_name}.${type}_${table_name}_${org_id}_src
        where start_date < '${vDate}' - interval 89 day
            and span = 'daily'
	    and funnel_id = $(echo ${layer1} | cut -d - -f 1)
        ; 

        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                $(echo ${layer1} | cut -d - -f 1) funnel_id, 
                layer_id, 
                sum(user) user, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_src
            where span = 'daily'
                and funnel_id = $(echo ${layer1} | cut -d - -f 1)
            group by layer_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error 

    export sql_5="   
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
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error

    export sql_6="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
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
                ifnull(100 * round(a.user / ifnull(b.user, a.user), 3), 0) conversion_rate, 
                null conversion_overall, 
                now() created_at, 
                now() updated_at
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_src
                where tag_date = '${vDate}' + interval 1 day
                    and span = '90 days'
                    and funnel_id = $(echo ${layer1} | cut -d - -f 1)
                ) a
            
                left join 
                (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}_src
                where tag_date = '${vDate}' + interval 1 day
                    and span = '90 days'
                    and funnel_id = $(echo ${layer1} | cut -d - -f 1)
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
                    and funnel_id = $(echo ${layer1} | cut -d - -f 1)
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
                max(if(layer_id = 1, user, null)) layer_min, 
                max(if(layer_id = 5, user, null)) layer_max
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
            group by tag_date, funnel_id
            ) b
            on a.tag_date = b.tag_date
                and a.funnel_id = b.funnel_id
        SET conversion_overall = 100 * ifnull(round(layer_max / layer_min, 5), 0)
        WHERE a.funnel_id = $(echo ${layer1} | cut -d - -f 1)
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error

    ###########################################
    #### the customized funnel layer model ####
    ###########################################
    
    export sql_9="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            token varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
            session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
            funnel_id int NOT NULL COMMENT '行銷漏斗的編號',  
            layer_id int NOT NULL COMMENT '行銷漏斗的階層, 最低1 最高10', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            key idx_tag_date (tag_date), 
            key idx_domain (funnel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='客戶自行設定條件的行銷漏斗 level 暫存展開表(long)'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp]
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_temp.error

    #### loop by funnel_id ####
    export sql_10="
        select funnel_id
        from codebook_cdp.funnel_config
        where org_id = ${org_id}
            and channel = '${project_name}'
        group by funnel_id
        order by funnel_id
        ;"    
    echo ''
    echo [Get the funnel_id]
    echo $sql_10
    mysql --login-path=$dest_login_path -e "$sql_10" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt

    while read funnel_id; 
    do 
        export sql_11="
            select concat(logic, if(logic_content is null or logic_content in ('', 'NULL'), '', logic_content)) layer1
            from codebook_cdp.funnel_config
            where org_id = ${org_id}
                and funnel_id = ${funnel_id}
                and channel = '${project_name}'
                and layer_id = 1
            ;"
        echo ''
        echo [Get the layer1]
        echo $sql_11
        mysql --login-path=$dest_login_path -e "$sql_11" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
	cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
	echo ${layer1}


        for layer_id in $(seq 7); 
            # 每個客戶最多 10 個漏斗，最多 7 層，每層最多有 10 個條件
        do
            export sql_12="
                select concat_ws('-', ifnull(logic_content, ''), ifnull(ev_function, ''), ifnull(attribute, '')) ev_layer
                from codebook_cdp.funnel_config
                where org_id = ${org_id}
                    and funnel_id = ${funnel_id}
                    and logic = 'event'
                    and layer_id = ${layer_id}
                ;"
            echo ''
            echo $sql_12
            echo [Get the event_layer with the function and attribute]
            mysql --login-path=$dest_login_path -e "$sql_12" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.error
            sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt
	    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt

            while read layer1; 
            do 
                while read ev_layer; 
                do 
                    if [ ${layer1} = page_url ]; 
                    then 
                        export sql_13="
                            INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src
                                select 
                                    null serial, 
                                    '${vDate}' + interval 1 day tag_date, 
                                    'daily' span, 
                                    '${vDate}' start_date, 
                                    '${vDate}' end_date, 
                                    ${funnel_id} funnel_id, 
                                    1 layer_id,
                                    count(*) user, 
                                    now() created_at, 
                                    now() updated_at
                                from (
                                    select token, session
                                    from ${project_name}.${type}_event_${org_id}_etl            
                                    group by token, session
                                    ) a      
                            ;
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                            ;"
                        echo ''
                        echo [假如第一層是全流量（沒有條件）]
                        echo [寫入第一層資料]
                        echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
                        echo $sql_13
                        mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error
        
                        export sql_14="    
                            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp    
                                select 
                                    null serial, 
                                    '${vDate}' + interval 1 day tag_date, 
                                    a.domain, 
                                    a.token, 
                                    session, 
                                    ${funnel_id} funnel_id, 
                                    if($(echo ${ev_layer} | cut -d - -f 2) = '', ${layer_id}, null) layer_id,
                                    now() created_at, 
                                    now() updated_at
                                from (
                                    select 
                                        token, 
                                        domain, 
                                        type, 
                                        event, 
                                        col1, 
                                        col2, 
                                        col3, 
                                        col4, 
                                        col5, 
                                        col6, 
                                        col7, 
                                        col8, 
                                        col9, 
                                        col10, 
                                        col11,
                                        col12,
                                        col13,
                                        col14,
                                        created_at
                                    from ${project_name}.${type}_event_${org_id}_src
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) a
                                    
                                    inner join
                                    (
                                    select token, domain, session
                                    from ${project_name}.${type}_event_${org_id}_etl
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) b
                                    on a.token = b.token
                                        and a.domain = b.domain                                  
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_temp AUTO_INCREMENT = 1
                            ;"
                        echo ''
                        echo [寫入第二層以後的資料: event only]
                        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp]
                        echo $sql_14
                        mysql --login-path=$dest_login_path -e "$sql_14" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp.error

                        export sql_15="    
                            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp    
                                select 
                                    null serial, 
                                    '${vDate}' + interval 1 day tag_date, 
                                    a.domain, 
                                    a.token, 
                                    session, 
                                    ${funnel_id} funnel_id, 
                                    case when $(echo ${ev_layer} | cut -d - -f 2) REGEXP '$(echo ${ev_layer} | cut -d - -f 3)' then ${layer_id} else null end layer_id,
                                    now() created_at, 
                                    now() updated_at
                                from (
                                    select 
                                        token, 
                                        domain, 
                                        type, 
                                        event, 
                                        col1, 
                                        col2, 
                                        col3, 
                                        col4, 
                                        col5, 
                                        col6, 
                                        col7, 
                                        col8, 
                                        col9, 
                                        col10, 
                                        col11,
                                        col12,
                                        col13,
                                        col14,
                                        created_at
                                    from ${project_name}.${type}_event_${org_id}_src
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) a
                                    
                                    inner join
                                    (
                                    select token, domain, session
                                    from ${project_name}.${type}_event_${org_id}_etl
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) b
                                    on a.token = b.token
                                        and a.domain = b.domain                                  
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_temp AUTO_INCREMENT = 1
                            ;"
                        echo ''
                        echo [寫入第二層以後的資料: event + attribute + content]
                        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp]
                        echo $sql_15
                        mysql --login-path=$dest_login_path -e "$sql_15" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp.error

                        export sql_16="    
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
                                        domain, 
                                        token, 
                                        session, 
                                        funnel_id, 
                                        layer_id,
                                        row_number () over (partition by domain, token, session, funnel_id order by layer_id) rid
                                    from ${project_name}.${type}_${table_name}_${org_id}_temp
                                    group by
                                        domain, 
                                        token, 
                                        session, 
                                        funnel_id, 
                                        layer_id
                                    order by 
                                        domain, 
                                        token, 
                                        session, 
                                        funnel_id, 
                                        layer_id
                                    ) a
                                where layer_id = rid
                                group by funnel_id, layer_id
                                order by funnel_id, layer_id
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                            ;

                            DELETE
                            from ${project_name}.${type}_${table_name}_${org_id}_src
                            where start_date < '${vDate}' - interval 89 day
                                and span = 'daily'
                                and funnel_id = ${funnel_id}
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
                                    and funnel_id = ${funnel_id}
                            group by funnel_id, layer_id
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                            ;

                            #DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp                            
                            #;"
                        echo ''
                        echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
                        echo $sql_16
                        mysql --login-path=$dest_login_path -e "$sql_16" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error
    
                    else
                        export sql_14="    
                            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp    
                                select 
                                    null serial, 
                                    '${vDate}' + interval 1 day tag_date, 
                                    a.domain, 
                                    a.token, 
                                    session, 
                                    ${funnel_id} funnel_id, 
                                    if($(echo ${ev_layer} | cut -d - -f 2) = '', ${layer_id}, null) layer_id,
                                    now() created_at, 
                                    now() updated_at
                                from (
                                    select 
                                        token, 
                                        domain, 
                                        type, 
                                        event, 
                                        col1, 
                                        col2, 
                                        col3, 
                                        col4, 
                                        col5, 
                                        col6, 
                                        col7, 
                                        col8, 
                                        col9, 
                                        col10, 
                                        col11,
                                        col12,
                                        col13,
                                        col14,
                                        created_at
                                    from ${project_name}.${type}_event_${org_id}_src
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) a
                                    
                                    inner join
                                    (
                                    select token, domain, session
                                    from ${project_name}.${type}_event_${org_id}_etl
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) b
                                    on a.token = b.token
                                        and a.domain = b.domain                                  
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_temp AUTO_INCREMENT = 1
                            ;"
                        echo ''
                        echo [寫入每一層的資料: event only]
                        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp]
                        echo $sql_14
                        mysql --login-path=$dest_login_path -e "$sql_14" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp.error

                        export sql_15="    
                            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp    
                                select 
                                    null serial, 
                                    '${vDate}' + interval 1 day tag_date, 
                                    a.domain, 
                                    a.token, 
                                    session, 
                                    ${funnel_id} funnel_id, 
                                    case when $(echo ${ev_layer} | cut -d - -f 2) REGEXP '$(echo ${ev_layer} | cut -d - -f 3)' then ${layer_id} else null end layer_id,
                                    now() created_at, 
                                    now() updated_at
                                from (
                                    select 
                                        token, 
                                        domain, 
                                        type, 
                                        event, 
                                        col1, 
                                        col2, 
                                        col3, 
                                        col4, 
                                        col5, 
                                        col6, 
                                        col7, 
                                        col8, 
                                        col9, 
                                        col10, 
                                        col11,
                                        col12,
                                        col13,
                                        col14,
                                        created_at
                                    from ${project_name}.${type}_event_${org_id}_src
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) a
                                    
                                    inner join
                                    (
                                    select token, domain, session
                                    from ${project_name}.${type}_event_${org_id}_etl
                                    where type = $(echo ${ev_layer} | cut -d - -f 1)
                                    ) b
                                    on a.token = b.token
                                        and a.domain = b.domain                                  
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_temp AUTO_INCREMENT = 1
                            ;"
                        echo ''
                        echo [寫入每一層的資料: event + attribute + content]
                        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp]
                        echo $sql_15
                        mysql --login-path=$dest_login_path -e "$sql_15" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_temp.error

                        export sql_16="    
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
                                        domain, 
                                        token, 
                                        session, 
                                        funnel_id, 
                                        layer_id,
                                        row_number () over (partition by domain, token, session, funnel_id order by layer_id) rid
                                    from ${project_name}.${type}_${table_name}_${org_id}_temp
                                    group by
                                        domain, 
                                        token, 
                                        session, 
                                        funnel_id, 
                                        layer_id
                                    order by 
                                        domain, 
                                        token, 
                                        session, 
                                        funnel_id, 
                                        layer_id
                                    ) a
                                where layer_id = rid
                                group by funnel_id, layer_id
                                order by funnel_id, layer_id
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                            ;

                            DELETE
                            from ${project_name}.${type}_${table_name}_${org_id}_src
                            where start_date < '${vDate}' - interval 89 day
                                and span = 'daily'
                                and funnel_id = ${funnel_id}
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
                                and funnel_id = ${funnel_id}
                            group by funnel_id, layer_id
                            ; 
                            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
                            ;

                            #DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp                            
                            #;"
                        echo ''
                        echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_src]
                        echo $sql_16
                        mysql --login-path=$dest_login_path -e "$sql_16" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error
    
                    fi
                done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_ev_layer.txt     
            done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_layer1.txt
        done
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt
    
    export sql_19="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
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
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_19
    mysql --login-path=$dest_login_path -e "$sql_19" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error


    export sql_20="
        UPDATE codebook_cdp.funnel a
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
        WHERE a.channel = '${project_name}'
            and a.status <> 1
        ;"
    echo ''
    echo [UPDATE the funnel status at codebook_cdp.funnel]
    mysql --login-path=$dest_login_path -e "$sql_20" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.funnel.error


    export sql_21="
        select concat_ws('-', org_id, id, status) funnel_status
        from codebook_cdp.funnel
        where org_id = ${org_id}
            and channel = '${project_name}'
        ;"
    echo ''
    echo [Get the funnel_status FROM codebook_cdp.funnel]
    mysql --login-path=$dest_login_path -e "$sql_21" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt


    while read funnel_status; 
    do 
        export sql_22="
            UPDATE cdp_data_team.funnel
            SET status = $(echo ${funnel_status} | cut -d - -f 3)
            WHERE org_id = $(echo ${funnel_status} | cut -d - -f 1)
                and id = $(echo ${funnel_status} | cut -d - -f 2)
                and channel = '${project_name}' 
            ;"
        echo ''
        echo [UPDATE cdp_data_team.funnel at CDP prod]
        #mysql --login-path=cdp_dev -e "$sql_22" 2>>$error_dir/$src_login_path/$project_name/cdp_data_team.funnel.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt


    mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp;"

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`
