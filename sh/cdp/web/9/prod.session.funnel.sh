#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
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


#### loop by org_id ####
export sql_0="
    select org_id
    from cdp_organization.organization_domain
    where domain_type = 'web'
    group by org_id
    limit 2, 2
    ;"    
echo ''
echo [Get the org_id]
#mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_9.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_9.error
#sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_9.txt


while read org_id; 
do 
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
    echo $sql_1
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error

    export sql_2=" 
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ${org_id} funnel_id, 
		layer_id,
		count(*) user, 
                now() created_at, 
                now() updated_at
            from (
                select
                    domain, 
                    fpc, 
                    session, 
                    ifnull(layer_id, 1) layer_id, 
                    row_number () over (partition by domain, fpc, session order by layer_id) rid
                from ${project_name}.${type}_both_${org_id}_etl a
                    left join
                    (
                    select *
                    from codebook_cdp.funnel_config
                    where org_id = ${org_id} 
                        and funnel_id = ${org_id}
                        and logic = 'event'
                    ) b
                    on a.event_type = b.logic_content
                group by 
                    domain, 
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
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error

    export sql_3="    
        delete
        from ${project_name}.${type}_${table_name}_${org_id}_src
        where start_date < '${vDate}' - interval 89 day
            and span = 'daily'
        ; 

        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                '90 days' span, 
                '${vDate}' - interval 89 day start_date, 
                '${vDate}' end_date, 
                ${org_id} funnel_id, 
                layer_id, 
                sum(user) user, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}_src
            where span = 'daily'
                and funnel_id = ${org_id}
	    group by layer_id
        ; 
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_src AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error 

    export sql_4="   
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
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error

    export sql_5="
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
                    and funnel_id = ${org_id}
                ) c
                on a.funnel_id = c.funnel_id 
                    and a.layer_id = c.layer_id
	; 
	ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
	; 

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
                    and funnel_id = ${org_id}
                ) a
            )
        WHERE tag_date = '${vDate}' + interval 1 day
            and funnel_id = ${org_id}
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}.error

    ###########################################
    #### the customized funnel layer model ####
    ###########################################

    export sql_9="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_temp (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code',
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
    mysql --login-path=$dest_login_path -e "$sql_9" 


    #### loop by funnel_id ####
    export sql_10="
        select funnel_id
        from codebook_cdp.funnel_config
        where org_id = ${org_id}
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
            from codebook_cdp.funnel_config
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
                        domain, 
                        fpc, 
                        session, 
                        ${funnel_id} funnel_id,
                        $(echo ${logic_content} | cut -d _ -f 1) layer_id, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_both_${org_id}_etl
                    where behavior = 'page_view' 
                        and page_url REGEXP '$(echo ${logic_content} | cut -d _ -f 2)'
                    group by 
                        domain, 
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
                    domain, 
                    fpc, 
                    session, 
                    ${funnel_id} funnel_id,
                    1 layer_id, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_both_${org_id}_etl
                where behavior = 'page_view' 
                    and page_url NOT REGEXP 
                        (
                        select group_concat(logic_content separator '|') logic_content
                        from codebook_cdp.funnel_config
                        where org_id = ${org_id}
                            and funnel_id = ${funnel_id}
                            and layer_id >= 2
                            and logic = 'page_url'                    
                        )
                group by 
                    domain, 
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
                from codebook_cdp.funnel_config
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
                            a.domain, 
                            a.fpc,
	                    ifnull(session, concat(date_format('${vDate}', '%Y%m%d'), '01')) session,
                            ${funnel_id} funnel_id, 
                            if($(echo ${ev_layer} | cut -d _ -f 2) like '%$(echo ${ev_layer} | cut -d _ -f 3)%', 
                                ${layer_id}, 1) layer_id,  
                            now() created_at, 
                            now() updated_at
                        from (
                            select 
                                ifnull(g.fpc, e.fpc) fpc, 
                                e.domain, 
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
                                e.created_at
                            from ${project_name}.${type}_event_${org_id}_src e
                                left join uuid.cdp_fpc_mapping g
                                    on e.fpc = g.origin_fpc 
                                        and g.domain = substring_index(e.domain, '/', 1)
                            where type = $(echo ${ev_layer} | cut -d _ -f 1)
                            ) a
                            
                            left join
                            (
                            select fpc, domain, created_at, session
                            from ${project_name}.${type}_both_${org_id}_etl
                            where behavior = 'event'
                                and event_type = $(echo ${ev_layer} | cut -d _ -f 1)
                            ) b
                            on a.fpc = b.fpc
                                and a.domain = b.domain
                                and a.created_at = b.created_at                    
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
                    a.domain, 
                    a.fpc, 
                    ifnull(session, concat(date_format('${vDate}', '%Y%m%d'), '01')) session,
                    ${funnel_id} funnel_id, 
                    1 layer_id,  
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        ifnull(g.fpc, e.fpc) fpc, 
                        e.domain, 
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
                        e.created_at
                    from ${project_name}.${type}_event_${org_id}_src e
                        left join uuid.cdp_fpc_mapping g
                            on e.fpc = g.origin_fpc 
                                and g.domain = substring_index(e.domain, '/', 1)
                    where type not in 
                        (         
                        select group_concat(distinct logic_content)
                        from codebook_cdp.funnel_config
                        where org_id = ${org_id}
                            and funnel_id = ${funnel_id}
                            and logic = 'event'
                        ) 
                    ) a
                    
                    left join
                    (
                    select fpc, domain, created_at, session
                    from ${project_name}.${type}_both_${org_id}_etl
                    where behavior = 'event'
                        and event_type not in
                            (
                            select group_concat(distinct logic_content)
                            from codebook_cdp.funnel_config
                            where org_id = ${org_id}
                                and funnel_id = ${funnel_id}
                                and logic = 'event'
                            )
                    ) b
                    on a.fpc = b.fpc
                        and a.domain = b.domain
                        and a.created_at = b.created_at   
            ;"
        echo ''
        echo $sql_16
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_temp on others]
        mysql --login-path=$dest_login_path -e "$sql_16" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_temp.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_id.txt


    export sql_17="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src
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
                    fpc, 
                    session, 
                    funnel_id, 
                    layer_id,
                    row_number () over (partition by domain, fpc, session, funnel_id order by layer_id) rid
                from ${project_name}.${type}_${table_name}_${org_id}_temp
                group by
                    domain, 
                    fpc, 
                    session, 
                    funnel_id, 
                    layer_id
                order by 
                    domain, 
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
        ;
        DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp
        ;"
    echo ''
    echo $sql_17
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src]
    echo [DROP TABLE ${project_name}.${type}_${table_name}_${org_id}_temp]    
    mysql --login-path=$dest_login_path -e "$sql_17" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_src.error


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
    mysql --login-path=$dest_login_path -e "$sql_18" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type}_${table_name}_${org_id}_src.error 

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
                from codebook_cdp.funnel_config
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
        WHERE a.channel = 'web'
            and a.status <> 1
        ;"
    echo ''
    echo [UPDATE the funnel status at codebook_cdp.funnel]
    #mysql --login-path=$dest_login_path -e "$sql_20" 2>>$error_dir/$src_login_path/$project_name/codebook_cdp.funnel.error


    export sql_21="
        select concat_ws('_', org_id, id, status) funnel_status
        from codebook_cdp.funnel
        where org_id = ${org_id}
        ;"
    echo ''
    echo [Get the funnel_status FROM codebook_cdp.funnel]
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
        #mysql --login-path=cdp_master -e "$sql_22" 2>>$error_dir/$src_login_path/$project_name/cdp_data_team.funnel.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_9.txt
#done < /root/datapool/export_file/cdp/web/web.cdp_org_id_9.txt
echo ''
echo 'end: ' `date`

