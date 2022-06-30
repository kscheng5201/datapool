#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web_event"
export type="session"
export table_name="kpi" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### Get DateName (Mon to Sun)
if [ -n "$1" ]; 
then
    vDateName=`date -d $1 '+%a'`
else
    vDateName=`date -d "1 day ago" '+%a'`
fi

#### Get the First and Last Date of Month ####
if [ -n "$1" ]; 
then 
    vMonthFirst=`date -d $1 +"%Y%m01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y%m%d"`

else 
    vMonthFirst=`date -d "1 day ago" +"%Y%m01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y%m%d"`
fi

#### Get the Last Date of Season ####
seasonEnd="
`date +"%Y-03-31"`
`date +"%Y-06-30"`
`date +"%Y-09-30"`
`date +"%Y-12-31"`
"


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
mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt


while read org_id; 
do 
    export sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            session int NOT NULL DEFAULT 0 COMMENT '工作階段數量',  
            user int NOT NULL DEFAULT 0 COMMENT '不重複上線用戶', 
            page_view int NOT NULL DEFAULT 0 COMMENT '頁面瀏覽次數',   
            event int NOT NULL DEFAULT 0 COMMENT '事件觸發次數',
            stay_interval int NOT NULL DEFAULT 0 COMMENT '平均停留秒數', 
            invalid_session int NOT NULL DEFAULT 0 COMMENT '僅動作一次的工作階段數量',
            bounce_rate int NOT NULL DEFAULT 0 COMMENT '跳出率(%)', 
            session_gr int NOT NULL DEFAULT 0 COMMENT '工作階段數量, 相較上期之成長率(%)', 
            user_gr int NOT NULL DEFAULT 0 COMMENT '不重複上線用戶, 相較上期之成長率(%)', 
            page_view_gr int NOT NULL DEFAULT 0 COMMENT '頁面瀏覽次數, 相較上期之成長率(%)', 
            event_gr int NOT NULL DEFAULT 0 COMMENT '事件觸發次數, 相較上期之成長率(%)', 
            stay_interval_gr int NOT NULL DEFAULT 0 COMMENT '平均停留秒數, 相較上期之成長率(%)', 
            bounce_rate_gr int NOT NULL DEFAULT 0 COMMENT '跳出率(%), 相較上期之成長率(%)',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain),
            key idx_tag_date (tag_date), 
	    key idx_start_date (start_date),
            key idx_domain (domain)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='kpi 統計表'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_fpc (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, fpc),
            key idx_tag_date (tag_date),
	    key idx_start_date (start_date), 
            key idx_domain (domain), 
            key idx_fpc (fpc)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每日 unique fpc 原始表'
	;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_graph (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            session int NOT NULL DEFAULT 0 COMMENT '工作階段數量',  
            user int NOT NULL DEFAULT 0 COMMENT '不重複上線用戶', 
            page_view int NOT NULL DEFAULT 0 COMMENT '頁面瀏覽次數',   
            event int NOT NULL DEFAULT 0 COMMENT '事件觸發次數',
            stay_interval int NOT NULL DEFAULT 0 COMMENT '平均停留秒數', 
            invalid_session int NOT NULL DEFAULT 0 COMMENT '僅動作一次的工作階段數量',
            bounce_rate int NOT NULL DEFAULT 0 COMMENT '跳出率(%)', 
            session_gr int NOT NULL DEFAULT 0 COMMENT '工作階段數量, 相較上期之成長率(%)', 
            user_gr int NOT NULL DEFAULT 0 COMMENT '不重複上線用戶, 相較上期之成長率(%)', 
            page_view_gr int NOT NULL DEFAULT 0 COMMENT '頁面瀏覽次數, 相較上期之成長率(%)', 
            event_gr int NOT NULL DEFAULT 0 COMMENT '事件觸發次數, 相較上期之成長率(%)', 
            stay_interval_gr int NOT NULL DEFAULT 0 COMMENT '平均停留秒數, 相較上期之成長率(%)', 
            bounce_rate_gr int NOT NULL DEFAULT 0 COMMENT '跳出率(%), 相較上期之成長率(%)',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),
            key idx_domain (domain)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='kpi 統計表'
        ;"

    export sql_2="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ifnull(domain, '#') domain, 
                sum(session) session, 
                sum(user) user, 
                sum(page_view) page_view, 
                sum(event) event, 
                sum(stay_interval) stay_interval, 
                sum(invalid_session) invalid_session, 
                100 * round(sum(invalid_session) / sum(session), 2) bounce_rate,
                null session_gr, 
                null user_gr, 
                null page_view_gr, 
                null event_gr, 
                null stay_interval_gr, 
                null bounce_rate_gr, 
                now() created_at,
                now() updated_at
            from (            
                select 
                    a.domain, 
                    session, 
                    user, 
                    page_view, 
                    event, 
                    stay_interval, 
                    invalid_session
                from (
                    select 
                        domain, 
                        count(distinct fpc) user, 
                        sum(if(behavior = 'page_view', 1, 0)) page_view, 
                        sum(if(behavior = 'event', 1, 0)) event   
                    from ${project_name}.${type}_both_${org_id}_etl
                    group by domain
                    ) a
                
                    inner join
                    (
                    select 
                        domain, 
                        count(*) session, 
                        round(avg(stay_interval)) stay_interval
                    from (
                        select  
                            domain, 
                            fpc, 
                            session, 
                            timestampdiff(second, min(created_at), max(created_at)) stay_interval
                        from ${project_name}.${type}_both_${org_id}_etl
                        group by 
                            domain, 
                            fpc, 
                            session
                        ) c
                        
                    group by domain
                    ) b
                    on a.domain = b.domain

                    inner join 
                    (
                    select 
                        domain, 
                        count(*) invalid_session
                    from (
                        select 
                            domain, 
                            fpc, 
                            session, 
                            count(*)
                        from ${project_name}.${type}_both_${org_id}_etl
                        group by 
                            domain, 
                            fpc, 
                            session
                        having count(*) = 1
                        ) e
                        
                    group by domain
                    ) d
                    on a.domain = d.domain
                
                ) f
            group by domain with rollup
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;


        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id} 
            (tag_date, span, start_date, end_date, domain)
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', b.domain
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id}
                where tag_date = '${vDate}'
                    and span = 'daily'
                    and domain <> '#'
                ) a
                
                right join
                (
                select *
                from codebook_cdp.organization_domain
                where org_id = ${org_id}
                    and domain_type = 'web'
                ) b
                on a.domain = b.domain
            where a.domain is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_fpc
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                domain, 
       		fpc,
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_both_${org_id}_etl
            group by 
                domain, 
                fpc
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_fpc AUTO_INCREMENT = 1
    	;"
    #### Export Data ####
    echo ''
    # echo $sql_1
    echo [start: date on ${vDate}]
    echo [create table if not exists ${project_name}.${type}_${table_name}_${org_id}_etl]
    mysql --login-path=$dest_login_path -e "$sql_1"

    #### Export Data ####
    echo ''
    # echo $sql_2
    echo [start: date on ${vDate}]
    echo [insert into ${project_name}.${type}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error


    export sql_3="
        UPDATE ${project_name}.${type}_${table_name}_${org_id} a
            INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
            ON a.tag_date = b.tag_date + interval 1 day
                and a.span = b.span
                and a.domain = b.domain
                and a.span = 'daily'
        SET a.session_gr = 100 * round((a.session - b.session) / b.session, 2), 
            a.user_gr = 100 * round((a.user - b.user) / b.user, 2), 
            a.page_view_gr = 100 * round((a.page_view - b.page_view) / b.page_view, 2),  
            a.event_gr = 100 * round((a.event - b.event) / b.event, 2),  
            a.stay_interval_gr = 100 * round((a.stay_interval - b.stay_interval) / b.stay_interval, 2),  
            a.bounce_rate_gr = 100 * round((a.bounce_rate - b.bounce_rate) / b.bounce_rate, 2) 	
        where a.tag_date = '${vDate}' + interval 1 day
        ;"
    echo ''
    echo $sql_3
    echo [start: date on ${vDate}]
    echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error


    if [ ${vDateName} = Sun ];
    then 
        export sql_4="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    sum(session) session, 
                    sum(user) user, 
                    sum(page_view) page_view, 
                    sum(event) event, 
                    sum(stay_interval) stay_interval, 
                    sum(invalid_session) invalid_session, 
                    100 * round(sum(invalid_session) / sum(session), 2) bounce_rate,
                    null session_gr, 
                    null user_gr, 
                    null page_view_gr, 
                    null event_gr, 
                    null stay_interval_gr, 
                    null bounce_rate_gr, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day 
                    and span = 'daily'
                group by domain
	    ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1 
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, domain, count(distinct fpc) user
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by domain
                ) b
                ON a.tag_date = b.tag_date and a.domain = b.domain
            SET a.user = b.user
            WHERE a.span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, count(distinct fpc) user
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                ) b
                ON a.tag_date = b.tag_date 
            SET a.user = b.user
            WHERE a.span = 'weekly'
                and a.domain = '#'
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
                ON a.tag_date = b.tag_date + interval 7 day
                    and a.span = b.span
                    and a.domain = b.domain
                    and a.span = 'weekly'
            SET a.session_gr = 100 * round((a.session - b.session) / b.session, 2), 
                a.user_gr = 100 * round((a.user - b.user) / b.user, 2), 
                a.page_view_gr = 100 * round((a.page_view - b.page_view) / b.page_view, 2),  
                a.event_gr = 100 * round((a.event - b.event) / b.event, 2),  
                a.stay_interval_gr = 100 * round((a.stay_interval - b.stay_interval) / b.stay_interval, 2),  
                a.bounce_rate_gr = 100 * round((a.bounce_rate - b.bounce_rate) / b.bounce_rate, 2) 	
            where a.tag_date = '${vDate}' + interval 1 day
	    ; 


            DELETE 
            FROM ${project_name}.${type}_${table_name}_${org_id}_graph
            WHERE span = 'weekly'
            ; 
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph
                select 
                    null serial, 
                    tag_date, 
                    'weekly' span, 
                    start_date, 
                    end_date, 
                    domain, 
                    session, 
                    user, 
                    page_view, 
                    event, 
                    stay_interval, 
                    invalid_session, 
                    bounce_rate, 
                    session_gr, 
                    user_gr, 
                    page_view_gr, 
                    event_gr, 
                    stay_interval_gr, 
                    bounce_rate_gr,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') - interval 7 day
                    and start_date < '${vDate}' + interval 1 day 
                    and span = 'daily'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph
            SET time_flag = if(start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), 'last', 'last_last')
            WHERE span = 'weekly'
            ;"
        echo ''
        echo $sql_4
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    

    else 
        echo [today is ${vDateName}, not Sun. No Need to do the weekly statistics.]
    fi 
  
    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_5="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    sum(session) session, 
                    sum(user) user, 
                    sum(page_view) page_view, 
                    sum(event) event, 
                    sum(stay_interval) stay_interval, 
                    sum(invalid_session) invalid_session, 
                    100 * round(sum(invalid_session) / sum(session), 2) bounce_rate,
                    null session_gr, 
                    null user_gr, 
                    null page_view_gr, 
                    null event_gr, 
                    null stay_interval_gr, 
                    null bounce_rate_gr, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                group by domain
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1 
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, domain, count(distinct fpc) user
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                group by domain
                ) b
                ON a.tag_date = b.tag_date and a.domain = b.domain
            SET a.user = b.user
            WHERE a.span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' tag_date, count(distinct fpc) user
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                ) b
                ON a.tag_date = b.tag_date 
            SET a.user = b.user
            WHERE a.span = 'monthly'
                and a.domain = '#'
            ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
                ON a.tag_date = b.tag_date + interval 1 month
                    and a.span = b.span
                    and a.domain = b.domain
                    and a.span = 'monthly'
            SET a.session_gr = 100 * round((a.session - b.session) / b.session, 2), 
                a.user_gr = 100 * round((a.user - b.user) / b.user, 2), 
                a.page_view_gr = 100 * round((a.page_view - b.page_view) / b.page_view, 2),  
                a.event_gr = 100 * round((a.event - b.event) / b.event, 2),  
                a.stay_interval_gr = 100 * round((a.stay_interval - b.stay_interval) / b.stay_interval, 2),  
                a.bounce_rate_gr = 100 * round((a.bounce_rate - b.bounce_rate) / b.bounce_rate, 2) 	
            where a.tag_date = '${vDate}' + interval 1 day
	    ;


            
            DELETE 
            FROM ${project_name}.${type}_${table_name}_${org_id}_graph
            WHERE span = 'monthly'
            ;
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph
                select 
                    null serial, 
                    tag_date, 
                    'monthly' span, 
                    start_date, 
                    end_date, 
                    domain, 
                    session, 
                    user, 
                    page_view, 
                    event, 
                    stay_interval, 
                    invalid_session, 
                    bounce_rate, 
                    session_gr, 
                    user_gr, 
                    page_view_gr, 
                    event_gr, 
                    stay_interval_gr, 
                    bounce_rate_gr,
                    null time_flag, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where start_date >= date_format('${vDate}', '%Y-%m-01') - interval 1 month
                    and start_date < '${vDate}' + interval 1 day 
                    and span = 'daily'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph
            SET time_flag = if(start_date >= date_format('${vDate}', '%Y-%m-01'), 'last', 'last_last')
            WHERE span = 'monthly'
            ;"
        echo ''
        # echo $sql_5
        echo [start: date on ${vDate}; ${vDate} = ${vMonthLast}?]
        echo [insert into ${project_name}.${type}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    

    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi 
    
    for seasonDate in $seasonEnd
    do
        if [ ${vDate} = ${seasonDate} ];
        then 
            export sql_6="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' - interval 3 month + interval 1 day start_date, 
                        '${vDate}' end_date, 
                        domain, 
                        sum(session) session, 
                        sum(user) user, 
                        sum(page_view) page_view, 
                        sum(event) event, 
                        sum(stay_interval) stay_interval, 
                        sum(invalid_session) invalid_session, 
                        100 * round(sum(invalid_session) / sum(session), 2) bounce_rate,
                        null session_gr, 
                        null user_gr, 
                        null page_view_gr, 
                        null event_gr, 
                        null stay_interval_gr, 
                        null bounce_rate_gr, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by domain
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1 
                ;

                UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                    INNER JOIN
                    (
                    select '${vDate}' + interval 1 day tag_date, domain, count(distinct fpc) user
                    from ${project_name}.${type}_${table_name}_${org_id}_fpc
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                    group by domain
                    ) b
                    ON a.tag_date = b.tag_date and a.domain = b.domain
                SET a.user = b.user
                WHERE a.span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                    INNER JOIN
                    (
                    select '${vDate}' + interval 1 day tag_date, count(distinct fpc) user
                    from ${project_name}.${type}_${table_name}_${org_id}_fpc
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                    ) b
                    ON a.tag_date = b.tag_date 
                SET a.user = b.user
                WHERE a.span = 'seasonal'
                    and a.domain = '#'
                ;

                UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                    INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
                    ON a.tag_date = b.tag_date + interval 1 day + interval 3 month - interval 1 day
                        and a.span = b.span
                        and a.domain = b.domain
                        and a.span = 'seasonal'
                SET a.session_gr = 100 * round((a.session - b.session) / b.session, 2), 
                    a.user_gr = 100 * round((a.user - b.user) / b.user, 2), 
                    a.page_view_gr = 100 * round((a.page_view - b.page_view) / b.page_view, 2),  
                    a.event_gr = 100 * round((a.event - b.event) / b.event, 2),  
                    a.stay_interval_gr = 100 * round((a.stay_interval - b.stay_interval) / b.stay_interval, 2),  
                    a.bounce_rate_gr = 100 * round((a.bounce_rate - b.bounce_rate) / b.bounce_rate, 2) 					
                where a.tag_date = '${vDate}' + interval 1 day
       		;

                
                DELETE 
                FROM ${project_name}.${type}_${table_name}_${org_id}_graph
                WHERE span = 'seasonal'
                ;
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph
                    select 
                        null serial, 
                        tag_date, 
                        'seasonal' span, 
                        start_date, 
                        end_date, 
                        domain, 
                        session, 
                        user, 
                        page_view, 
                        event, 
                        stay_interval, 
                        invalid_session, 
                        bounce_rate, 
                        session_gr, 
                        user_gr, 
                        page_view_gr, 
                        event_gr, 
                        stay_interval_gr, 
                        bounce_rate_gr,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}
                    where start_date >= '${vDate}' - interval 3*2 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day 
                        and span = 'daily'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph
                SET time_flag = if(start_date >= '${vDate}' - interval 3 month + interval 1 day, 'last', 'last_last')
                WHERE span = 'seasonal'
                ; 


                delete 
                from ${project_name}.${type}_${table_name}_${org_id}_fpc
                where start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                ; 
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_fpc AUTO_INCREMENT = 1
                ;"
            echo ''
            # echo $sql_6
            echo [start: date on ${vDate}; ${vDate} = ${seasonDate}?]
            echo [insert into ${project_name}.${type}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    
        else 
            echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
        fi 
    done 
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo 'end: ' `date`
