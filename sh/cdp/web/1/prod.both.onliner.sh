#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export type_s="session"
export type_p="person"
export table_name="onliner" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### Get DateName ####
if [ -n "$1" ]; 
then
    vDateName=`date -d $1 '+%a'`
else
    vDateName=`date -d "1 day ago" '+%a'`
fi

#### Get First and Last Date of Month ####
if [ -n "$1" ]; 
then 
    vMonthFirst=`date -d $1 +"%Y-%m-01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y-%m-01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi

#### Get Last Date of Season ####
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
#mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_1.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.error
#sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_1.txt


while read org_id; 
do 
    export sql_1="   
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'weekly/monthly/seasonal',
	    yearweek int COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'weekly 實際的 yearweek', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            dow varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Day of Week: Mon to Sun', 
            hour varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '時間格式 00:00-23:00', 
            meridiem varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '十二小時制,分別為上午（拉丁文 Ante Meridiem 表示中午之前）和下午(拉丁文 Post Meridiem 表示中午之後)',
            session int DEFAULT NULL COMMENT '流量數目',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain, dow, hour),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_domain (domain), 
            key idx_dow (dow), 
            key idx_hour (hour)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【長期統計】熱門時段(流量)'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type_p}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'weekly/monthly/seasonal',
 	    yearweek int COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'weekly 實際的 yearweek',
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            dow varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Day of Week: Mon to Sun', 
            hour varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '時間格式 00:00-23:00', 
            meridiem varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '十二小時制,分別為上午（拉丁文 Ante Meridiem 表示中午之前）和下午(拉丁文 Post Meridiem 表示中午之後)',
            user int DEFAULT NULL COMMENT '不重複用戶的上線人數',
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain, dow, hour),
            key idx_tag_date (tag_date), 
            key idx_span (span), 
            key idx_domain (domain), 
            key idx_dow (dow), 
            key idx_hour (hour)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【長期統計】熱門時段(人數)'
        ;"
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${type_s}_${table_name}_${org_id}_src.error

    export sql_2=" 
        select domain
        from cdp_organization.organization_domain
        where domain_type = 'web'
            and org_id = ${org_id}
        ;"
    echo ''
    echo $sql_2
    echo [Get the domain of the organization ${org_id}]
    mysql --login-path=$src_login_path -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_domain.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_domain.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_domain.txt


    while read domain; 
    do
        export sql_3="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date,
		    'weekly' span, 
                    yearweek('${vDate}', 7) yearweek, 
                    '${vDate}' start_date,     
                    '${vDate}' end_date,  
                    '${domain}' domain, 
                    c.dow, 
                    c.hour, 
                    meridiem,
                    ifnull(session, 0) session, 
        		    null time_flag,
                    now() created_at, 
                    now() updated_at   
                from (
                    select 
                        domain, 
                        dow, 
                        hour, 
                        count(*) session
                    from (
                        select 
                            fpc, 
                            left(dayname(created_at), 3) dow, 
                            date_format(min(created_at), '%H:00') hour,
                            '${domain}' domain, 
                            session
                        from ${project_name}.${type_s}_both_${org_id}_etl
                        where domain = '${domain}'
                        group by 
                            fpc, 
                            left(dayname(created_at), 3), 
                            domain, 
                            session
                        ) a
                        
                    group by 
                        domain, 
                        dow,
                        hour
                     ) b
                    
                    right join 
                    (
                    select dow, hour, meridiem
                    from codebook_cdp.onliner_prototype
                    where dow = '${vDateName}'
                    ) c
                    on b.dow = c.dow and b.hour = c.hour
                    
                group by 
                    c.dow, 
                    c.hour
            ;
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;
            
            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date,
		    'weekly' span, 
                    yearweek('${vDate}', 7) yearweek, 
                    '${vDate}' start_date,     
                    '${vDate}' end_date,  
                    '${domain}' domain, 
                    c.dow, 
                    c.hour, 
                    meridiem,
                    ifnull(user, 0) user, 
        		    null time_flag,
                    now() created_at, 
                    now() updated_at   
                from (
                    select 
                        domain, 
                        dow, 
                        hour, 
                        count(distinct fpc) user
                    from (
                        select 
                            fpc, 
                            left(dayname(created_at), 3) dow, 
                            date_format(min(created_at), '%H:00') hour,
                            '${domain}' domain, 
                            session
                        from ${project_name}.${type_s}_both_${org_id}_etl
                        where domain = '${domain}'
                        group by 
                            fpc, 
                            left(dayname(created_at), 3), 
                            domain, 
                            session
                        ) a
                        
                    group by 
                        domain, 
                        dow,
                        hour
                     ) b
                    
                    right join 
                    (
                    select dow, hour, meridiem
                    from codebook_cdp.onliner_prototype
                    where dow = '${vDateName}'
                    ) c
                    on b.dow = c.dow and b.hour = c.hour
                    
                group by 
                    c.dow, 
                    c.hour
            ;
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo $sql_3
        echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}]
        echo [INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}]        
        mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_domain.txt


    export sql_4="
        INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date,
                'weekly' span, 
                yearweek('${vDate}', 7) yearweek,  
                '${vDate}' start_date,     
                '${vDate}' end_date,  
                'ALL' domain, 
                c.dow, 
                c.hour, 
                meridiem,
                ifnull(session, 0) session, 
                null time_flag,
                now() created_at, 
                now() updated_at   
            from (
                select 
                    dow, 
                    hour, 
                    count(*) session
                from (
                    select 
                        fpc, 
                        left(dayname(created_at), 3) dow, 
                        date_format(min(created_at), '%H:00') hour,
                        session
                    from ${project_name}.${type_s}_both_${org_id}_etl
                    group by 
                        fpc, 
                        left(dayname(created_at), 3), 
                        session
                    ) a
                    
                group by 
                    dow,
                    hour
                 ) b
                
                right join 
                (
                select dow, hour, meridiem
                from codebook_cdp.onliner_prototype 
                where dow = '${vDateName}'
                ) c
                on b.dow = c.dow and b.hour = c.hour
                
            group by 
                c.dow, 
                c.hour
        ;
        ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1      
        ;
        
        INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'weekly' span, 
                yearweek('${vDate}', 7) yearweek, 
                '${vDate}' start_date,     
                '${vDate}' end_date,  
                'ALL' domain, 
                c.dow, 
                c.hour, 
                meridiem,
                ifnull(user, 0) user, 
                null time_flag,
                now() created_at, 
                now() updated_at   
            from (
                select 
                    dow, 
                    hour, 
                    count(distinct fpc) user
                from (
                    select 
                        fpc, 
                        left(dayname(created_at), 3) dow, 
                        date_format(min(created_at), '%H:00') hour,
                        session
                    from ${project_name}.${type_s}_both_${org_id}_etl
                    group by 
                        fpc, 
                        left(dayname(created_at), 3), 
                        session
                    ) a
                    
                group by 
                    dow,
                    hour
                 ) b
                
                right join 
                (
                select dow, hour, meridiem
                from codebook_cdp.onliner_prototype
                where dow = '${vDateName}'
                ) c
                on b.dow = c.dow and b.hour = c.hour
                
            group by 
                c.dow, 
                c.hour
        ;
        ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1      
        ;"        
    echo ''
    # echo $sql_5
    echo [INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id} on domain #]
    echo [INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id} on domain #]    
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error


    if [ ${vDateName} = Sun ]; 
    then 
        export sql_5="
            UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
            SET time_flag = if(yearweek = yearweek('${vDate}', 7), 'last', null)
            WHERE span not in ('monthly', 'seasonal')        
            ;
            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(yearweek = yearweek('${vDate}', 7), 'last', null)
            WHERE span not in ('monthly', 'seasonal')        
            ;"            
        echo ''
        echo [UPDATE the time_flag on week: ${project_name}.${type_s}_${table_name}_${org_id}]
        echo [UPDATE the time_flag on week: ${project_name}.${type_p}_${table_name}_${org_id}]        
        mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error
    else 
        echo [today is ${vDateName}, Not Sun! No need to mark the new week.]
    fi


    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_6="
            INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span,
		    null yearweek,  
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    a.dow, 
                    hour,
                    meridiem,
                    ifnull(round(session / freq), 0) session,
                    null time_flag,  
                    now() created_at, 
                    now() updated_at
                from (   
                    select 
                        domain, 
                        dow, 
                        hour, 
                        meridiem, 
                        sum(session) session
                    from ${project_name}.${type_s}_${table_name}_${org_id}
                    where start_date >= date_format('${vDate}', '%Y-%m-01')
                        and start_date < '${vDate}' + interval 1 day
                        and span not in ('monthly', 'seasonal')
                    group by 
                        domain, 
                        dow, 
                        hour, 
                        meridiem
                    ) a
                    
                    left join
                    (
                    select dow, count(*) freq
                    from (
                        select start_date, left(dayname(start_date), 3) dow
                        from ${project_name}.${type_s}_${table_name}_${org_id}
                        where start_date >= date_format('${vDate}', '%Y-%m-01')
                            and start_date < '${vDate}' + interval 1 day 
                        group by start_date, left(dayname(start_date), 3)
                        ) b
                        
                    group by dow
                    ) c
                    on a.dow = c.dow
            ; 
            ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;

            INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span,
		    null yearweek,  
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    a.dow, 
                    hour,
                    meridiem,
                    ifnull(round(user / freq), 0) user,
                    null time_flag,  
                    now() created_at, 
                    now() updated_at
                from (   
                    select 
                        domain, 
                        dow, 
                        hour, 
                        meridiem, 
                        sum(user) user
                    from ${project_name}.${type_p}_${table_name}_${org_id}
                    where start_date >= date_format('${vDate}', '%Y-%m-01')
                        and start_date < '${vDate}' + interval 1 day
                        and span not in ('monthly', 'seasonal')
                    group by 
                        domain, 
                        dow, 
                        hour, 
                        meridiem
                    ) a
                    
                    left join
                    (
                    select dow, count(*) freq
                    from (
                        select start_date, left(dayname(start_date), 3) dow
                        from ${project_name}.${type_s}_${table_name}_${org_id}
                        where start_date >= date_format('${vDate}', '%Y-%m-01')
                            and start_date < '${vDate}' + interval 1 day 
                        group by start_date, left(dayname(start_date), 3)
                        ) b
                        
                    group by dow
                    ) c
                    on a.dow = c.dow
            ; 
            ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;

            UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;
            UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        # echo $sql_6
        echo [start: date on ${vDate}. ${vDate} = ${vMonthLast}?]
        echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
        echo [UPDATE the time_flag on month: ${project_name}.${type_s}_${table_name}_${org_id}]
        echo [insert into ${project_name}.${type_p}_${table_name}_${org_id}]
        echo [UPDATE the time_flag on month: ${project_name}.${type_p}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    
    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi     
    
    
    for seasonDate in $seasonEnd
    do
        if [ ${vDate} = ${seasonDate} ];
        then 
            export sql_7="
                INSERT INTO ${project_name}.${type_s}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span,
			null yearweek,  
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,
			domain, 
                        a.dow, 
                        hour,
                        meridiem,
                        ifnull(round(session / freq), 0) session,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (   
                        select 
                            domain, 
                            dow, 
                            hour, 
                            meridiem, 
                            sum(session) session
                        from ${project_name}.${type_s}_${table_name}_${org_id}
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and span not in ('monthly', 'seasonal')
                        group by 
                            domain, 
                            dow, 
                            hour, 
                            meridiem
                        ) a
                        
                        left join
                        (
                        select dow, count(*) freq
                        from (
                            select start_date, left(dayname(start_date), 3) dow
                            from ${project_name}.${type_s}_${table_name}_${org_id}
                            where start_date >= '${vDate}' + interval 1 day - interval 3 month
                                and start_date < '${vDate}' + interval 1 day
                            group by start_date, left(dayname(start_date), 3)
                            ) b
                            
                        group by dow
                        ) c
                        on a.dow = c.dow
                ; 
                ALTER TABLE ${project_name}.${type_s}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;

                INSERT INTO ${project_name}.${type_p}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span,
			null yearweek,  
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date,
			domain, 
                        a.dow, 
                        hour,
                        meridiem,
                        ifnull(round(user / freq), 0) user,
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from (   
                        select 
                            domain, 
                            dow, 
                            hour, 
                            meridiem, 
                            sum(user) user
                        from ${project_name}.${type_p}_${table_name}_${org_id}
                        where start_date >= '${vDate}' + interval 1 day - interval 3 month
                            and start_date < '${vDate}' + interval 1 day
                            and span not in ('monthly', 'seasonal')
                        group by 
                            domain, 
                            dow, 
                            hour, 
                            meridiem
                        ) a
                        
                        left join
                        (
                        select dow, count(*) freq
                        from (
                            select start_date, left(dayname(start_date), 3) dow
                            from ${project_name}.${type_s}_${table_name}_${org_id}
                            where start_date >= '${vDate}' + interval 1 day - interval 3 month
                                and start_date < '${vDate}' + interval 1 day
                            group by start_date, left(dayname(start_date), 3)
                            ) b
                            
                        group by dow
                        ) c
                        on a.dow = c.dow
                ; 
                ALTER TABLE ${project_name}.${type_p}_${table_name}_${org_id} AUTO_INCREMENT = 1
                ;


                UPDATE ${project_name}.${type_s}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;
                UPDATE ${project_name}.${type_p}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            # echo $sql_6
            echo [start: date on ${vDate}. ${vDate} = ${seasonDate}?]
            echo [insert into ${project_name}.${type_s}_${table_name}_${org_id}]
            echo [UPDATE the time_flag on season: ${project_name}.${type_s}_${table_name}_${org_id}]
            echo [insert into ${project_name}.${type_p}_${table_name}_${org_id}]
            echo [UPDATE the time_flag on season: ${project_name}.${type_p}_${table_name}_${org_id}]
            mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type_s}_${table_name}_${org_id}.error    
        else 
            echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
        fi 
    done 

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id_1.txt
#done < /root/datapool/export_file/cdp/web/web.cdp_org_id_1.txt
echo ''
echo [end the ${vDate} job at `date`]
