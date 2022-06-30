#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="line"
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
    vMonthFirst=`date -d $1 +"%Y%m01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y%m01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi

#### Get Last Date of Season ####
seasonEnd="
`date +"%Y-03-31"`
`date +"%Y-06-30"`
`date +"%Y-09-30"`
`date +"%Y-12-31"`
"


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
            user int DEFAULT 0 NULL COMMENT '不重複上線用戶', 
            reg int DEFAULT 0 NULL COMMENT '新用戶註冊數',
            user_gr int DEFAULT NULL COMMENT '不重複上線用戶, 相較上期之成長率(%)',
    	    reg_gr int DEFAULT NULL COMMENT '新用戶註冊數, 相較上期之成長率(%)', 
            time_flag varchar(16) DEFAULT NULL COMMENT 'last: 上期; last_last: 上上期', 
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, domain),
            key idx_tag_date (tag_date), 
    	    key idx_start_date (start_date),
            key idx_domain (domain)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【指標統計】上線用戶 & 會員新增人數'
        ;
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_${project_name} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain', 
            line_unique_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'line_unique 的 id',
            created_at timestamp NOT NULL DEFAULT current_timestamp COMMENT '建立時間', 
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, domain, line_unique_id),
            key idx_tag_date (tag_date),
            key idx_start_date (start_date), 
            key idx_domain (domain), 
            key idx_line_unique_id (line_unique_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每日 unique fpc 原始表'
    	;"
    echo ''
    echo [start: date on ${vDate}]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_${project_name}]
    echo ''
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1"

    export sql_2="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${org_id}
            and domain_type = '${project_name}'
    	;"
    echo ''
    echo [Get the db_id on web]
    mysql --login-path=$src_login_path -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt

    while read db_id; 
    do 
        export sql_3="
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                ifnull(domain, 'ALL') domain,
                count(distinct ${project_name}_unique_id) user,
                count(*) reg, 
                null user_gr, 
                null reg_gr, 
                null time_flag,
                now() created_at, 
                now() updated_at
            from cdp_${project_name}_${db_id}.${project_name}_event_raw_data
            where created_at >= unix_timestamp('${vDate}')
                and created_at < unix_timestamp('${vDate}' + interval 1 day)
                and type = 10
            group by domain
                with rollup
            ;"
        echo ''
        echo [start: date on ${vDate}]
        echo [Export Data to ${project_name}.${table_name}_${org_id}_${db_id}.txt]
        echo ''
        echo $sql_3        
        mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.error

        echo ''
        echo [start: date on ${vDate}]
        echo [Import Data from ${project_name}.${table_name}_${org_id}_${db_id}.txt to ${project_name}.${type}_${table_name}_${org_id}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}.error 


        export sql_4="
            SET NAMES utf8mb4
            ;
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                domain,
                ${project_name}_unique_id,
                now() created_at, 
                now() updated_at
            from cdp_${project_name}_${db_id}.${project_name}_event_raw_data
            where created_at >= unix_timestamp('${vDate}')
                and created_at < unix_timestamp('${vDate}' + interval 1 day)
                and type = 10
            ;"
        echo ''
        echo [start: date on ${vDate}]
        echo [Export Data to ${project_name}.${table_name}_${org_id}_${db_id}_unique_id.txt]
        echo ''
        echo $sql_4        
        mysql --login-path=$src_login_path -e "$sql_4" > $export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_unique_id.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_unique_id.error

        echo ''
        echo [start: date on ${vDate}]
        echo [Import Data from ${project_name}.${table_name}_${org_id}_${db_id}_unique_id.txt to ${project_name}.${type}_${table_name}_${org_id}_${project_name}]
        mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.${table_name}_${org_id}_${db_id}_unique_id.txt' INTO TABLE ${project_name}.${type}_${table_name}_${org_id}_${project_name} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_${db_id}_${project_name}.error 

    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_db_id.txt


    export sql_5="
        INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id} (tag_date, span, start_date, end_date, domain)    
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', domain
            from codebook_cdp.organization_domain
            where org_id = ${org_id}
                and domain_type = '${project_name}'

            UNION ALL
            
            select '${vDate}' + interval 1 day, 'daily', '${vDate}', '${vDate}', 'ALL'
        ;"
    echo ''
    echo [start: date on ${vDate}]
    echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}]
    echo ''
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error


    if [ ${vDateName} = Sun ];
    then 
        export sql_6="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'weekly' span, 
                    STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    null user,
                    sum(reg) reg, 
                    null user_gr,
      		    null reg_gr,  
       		    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day 
                    and span = 'daily'
                group by domain
    	    ;            
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1 
            ;"
        echo ''
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
        echo ''
        echo $sql_6
        mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    

        export sql_7="
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, domain, count(distinct ${project_name}_unique_id) user
                from ${project_name}.${type}_${table_name}_${org_id}_${project_name}
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                group by domain
                ) b
                ON a.tag_date = b.tag_date and a.domain = b.domain
            SET a.user = ifnull(b.user, 0)
            WHERE a.span = 'weekly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, domain, count(distinct ${project_name}_unique_id) user
                from ${project_name}.${type}_${table_name}_${org_id}_${project_name}
                where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W')
                    and start_date < '${vDate}' + interval 1 day
                ) b
                ON a.tag_date = b.tag_date 
            SET a.user = ifnull(b.user, 0)
            WHERE a.span = 'weekly'
                and a.domain = 'ALL'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET user = 0
            WHERE span = 'weekly'
                and tag_date = '${vDate}' + interval 1 day
                and user is null
	    ;

            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
                ON a.tag_date = b.tag_date + interval 7 day
                    and a.span = b.span
                    and a.domain = b.domain
                    and a.span = 'weekly'
            SET a.user_gr = ifnull(100 * round((a.user - b.user) / b.user, 2), 999999), 
            	a.reg_gr = ifnull(100 * round((a.reg - b.reg) / b.reg, 2), 999999)  
            WHERE a.tag_date = '${vDate}' + interval 1 day
            ; 
            
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'weekly'
            ;"
        echo ''
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on domain]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on domain ALL]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on growth]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on time_flag]
        echo $sql_7
        mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    
    else 
        echo [today is ${vDateName}, not Sun. No Need to do the weekly statistics.]
    fi 
  
    if [ ${vDate} = ${vMonthLast} ];
    then 
        export sql_8="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'monthly' span, 
                    date_format('${vDate}', '%Y-%m-01') start_date, 
                    '${vDate}' end_date, 
                    domain, 
                    null user,
                    sum(reg) reg, 
                    null user_gr,
       		    null reg_gr,  
       		    null time_flag,
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                    and span = 'daily'
                group by domain
    	    ;            
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1 
            ;"
        echo ''
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
        echo ''
        echo $sql_8
        mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    

        export sql_9="
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, domain, count(distinct ${project_name}_unique_id) user
                from ${project_name}.${type}_${table_name}_${org_id}_${project_name}
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                group by domain
                ) b
                ON a.tag_date = b.tag_date and a.domain = b.domain
            SET a.user = ifnull(b.user, 0)
            WHERE a.span = 'monthly'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN
                (
                select '${vDate}' + interval 1 day tag_date, domain, count(distinct ${project_name}_unique_id) user
                from ${project_name}.${type}_${table_name}_${org_id}_${project_name}
                where start_date >= date_format('${vDate}', '%Y-%m-01')
                    and start_date < '${vDate}' + interval 1 day
                ) b
                ON a.tag_date = b.tag_date 
            SET a.user = ifnull(b.user, 0)
            WHERE a.span = 'monthly'
                and a.domain = 'ALL'
            ;
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET user = 0
            WHERE span = 'monthly'
		and tag_date = '${vDate}' + interval 1 day
                and user is null
            ; 

            UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
                ON a.tag_date = b.tag_date + interval 1 month
                    and a.span = b.span
                    and a.domain = b.domain
                    and a.span = 'monthly'
            SET a.user_gr = ifnull(100 * round((a.user - b.user) / b.user, 2), 999999), 
            	a.reg_gr = ifnull(100 * round((a.reg - b.reg) / b.reg, 2), 999999)  
            WHERE a.tag_date = '${vDate}' + interval 1 day
            ; 
            
            UPDATE ${project_name}.${type}_${table_name}_${org_id}
            SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
            WHERE span = 'monthly'
            ;"
        echo ''
        echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on domain]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on domain ALL]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on growth]
        echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on time_flag]
        echo $sql_9
        mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    

    else 
        echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
    fi 
    
    for seasonDate in $seasonEnd
    do
        if [ ${vDate} = ${seasonDate} ];
        then 
            export sql_10="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'seasonal' span, 
                        '${vDate}' + interval 1 day - interval 3 month start_date, 
                        '${vDate}' end_date, 
                        domain, 
                        null user,
                        sum(reg) reg, 
                        null user_gr,
                        null reg_gr,  
                        null time_flag,
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                        and span = 'monthly'
                    group by domain
                ;            
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1 
                ;"
            echo ''
            echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}]
            echo ''
            echo $sql_10
            mysql --login-path=$dest_login_path -e "$sql_10" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    
    
            export sql_11="
                UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                    INNER JOIN
                    (
                    select '${vDate}' + interval 1 day tag_date, domain, count(distinct ${project_name}_unique_id) user
                    from ${project_name}.${type}_${table_name}_${org_id}_${project_name}
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                    group by domain
                    ) b
                    ON a.tag_date = b.tag_date and a.domain = b.domain
                SET a.user = ifnull(b.user, 0)
                WHERE a.span = 'seasonal'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                    INNER JOIN
                    (
                    select '${vDate}' + interval 1 day tag_date, domain, count(distinct ${project_name}_unique_id) user
                    from ${project_name}.${type}_${table_name}_${org_id}_${project_name}
                    where start_date >= '${vDate}' - interval 3 month + interval 1 day
                        and start_date < '${vDate}' + interval 1 day
                    ) b
                    ON a.tag_date = b.tag_date 
                SET a.user = ifnull(b.user, 0)
                WHERE a.span = 'seasonal'
                    and a.domain = 'ALL'
                ;
                UPDATE ${project_name}.${type}_${table_name}_${org_id}
                SET user = 0
                WHERE span = 'seasonal'
                    and tag_date = '${vDate}' + interval 1 day
                    and user is null
                ;

                UPDATE ${project_name}.${type}_${table_name}_${org_id} a
                    INNER JOIN ${project_name}.${type}_${table_name}_${org_id} b
                    ON a.tag_date = b.tag_date + interval 3 month
                        and a.span = b.span
                        and a.domain = b.domain
                        and a.span = 'seasonal'
                SET a.user_gr = ifnull(100 * round((a.user - b.user) / b.user, 2), 999999), 
                    a.reg_gr = ifnull(100 * round((a.reg - b.reg) / b.reg, 2), 999999)  
                WHERE a.tag_date = '${vDate}' + interval 1 day
                ; 
                
                UPDATE ${project_name}.${type}_${table_name}_${org_id}
                SET time_flag = if(tag_date = '${vDate}' + interval 1 day, 'last', null)
                WHERE span = 'seasonal'
                ;"
            echo ''
            echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on domain]
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on domain ALL]
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on growth]
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id} on time_flag]
            echo $sql_11
            mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    

        else 
            echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
        fi 
    done 
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`
