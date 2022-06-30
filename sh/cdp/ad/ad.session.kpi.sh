#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 活動期間統計
# Author: Benson Cheng
# Created_at: 2021-12-21
# Updated_at: 2021-12-30
####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export type="session"
export table_name="kpi" 
export src_login_path="cdp"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
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
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y%m%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y%m01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y%m%d"`
fi

#### Get Last Date of Season ####
seasonEnd="
`date +"%Y0331"`
`date +"%Y0630"`
`date +"%Y0930"`
`date +"%Y1231"`
`date -d "$(date +%Y-01-01) -1 day" +"%Y-%m-%d"`
"


while read org_id; 
do 
    export sql_1="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id} (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/ALL', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            user_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '用戶類型：全部用戶(ALL); 新用戶(new); 舊用戶(old); 重複進站用戶(repeated)',            
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            click int NOT NULL DEFAULT '0' COMMENT '點擊次數：有點到 utm/campaign 的 page_url 紀錄',
            user int NOT NULL DEFAULT '0' COMMENT '進站人數：每天每人點到 utm/campaign 的次數',
            new_user int NOT NULL DEFAULT '0' COMMENT '新用戶：今天新出現又有點過 utm（不一定是第一筆紀錄）的不重複 fpc 數量',
            new_prop int NOT NULL DEFAULT '0' COMMENT '新用戶佔全部用戶的比例',
            stay_time int NOT NULL DEFAULT 0 COMMENT '有效工作階段平均停留秒數',
            valid int NOT NULL DEFAULT 0 COMMENT '有效工作階段數量',
            invalid int NOT NULL DEFAULT 0 NULL COMMENT '無效工作階段（僅動作一次的工作階段）數量',
            bounce_rate int NOT NULL DEFAULT 0 COMMENT '跳出率(%)',
            purchased int NOT NULL DEFAULT 0 COMMENT '完成購買次數（event = 14）',
            purchased_rate float NOT NULL DEFAULT 0 COMMENT '完成購買次數（event = 14）／有效流量數',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, campaign_id, utm_id, user_type),
            key idx_tag_date (tag_date), 
            key idx_span (span),  
            key idx_start_date (start_date), 
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id),
            key idx_user_type (user_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【指標統計】'
        ;"       
    echo ''
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}]
    echo $sql_1    
    mysql --login-path=$dest_login_path -e "$sql_1"
    
    export sql_2="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date,
                '${vDate}' end_date, 
                'ALL' user_type,
                a.campaign_id, 
                a.utm_id, 
                click,
                user, 
                new_user, 
                new_prop,
                ifnull(stay_time, 0) stay_time,
                ifnull(valid, 0) valid,
                ifnull(invalid, 0) invalid,
                ifnull(round(100 * ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0))), 0) bounce_rate, 
                ifnull(purchased, 0) purchased, 
                ifnull(round(100 * ifnull(purchased, 0) / ifnull(valid, 0)), 4) purchased_rate, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    utm_id,
                    sum(on_utm) click,     
                    count(distinct if(on_utm = 1, fpc, null)) user,
                    count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) new_user, 
                    round(100 * count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) / count(distinct if(on_utm = 1, fpc, null))) new_prop
                from ${project_name}.${type}_both_${org_id}_etl  
                group by 
                    campaign_id,
                    utm_id
                ) a
                
                left join
                (
                select 
                    campaign_id, 
                    utm_id, 
                    count(*) valid, 
                    round(avg(stay_time)) stay_time
                from (
                    select 
                        campaign_id, 
                        utm_id, 
                        fpc, 
                        session, 
                        count(*), 
                        timestampdiff(second, min(created_at), max(created_at)) stay_time
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'valid'
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl
                        where on_utm = 1
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id, fpc, session
                    ) bb
                group by campaign_id, utm_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.utm_id = b.utm_id
                
                left join
                (
                select campaign_id, utm_id, count(*) invalid
                from (
                    select campaign_id, utm_id, fpc, session, count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'invalid'
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl
                        where on_utm = 1
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id, fpc, session
                    having count(*) = 1
                    ) cc
                group by campaign_id, utm_id
                ) c
                on a.campaign_id = c.campaign_id
                    and a.utm_id = c.utm_id
            
                left join
                (
                select campaign_id, utm_id, count(*) purchased
                from ${project_name}.${type}_both_${org_id}_etl
                where event_type = 14
                    and (campaign_id, utm_id, fpc, session) in
                    (
                    select campaign_id, utm_id, fpc, session
                    from ${project_name}.${type}_both_${org_id}_etl
                    where on_utm = 1
                    group by campaign_id, utm_id, fpc, session
                    )
                group by campaign_id, utm_id
                having count(*) >= 1
                ) d
                on a.campaign_id = d.campaign_id
                    and a.utm_id = d.utm_id
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had record WHERE user_type = new]
    echo $sql_2
    mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql2.error

    export sql_3="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                'ALL' user_type,
                b.campaign_id, 
                b.utm_id, 
                0 click, 
                0 user, 
                0 new_user, 
                0 new_prop, 
                0 stay_time, 
                0 valid, 
                0 invalid, 
                0 bounce_rate, 
                0 purchased, 
                0 purchased_rate, 
                now() created_at, 
                now() updated_at
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id} 
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and user_type = 'ALL'
                ) a
                
                right join
                (
                select campaign_id, utm_id
                from ${project_name}.campaign_utm_${org_id}
                where utm_start <= '${vDate}'
                    and utm_end >= '${vDate}'
                group by campaign_id, utm_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.utm_id = b.utm_id
            where a.utm_id is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had no record WHERE user_type = new]
    echo $sql_3
    mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql3.error

    
    export sql_4="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date,
                '${vDate}' end_date, 
                'new' user_type,
                a.campaign_id, 
                a.utm_id, 
                click,
                user, 
                new_user, 
                new_prop,
                ifnull(stay_time, 0) stay_time,
                ifnull(valid, 0) valid,
                ifnull(invalid, 0) invalid,
                ifnull(round(100 * ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0))), 0) bounce_rate, 
                ifnull(purchased, 0) purchased, 
                ifnull(round(100 * ifnull(purchased, 0) / ifnull(valid, 0)), 4) purchased_rate, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    utm_id,
                    sum(on_utm) click,     
                    count(distinct if(on_utm = 1, fpc, null)) user,
                    count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) new_user, 
                    round(100 * count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) / count(distinct if(on_utm = 1, fpc, null))) new_prop
                from ${project_name}.${type}_both_${org_id}_etl  
                where is_new = 1
                group by 
                    campaign_id,
                    utm_id
                ) a
                
                left join
                (
                select 
                    campaign_id, 
                    utm_id, 
                    count(*) valid, 
                    round(avg(stay_time)) stay_time
                from (
                    select 
                        campaign_id, 
                        utm_id, 
                        fpc, 
                        session, 
                        count(*), 
                        timestampdiff(second, min(created_at), max(created_at)) stay_time
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'valid'
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl
                        where on_utm = 1
                            and is_new = 1
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id, fpc, session
                    ) bb
                group by campaign_id, utm_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.utm_id = b.utm_id
                
                left join
                (
                select campaign_id, utm_id, count(*) invalid
                from (
                    select campaign_id, utm_id, fpc, session, count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'invalid'
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl
                        where on_utm = 1
                            and is_new = 1
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id, fpc, session
                    ) cc
                group by campaign_id, utm_id
                ) c
                on a.campaign_id = c.campaign_id
                    and a.utm_id = c.utm_id
            
                left join
                (
                select campaign_id, utm_id, count(*) purchased
                from ${project_name}.${type}_both_${org_id}_etl
                where event_type = 14
                    and (campaign_id, utm_id, fpc, session) in
                    (
                    select campaign_id, utm_id, fpc, session
                    from ${project_name}.${type}_both_${org_id}_etl
                    where on_utm = 1
                        and is_new = 1
                    group by campaign_id, utm_id, fpc, session
                    )
                group by campaign_id, utm_id
                having count(*) >= 1
                ) d
                on a.campaign_id = d.campaign_id
                    and a.utm_id = d.utm_id
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had record WHERE user_type = new]
    echo $sql_4
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql4.error


    export sql_5="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                'new' user_type,
                b.campaign_id, 
                b.utm_id, 
                0 click, 
                0 user, 
                0 new_user, 
                0 new_prop, 
                0 stay_time, 
                0 valid, 
                0 invalid, 
                0 bounce_rate, 
                0 purchased, 
                0 purchased_rate, 
                now() created_at, 
                now() updated_at
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id} 
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and user_type = 'new'
                ) a
                
                right join
                (
                select campaign_id, utm_id
                from ${project_name}.campaign_utm_${org_id}
                where utm_start <= '${vDate}'
                    and utm_end >= '${vDate}'
                group by campaign_id, utm_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.utm_id = b.utm_id
            where a.utm_id is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had no record WHERE user_type = new]
    echo $sql_5
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql5.error

    export sql_6="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date,
                '${vDate}' end_date, 
                'old' user_type,
                a.campaign_id, 
                a.utm_id, 
                click,
                user, 
                new_user, 
                new_prop,
                ifnull(stay_time, 0) stay_time,
                ifnull(valid, 0) valid,
                ifnull(invalid, 0) invalid,
                ifnull(round(100 * ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0))), 0) bounce_rate, 
                ifnull(purchased, 0) purchased, 
                ifnull(round(100 * ifnull(purchased, 0) / ifnull(valid, 0)), 4) purchased_rate, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    campaign_id,
                    utm_id,
                    sum(on_utm) click,     
                    count(distinct if(on_utm = 1, fpc, null)) user,
                    count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) new_user, 
                    round(100 * count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) / count(distinct if(on_utm = 1, fpc, null))) new_prop
                from ${project_name}.${type}_both_${org_id}_etl  
                where is_new = 0
                group by 
                    campaign_id,
                    utm_id
                ) a
                
                left join
                (
                select 
                    campaign_id, 
                    utm_id, 
                    count(*) valid, 
                    round(avg(stay_time)) stay_time
                from (
                    select 
                        campaign_id, 
                        utm_id, 
                        fpc, 
                        session, 
                        count(*), 
                        timestampdiff(second, min(created_at), max(created_at)) stay_time
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'valid'
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl
                        where on_utm = 1
                            and is_new = 0
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id, fpc, session
                    ) bb
                group by campaign_id, utm_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.utm_id = b.utm_id
                
                left join
                (
                select campaign_id, utm_id, count(*) invalid
                from (
                    select campaign_id, utm_id, fpc, session, count(*)
                    from ${project_name}.${type}_both_${org_id}_etl
                    where session_type = 'invalid'
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl
                        where on_utm = 1
                            and is_new = 0
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id, fpc, session
                    ) cc
                group by campaign_id, utm_id
                ) c
                on a.campaign_id = c.campaign_id
                    and a.utm_id = c.utm_id
            
                left join
                (
                select campaign_id, utm_id, count(*) purchased
                from ${project_name}.${type}_both_${org_id}_etl
                where event_type = 14
                    and (campaign_id, utm_id, fpc, session) in
                    (
                    select campaign_id, utm_id, fpc, session
                    from ${project_name}.${type}_both_${org_id}_etl
                    where on_utm = 1
                        and is_new = 0
                    group by campaign_id, utm_id, fpc, session
                    )
                group by campaign_id, utm_id
                having count(*) >= 1
                ) d
                on a.campaign_id = d.campaign_id
                    and a.utm_id = d.utm_id
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had record WHERE user_type = new]
    echo $sql_6
    mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql6.error


    export sql_7="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date, 
                '${vDate}' end_date, 
                'old' user_type,
                b.campaign_id, 
                b.utm_id, 
                0 click, 
                0 user, 
                0 new_user, 
                0 new_prop, 
                0 stay_time, 
                0 valid, 
                0 invalid, 
                0 bounce_rate, 
                0 purchased, 
                0 purchased_rate, 
                now() created_at, 
                now() updated_at
            from (
                select *
                from ${project_name}.${type}_${table_name}_${org_id} 
                where tag_date = '${vDate}' + interval 1 day
                    and span = 'daily'
                    and user_type = 'old'
                ) a
                
                right join
                (
                select campaign_id, utm_id
                from ${project_name}.campaign_utm_${org_id}
                where utm_start <= '${vDate}'
                    and utm_end >= '${vDate}'
                group by campaign_id, utm_id
                ) b
                on a.campaign_id = b.campaign_id
                    and a.utm_id = b.utm_id
            where a.utm_id is null
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had no record WHERE user_type = new]
    echo $sql_7
    mysql --login-path=$dest_login_path -e "$sql_7" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql7.error

    export sql_8="
        INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
            select 
                null serial, 
                '${vDate}' + interval 1 day tag_date, 
                'daily' span, 
                '${vDate}' start_date,
                '${vDate}' end_date, 
                user_type, 
                campaign_id, 
                0 utm_id, 
                sum(click) click, 
                sum(user) user, 
                sum(new_user) new_user, 
                ifnull(round(100 * sum(new_user) / sum(user)), 0) new_prop, 
                ifnull(round(avg(if(stay_time >= 1, stay_time, null))), 0) stay_time, 
                sum(valid) valid, 
                sum(invalid) invalid, 
                ifnull(round(100 * sum(invalid) / (sum(valid) + sum(invalid))), 0) bounce_rate, 
                sum(purchased) purchased, 
                ifnull(round(100 * sum(purchased) / (sum(valid) + sum(invalid))), 4) purchased_rate, 
                now() created_at, 
                now() updated_at
            from ${project_name}.${type}_${table_name}_${org_id}
            where tag_date = '${vDate}' + interval 1 day
                and span = 'daily'
                and user_type in ('ALL', 'new', 'old')
            group by user_type, campaign_id        
        ;
        ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which is campaign_id in total]
    echo $sql_8
    mysql --login-path=$dest_login_path -e "$sql_8" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql8.error

    export sql_9="
        DELETE 
        FROM ${project_name}.${type}_${table_name}_${org_id}
        WHERE span = 'FULL'
            and end_date >= '${vDate}'
        ;"
    echo ''
    echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id} WHERE span is 'FULL' and end_date >= '${vDate}']
    echo $sql_9
    mysql --login-path=$dest_login_path -e "$sql_9" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql9.error
    
    
    while read utm_detail; 
    do 
        export sql_10="
            CREATE TABLE IF NOT EXISTS ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2) (
                serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
                campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
                utm_id int(11) signed NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
                fpc varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '瀏覽器指紋碼 fingerprint code', 
                domain varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '來源 domain',
                behavior varchar(16) NOT NULL COMMENT '網頁上的行為: page_view or event',
                traffic_type varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '流量: 直接流量、自然流量、廣告流量、其他流量',
                referrer varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '已分類 referrer',
                page_title varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '頁面標題', 
                event_type tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '事件功能代碼',
                on_utm tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此紀錄是否踩到 utm: 1-有, 0-無',
                is_new tinyint(2) signed NOT NULL DEFAULT '0' COMMENT '此 fpc 是否為 utm 開始後才出現的新用戶: 1-是, 0-否',
                session varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'session/工作階段',
                session_type varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in/valid',
                created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
                updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
                key idx_created_at (created_at), 
                key idx_fpc (fpc),             
                key idx_campaign_id (campaign_id),
                key idx_utm_id (utm_id), 
                key idx_session_type (session_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='網頁瀏覽與事件觸發的每日 etl 整合表(browser fpc)，有 session'
            ;"
        echo ''
        echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_src_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)]
        echo $sql_10
        mysql --login-path=$dest_login_path -e "$sql_10" 
        
        export sql_11="
            INSERT INTO ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                select 
                    null serial, 
                    campaign_id, 
                    utm_id, 
                    fpc,
                    a.domain, 
                    behavior, 
                    traffic_type, 
                    referrer, 
                    page_title, 
                    event_type, 
                    on_utm, 
                    is_new, 
                    session, 
                    session_type,
                    created_at, 
                    now() updated_at
                from ${project_name}.${type}_both_${org_id}_etl_log a
                where created_at >= '$(echo ${utm_detail} | cut -d _ -f 3)'
                    and created_at < '$(echo ${utm_detail} | cut -d _ -f 4)' + interval 1 day
                    and campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                    and utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                    and a.fpc in
                    (
                    select fpc
                    from (
                        select fpc, count(distinct session)
                        from ${project_name}.${type}_both_${org_id}_etl_log
                        where created_at >= '$(echo ${utm_detail} | cut -d _ -f 3)'
                            and created_at < '$(echo ${utm_detail} | cut -d _ -f 4)' + interval 1 day
                            and campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                            and utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                            and on_utm = 1
                        group by fpc
                        having count(distinct session) >= 2
                        ) a
                    )
            ;" 
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_src_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)]
        echo $sql_11
        mysql --login-path=$dest_login_path -e "$sql_11" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql11.error

        export sql_12="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    '$(echo ${utm_detail} | cut -d _ -f 3)' start_date,
                    '$(echo ${utm_detail} | cut -d _ -f 4)' end_date, 
                    user_type, 
                    campaign_id, 
                    utm_id, 
                    sum(click) click, 
                    sum(user) user, 
                    sum(new_user) new_user, 
                    ifnull(round(100 * sum(new_user) / sum(user)), 0) new_prop, 
                    ifnull(round(avg(if(stay_time >= 1, stay_time, null))), 0) stay_time, 
                    sum(valid) valid, 
                    sum(invalid) invalid, 
                    ifnull(round(100 * sum(invalid) / (sum(valid) + sum(invalid))), 0) bounce_rate, 
                    sum(purchased) purchased, 
                    ifnull(round(100 * sum(purchased) / (sum(valid) + sum(invalid))), 4) purchased_rate, 
                    now() created_at, 
                    now() updated_at
                from ${project_name}.${type}_${table_name}_${org_id}
                where campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                    and utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                    and span = 'daily'
                    and user_type in ('ALL', 'new', 'old')
                group by 
                    user_type, 
                    campaign_id, 
                    utm_id
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} WHERE span = 'FULL']
        echo $sql_12
        mysql --login-path=$dest_login_path -e "$sql_12" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql12.error
 

        export sql_13="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    '$(echo ${utm_detail} | cut -d _ -f 3)' start_date,
                    '$(echo ${utm_detail} | cut -d _ -f 4)' end_date, 
                    'repeated' user_type,
                    a.campaign_id, 
                    a.utm_id, 
                    click,
                    user, 
                    new_user, 
                    new_prop,
                    ifnull(stay_time, 0) stay_time,
                    ifnull(valid, 0) valid,
                    ifnull(invalid, 0) invalid,
                    ifnull(round(100 * ifnull(invalid, 0) / (ifnull(valid, 0) + ifnull(invalid, 0))), 0) bounce_rate, 
                    ifnull(purchased, 0) purchased, 
                    ifnull(round(100 * ifnull(purchased, 0) / ifnull(valid, 0)), 4) purchased_rate, 
                    now() created_at, 
                    now() updated_at
                from (
                    select 
                        campaign_id,
                        utm_id,
                        sum(on_utm) click,     
                        count(distinct if(on_utm = 1, fpc, null)) user,
                        count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) new_user, 
                        round(100 * count(distinct if(on_utm = 1 and is_new = 1, fpc, null)) / count(distinct if(on_utm = 1, fpc, null))) new_prop
                    from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)  
                    group by 
                        campaign_id,
                        utm_id
                    ) a
                    
                    left join
                    (
                    select 
                        campaign_id, 
                        utm_id, 
                        count(*) valid, 
                        round(avg(stay_time)) stay_time
                    from (
                        select 
                            campaign_id, 
                            utm_id, 
                            fpc, 
                            session, 
                            count(*), 
                            timestampdiff(second, min(created_at), max(created_at)) stay_time
                        from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                        where session_type = 'valid'
                            and (campaign_id, utm_id, fpc, session) in
                            (
                            select campaign_id, utm_id, fpc, session
                            from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                            where on_utm = 1
                            group by campaign_id, utm_id, fpc, session
                            )
                        group by campaign_id, utm_id, fpc, session
                        ) bb
                    group by campaign_id, utm_id
                    ) b
                    on a.campaign_id = b.campaign_id
                        and a.utm_id = b.utm_id
                    
                    left join
                    (
                    select campaign_id, utm_id, count(*) invalid
                    from (
                        select campaign_id, utm_id, fpc, session, count(*)
                        from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                        where session_type = 'invalid'
                            and (campaign_id, utm_id, fpc, session) in
                            (
                            select campaign_id, utm_id, fpc, session
                            from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                            where on_utm = 1
                            group by campaign_id, utm_id, fpc, session
                            )
                        group by campaign_id, utm_id, fpc, session
                        ) cc
                    group by campaign_id, utm_id
                    ) c
                    on a.campaign_id = c.campaign_id
                        and a.utm_id = c.utm_id
                
                    left join
                    (
                    select campaign_id, utm_id, count(*) purchased
                    from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                    where event_type = 14
                        and (campaign_id, utm_id, fpc, session) in
                        (
                        select campaign_id, utm_id, fpc, session
                        from ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)
                        where on_utm = 1
                        group by campaign_id, utm_id, fpc, session
                        )
                    group by campaign_id, utm_id
                    having count(*) >= 1
                    ) d
                    on a.campaign_id = d.campaign_id
                        and a.utm_id = d.utm_id
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had record FROM ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)  ]
        echo $sql_13
        mysql --login-path=$dest_login_path -e "$sql_13" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql13.error
    
        export sql_14="
            INSERT INTO ${project_name}.${type}_${table_name}_${org_id}
                select 
                    null serial, 
                    '${vDate}' + interval 1 day tag_date, 
                    'FULL' span, 
                    '$(echo ${utm_detail} | cut -d _ -f 3)' start_date, 
                    '$(echo ${utm_detail} | cut -d _ -f 4)' end_date, 
                    'repeated' user_type,
                    b.campaign_id, 
                    b.utm_id, 
                    0 click, 
                    0 user, 
                    0 new_user, 
                    0 new_prop, 
                    0 stay_time, 
                    0 valid, 
                    0 invalid, 
                    0 bounce_rate, 
                    0 purchased, 
                    0 purchased_rate, 
                    now() created_at, 
                    now() updated_at
                from (
                    select *
                    from ${project_name}.${type}_${table_name}_${org_id} 
                    where tag_date = '${vDate}' + interval 1 day
                        and span = 'FULL'
                        and user_type = 'repeated'
                        and campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                        and utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                    ) a
                    
                    right join
                    (
                    select campaign_id, utm_id
                    from ${project_name}.campaign_utm_${org_id}
                    where utm_start <= '${vDate}'
                        and utm_end >= '${vDate}'
                        and campaign_id = $(echo ${utm_detail} | cut -d _ -f 1)
                        and utm_id = $(echo ${utm_detail} | cut -d _ -f 2)
                    group by campaign_id, utm_id
                    ) b
                    on a.campaign_id = b.campaign_id
                        and a.utm_id = b.utm_id
                where a.utm_id is null
            ;
            ALTER TABLE ${project_name}.${type}_${table_name}_${org_id} AUTO_INCREMENT = 1
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id} which had no record FROM ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)]
        echo $sql_14
        mysql --login-path=$dest_login_path -e "$sql_14" 2>>$error_dir/${src_login_path}/$project_name/${project_name}.${type}_${table_name}_${org_id}_sql14.error

        echo ''
        echo [DROP TABLE ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2)]
        mysql --login-path=$dest_login_path -e "DROP TABLE ${project_name}.${type}_both_${org_id}_etl_$(echo ${utm_detail} | cut -d _ -f 1)_$(echo ${utm_detail} | cut -d _ -f 2);"


    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
echo ''
echo 'end: ' `date`
