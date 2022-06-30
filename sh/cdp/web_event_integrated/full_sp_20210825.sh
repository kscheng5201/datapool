#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web_event_integrated"
#export stakeholder="tmnewa" # 新安東京海上產險
#export src_login_path_1='tracker'
#export src_login_path_2='cdp'

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y%m%d"`
fi

export sql_query_1="
    SET innodb_lock_wait_timeout = 5000; 
    
    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_device_rank_d_tmnewa; 
	drop table if exists web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_device_rank_d_tmnewa as
        select fpc, device_type, count(*) freq, concat(max(date), space(1), max(start_time)) datetime
        from web_event_integrated.fpc_web_event_tmnewa
        where device_type is not null
            and date = date(now()) - interval 1 day
        group by fpc, device_type
    ; 

    select 'create index';
    create index idx_fpc on web_event_integrated.temp_pivot_device_rank_d_tmnewa (fpc); 
    create index idx_device_type on web_event_integrated.temp_pivot_device_rank_d_tmnewa (device_type);     

    select 'UPDATE web_event_integrated.pivot_device_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_device_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_device_rank_d_tmnewa b
        on a.fpc = b.fpc
        and a.device_type = b.device_type
    SET a.stat_date = date(now()) - interval 1 day, 
        a.freq = a.freq + ifnull(b.freq, 0), 
        a.datetime = if(b.datetime > a.datetime, b.datetime, a.datetime)
    ; 

    select 'drop table if exists';
	drop table if exists web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking as
        select  
            stat_date, 
            fpc, 
            device_type, 
            freq, 
            datetime,
            row_number () over (partition by fpc order by freq desc, datetime desc) ranking
        from web_event_integrated.pivot_device_rank_d_tmnewa
	; 

    select 'create index';
    create index idx_fpc on web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking (fpc); 
    create index idx_device_type on web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking (device_type);   
    create index idx_datetime on web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking (datetime);   
	create index idx_complex on web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking (fpc, device_type, datetime); 

    select 'UPDATE web_event_integrated.pivot_device_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_device_rank_d_tmnewa a
		LEFT JOIN web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking b
        on a.fpc = b.fpc and a.device_type = b.device_type and a.datetime = b.datetime
    SET a.ranking = b.ranking
    ; 

    select 'INSERT INTO web_event_integrated.pivot_device_rank_d_tmnewa';
	INSERT INTO web_event_integrated.pivot_device_rank_d_tmnewa
		SELECT 
			date(now()) - interval 1 day stat_date, 
			b.fpc, 
			b.device_type, 
			b.freq, 
			b.datetime, 
			row_number () over (partition by b.fpc order by b.freq desc, b.datetime desc)
		FROM web_event_integrated.pivot_device_rank_d_tmnewa a
		RIGHT JOIN web_event_integrated.temp_pivot_device_rank_d_tmnewa b
			on a.fpc = b.fpc
			and a.device_type = b.device_type
		WHERE a.fpc is null or a.device_type is null
	; 

    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_device_rank_d_tmnewa; 
	drop table if exists web_event_integrated.temp_pivot_device_rank_d_tmnewa_ranking; 

    select '';
	"

export sql_query_2="
    SET innodb_lock_wait_timeout = 5000;

	select 'drop table if exists'; 
    drop table if exists web_event_integrated.temp_pivot_domain_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_domain_rank_d_tmnewa;     

#    /** domain in particular on each fpc's datetime per day **/
    select 'create table if not exists'; 
    create table if not exists web_event_integrated.temp_pivot_domain_d_tmnewa as
        select 
            date, 
            fpc, 
            entry_domain domain,
            count(*) freq, 
            concat(max(date), space(1), max(start_time)) datetime
        from web_event_integrated.fpc_web_event_tmnewa
        where date = date(now()) - interval 1 day
        group by 
            date, 
            fpc, 
            entry_domain
    ; 
    select 'create index'; 
    create index idx_fpc on web_event_integrated.temp_pivot_domain_d_tmnewa (fpc); 
    create index idx_domain on web_event_integrated.temp_pivot_domain_d_tmnewa (domain);    
    create index idx_complex on web_event_integrated.temp_pivot_domain_d_tmnewa (fpc, domain);    

	select 'UPDATE web_event_integrated.pivot_domain_rank_d_tmnewa: LEFT JOIN'; 
 #   -- aggregated table on domain
    UPDATE web_event_integrated.pivot_domain_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_domain b 
        on a.fpc = b.fpc and a.domain = b.domain
    SET a.stat_date = date(now()) - interval 1 day, 
        a.freq = a.freq + ifnull(b.freq, 0)
    ; 
	
    select 'create table if not exists (again)'; 
    create table if not exists temp_pivot_domain_rank_d_tmnewa as
        select 
            stat_date, 
            fpc, 
            domain, 
            freq, 
            row_number () over (partition by fpc order by freq desc, datetime desc) ranking
        from web_event_integrated.pivot_domain_rank_d_tmnewa
        where stat_date = date(now()) - interval 1 day
    ; 
    
    select 'create index'; 
    create index idx_fpc on web_event_integrated.temp_pivot_domain_rank_d_tmnewa (fpc); 
    create index idx_domain on web_event_integrated.temp_pivot_domain_rank_d_tmnewa (domain);   
	create index idx_complex on web_event_integrated.temp_pivot_domain_rank_d_tmnewa (fpc, domain); 

	select 'UPDATE web_event_integrated.pivot_domain_rank_d_tmnewa: ranking'; 
    UPDATE web_event_integrated.pivot_domain_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_domain_rank_d_tmnewa b 
        on a.fpc = b.fpc and a.domain = b.domain 
    SET a.ranking = b.ranking
    ; 

	select 'INSERT INTO web_event_integrated.pivot_domain_rank_d_tmnewa: RIGHT JOIN'; 
	INSERT INTO web_event_integrated.pivot_domain_rank_d_tmnewa
		SELECT 
			date(now()) - interval 1 day stat_date, 
			b.fpc, 
			b.domain, 
			b.freq, 
			b.datetime, 
			row_number () over (partition by a.fpc order by b.freq desc, b.datetime desc) ranking
		FROM web_event_integrated.pivot_domain_rank_d_tmnewa a
			RIGHT JOIN web_event_integrated.temp_pivot_domain_d_tmnewa b 
			on a.fpc = b.fpc and a.domain = b.domain
		WHERE a.fpc is null or a.domain is null
	; 

	select 'drop table if exists'; 
    drop table if exists web_event_integrated.temp_pivot_domain_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_domain_rank_d_tmnewa;     

	select 'FINISH!! sp_pivot_domain_rank_d_20210817'; 
	"

export sql_query_3="
#    /** 行銷漏斗：最近一次互動的行銷漏斗數字 **/
    SET innodb_lock_wait_timeout = 5000;

	select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_funnel_last_d_tmnewa; 

	select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_funnel_last_d_tmnewa as
        select fpc, max(funnel_layer) funnel_layer, concat(max(date), space(1), max(start_time)) datetime
        from web_event_integrated.fpc_web_event_tmnewa
        where date = date(now()) - interval 1 day
        group by fpc
    ; 
	select 'ALTER TABLE';
	ALTER TABLE web_event_integrated.temp_pivot_funnel_last_d_tmnewa ADD primary key (fpc);   

	select 'UPDATE web_event_integrated.pivot_funnel_last_d_tmnewa';
    UPDATE web_event_integrated.pivot_funnel_last_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_funnel_last_d_tmnewa b
        on a.fpc = b.fpc
    SET a.stat_date = date(now()) - interval 1 day, 
        a.funnel_layer = if(b.datetime > a.datetime, b.funnel_layer, a.funnel_layer),
        a.datetime = if(b.datetime > a.datetime, b.datetime, a.datetime) 
    ; 

	select 'INSERT INTO web_event_integrated.pivot_funnel_last_d_tmnewa';
    INSERT INTO web_event_integrated.pivot_funnel_last_d_tmnewa
        SELECT 
            date(now()) - interval 1 day stat_date, 
            b.fpc, 
            b.funnel_layer, 
            b.datetime
        FROM web_event_integrated.pivot_funnel_last_d_tmnewa a
            RIGHT JOIN web_event_integrated.temp_pivot_funnel_last_d_tmnewa b
            on a.fpc = b.fpc
        WHERE a.fpc is null
    ; 

	select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_funnel_last_d_tmnewa; 
    
    select 'FINISH!! sp_pivot_funnel_last_d_20210817'; 
	"

export sql_query_4="
    SET innodb_lock_wait_timeout = 5000;
    
    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_medium_rank_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking;     

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_medium_rank_d_tmnewa as
        select 
            fpc, 
            utm_medium, 
            count(*) freq,
            concat(max(date), space(1), max(start_time)) datetime
        from web_event_integrated.fpc_web_event_tmnewa
        where date = date(now()) - interval 1 day
            and utm_medium is not null
        group by fpc, utm_medium
    ; 

    select 'create index and alter table';
    create index idx_fpc on web_event_integrated.temp_pivot_medium_rank_d_tmnewa (fpc); 
    create index idx_utm_medium on web_event_integrated.temp_pivot_medium_rank_d_tmnewa (utm_medium);    
	ALTER TABLE web_event_integrated.temp_pivot_medium_rank_d_tmnewa ADD PRIMARY KEY (fpc, utm_medium);
#--    alter table web_event_integrated.pivot_medium_rank_d_tmnewa convert to character set utf8mb4 collate utf8mb4_unicode_ci;
    alter table web_event_integrated.temp_pivot_medium_rank_d_tmnewa convert to character set utf8mb4 collate utf8mb4_unicode_ci;    

    select 'UPDATE web_event_integrated.pivot_medium_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_medium_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_medium_rank_d_tmnewa b
        on a.fpc = b.fpc and a.utm_medium = b.utm_medium
    SET a.freq = a.freq + ifnull(b.freq, 0), 
        a.datetime = ifnull(b.datetime, a.datetime), 
        a.stat_date = date(now()) - interval 1 day
    ; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking as
        select  
            stat_date, 
            fpc, 
            utm_medium, 
            freq, 
            datetime,
            row_number () over (partition by fpc order by freq desc, datetime desc) ranking
        from web_event_integrated.pivot_medium_rank_d_tmnewa
    ; 

    select 'create index and alter table';
    create index idx_fpc on web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking (fpc); 
    create index idx_utm_medium on web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking (utm_medium);  
    create index idx_datetime on web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking (datetime); 
	ALTER TABLE web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking ADD PRIMARY KEY (fpc, utm_medium, datetime);

    select 'UPDATE web_event_integrated.pivot_medium_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_medium_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking b
        on a.fpc = b.fpc and a.utm_medium = b.utm_medium and a.datetime = b.datetime
    SET a.ranking = b.ranking
    ; 

    select 'INSERT INTO web_event_integrated.pivot_medium_rank_d_tmnewa';
    INSERT INTO web_event_integrated.pivot_medium_rank_d_tmnewa
        SELECT 
            date(now()) - interval 1 day stat_date, 
            b.fpc, 
            b.utm_medium, 
            b.freq, 
            b.datetime,
            row_number () over (partition by b.fpc order by b.freq desc, b.datetime desc) ranking
        FROM web_event_integrated.pivot_medium_rank_d_tmnewa a
            RIGHT JOIN web_event_integrated.temp_pivot_medium_rank_d_tmnewa b
            on a.fpc = b.fpc and a.utm_medium = b.utm_medium
        WHERE a.fpc is null or a.utm_medium is null
    ; 

    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_medium_rank_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_medium_rank_d_tmnewa_ranking; 

    select 'FINISH!! sp_pivot_medium_rank_d_20210817';
	"

export sql_query_5="
    SET innodb_lock_wait_timeout = 5000;
    
    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_period_rank_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_period_rank_d_tmnewa as
        select fpc, period, count(*) freq, concat(max(date), space(1), max(start_time)) datetime
        from web_event_integrated.fpc_web_event_tmnewa
        where date = date(now()) - interval 1 day
        group by fpc, period
    ; 

    select 'create index and alter table';
    create index idx_fpc on web_event_integrated.temp_pivot_period_rank_d_tmnewa (fpc); 
    create index idx_period on web_event_integrated.temp_pivot_period_rank_d_tmnewa (period); 
    ALTER TABLE web_event_integrated.temp_pivot_period_rank_d_tmnewa ADD primary key (fpc, period);     
    alter table web_event_integrated.temp_pivot_period_rank_d_tmnewa convert to character set utf8mb4 collate utf8mb4_unicode_ci;    

    select 'UPDATE web_event_integrated.pivot_period_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_period_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_period_rank_d_tmnewa b
        on a.fpc = b.fpc
        and a.period = b.period
    SET a.stat_date = date(now()) - interval 1 day, 
        a.freq = a.freq + ifnull(b.freq, 0), 
        a.datetime = ifnull(b.datetime, a.datetime)
    ; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking as
        select  
            stat_date, 
            fpc, 
            period, 
            freq, 
            datetime,
            row_number () over (partition by fpc order by freq desc, datetime desc) ranking
        from web_event_integrated.pivot_period_rank_d_tmnewa
    ; 

    select 'create index and alter table';
    create index idx_fpc on web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking (fpc); 
    create index idx_period on web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking (period);  
    create index idx_datetime on web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking (datetime); 
	ALTER TABLE web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking ADD primary key (fpc, period, datetime); 

    select 'UPDATE web_event_integrated.pivot_period_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_period_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking b
        on a.fpc = b.fpc and a.period = b.period and a.datetime = b.datetime
    SET a.ranking = b.ranking
    ; 

    select 'INSERT INTO web_event_integrated.pivot_period_rank_d_tmnewa';
    INSERT INTO web_event_integrated.pivot_period_rank_d_tmnewa
        SELECT 
            date(now()) - interval 1 day stat_date, 
            b.fpc, 
            b.period, 
            b.freq, 
            b.datetime,
            row_number () over (partition by b.fpc order by b.freq desc, b.datetime desc) ranking
        FROM web_event_integrated.pivot_period_rank_d_tmnewa a
            RIGHT JOIN web_event_integrated.temp_pivot_period_rank_d_tmnewa b
            on a.fpc = b.fpc
            and a.period = b.period
        WHERE a.fpc is null or b.period is null
    ; 

    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_period_rank_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_period_rank_d_tmnewa_ranking; 

    select 'FINISH!! sp_pivot_period_rank_d_20210817';
	"

export sql_query_6="
    SET innodb_lock_wait_timeout = 5000;
    
    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_source_rank_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking;     

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_source_rank_d_tmnewa as
        select 
            fpc, 
            utm_source, 
            count(*) freq,
            concat(max(date), space(1), max(start_time)) datetime
        from web_event_integrated.fpc_web_event_tmnewa
        where date = date(now()) - interval 1 day
            and utm_source is not null
        group by fpc, utm_source
    ; 

    select 'create index and alter table';
    create index idx_fpc on web_event_integrated.temp_pivot_source_rank_d_tmnewa (fpc); 
    create index idx_utm_source on web_event_integrated.temp_pivot_source_rank_d_tmnewa (utm_source);     
	ALTER TABLE web_event_integrated.temp_pivot_source_rank_d_tmnewa ADD PRIMARY KEY (fpc, utm_source);
# --    alter table web_event_integrated.pivot_source_rank_d_tmnewa convert to character set utf8mb4 collate utf8mb4_unicode_ci;
    alter table web_event_integrated.temp_pivot_source_rank_d_tmnewa convert to character set utf8mb4 collate utf8mb4_unicode_ci;    

    select 'UPDATE web_event_integrated.pivot_source_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_source_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_source_rank_d_tmnewa b
        on a.fpc = b.fpc and a.utm_source = b.utm_source
    SET a.freq = a.freq + ifnull(b.freq, 0), 
        a.datetime = if(b.datetime > a.datetime, b.datetime, a.datetime), 
        a.stat_date = date(now()) - interval 1 day
    ; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking as
        select  
            stat_date, 
            fpc, 
            utm_source, 
            freq, 
            datetime,
            row_number () over (partition by fpc order by freq desc, datetime desc) ranking
        from web_event_integrated.pivot_source_rank_d_tmnewa
    ; 

    select 'create index and alter table';
    create index idx_fpc on web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking (fpc); 
    create index idx_utm_source on web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking (utm_source);  
    create index idx_datetime on web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking (datetime); 
    ALTER TABLE web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking ADD PRIMARY KEY (fpc, utm_source, datetime); 

    select 'UPDATE web_event_integrated.pivot_source_rank_d_tmnewa';
    UPDATE web_event_integrated.pivot_source_rank_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking b
        on a.fpc = b.fpc and a.utm_source = b.utm_source and a.datetime = b.datetime
    SET a.ranking = b.ranking
    ; 

    select 'INSERT INTO web_event_integrated.pivot_source_rank_d_tmnewa';
    INSERT INTO web_event_integrated.pivot_source_rank_d_tmnewa
        SELECT 
            date(now()) - interval 1 day stat_date, 
            b.fpc, 
            b.utm_source, 
            b.freq, 
            b.datetime,
            row_number () over (partition by b.fpc order by b.freq desc, b.datetime desc) ranking
        FROM web_event_integrated.pivot_source_rank_d_tmnewa a
            RIGHT JOIN web_event_integrated.temp_pivot_source_rank_d_tmnewa b
            on a.fpc = b.fpc and a.utm_source = b.utm_source
        WHERE a.fpc is null or a.utm_source is null
    ; 

    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_source_rank_d_tmnewa; 
    drop table if exists web_event_integrated.temp_pivot_source_rank_d_tmnewa_ranking; 

    select 'FINISH!! sp_pivot_source_rank_d_20210817';
	"

export sql_query_7="
    SET innodb_lock_wait_timeout = 5000;
    
    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_weekday_prop_d_tmnewa; 

    select 'create table if not exists';
    create table if not exists web_event_integrated.temp_pivot_weekday_prop_d_tmnewa as
        select 
            fpc, 
            sum(if(weekday(date) >= 5, 1, 0)) session_on_weekend,
            sum(if(weekday(date) <= 4, 1, 0)) session_on_weekday
#        -- Note: 0 = Monday, 1 = Tuesday, 2 = Wednesday, 3 = Thursday, 4 = Friday, 5 = Saturday, 6 = Sunday.
        from web_event_integrated.fpc_web_event_tmnewa
        where date = date(now()) - interval 1 day
        group by fpc
    ; 

    select 'alter table';
	ALTER TABLE web_event_integrated.temp_pivot_weekday_prop_d_tmnewa ADD PRIMARY KEY (fpc);

    select 'UPDATE web_event_integrated.pivot_weekday_prop_d_tmnewa';
    UPDATE web_event_integrated.pivot_weekday_prop_d_tmnewa a
        LEFT JOIN web_event_integrated.temp_pivot_weekday_prop_d_tmnewa b
        ON a.fpc = b.fpc
    SET a.session_on_weekend = a.session_on_weekend + ifnull(b.session_on_weekend, 0), 
        a.session_on_weekday = a.session_on_weekday + ifnull(b.session_on_weekday, 0), 
        a.stat_date = date(now()) - interval 1 day
    ; 

    select 'INSERT INTO web_event_integrated.pivot_weekday_prop_d_tmnewa';
	INSERT INTO web_event_integrated.pivot_weekday_prop_d_tmnewa
		SELECT 
			date(now()) - interval 1 day stat_date, 
			b.fpc, 
			b.session_on_weekend, 
			b.session_on_weekday
		FROM web_event_integrated.pivot_weekday_prop_d_tmnewa a
			RIGHT JOIN web_event_integrated.temp_pivot_weekday_prop_d_tmnewa b
			ON a.fpc = b.fpc
		WHERE a.fpc is null
	; 

    select 'drop table if exists';
    drop table if exists web_event_integrated.temp_pivot_weekday_prop_d_tmnewa; 

    select 'FINISH!! sp_pivot_weekday_prop_d_20210817';
	"

export sql_query_8="
   drop table if exists web_event_integrated.full_web_event_tmnewa; 
    
    create table web_event_integrated.full_web_event_tmnewa as
        select 
            a.stat_date, 
            a.fpc, 
            a.first_date, 
            a.last_date, 
            timestampdiff(day, a.first_date, a.last_date) interval_day,
            timestampdiff(day, a.last_date, date(now())) interval_last_visit_day, 
            if(timestampdiff(day, a.first_date, a.last_date) = 0, -1,
                round(
                    timestampdiff(day, a.last_date, date(now())) / timestampdiff(day, a.first_date, a.last_date)
                    , 4
                    )
                ) interactive_cycle_multiple, 
            a.session_freq, 
            timestampdiff(day, a.first_date, a.last_date) / a.session_freq avg_visit_day, 
            a.stay_second, 
            a.stay_second / a.session_freq avg_visit_sec, 
            
            b.domain most_domain, 
            c.utm_source most_source, 
            d.utm_medium most_medium, 
            (session_on_weekend / 2) / ((session_on_weekend / 2) + (session_on_weekday / 5)) weekend_visit_prop, 
            funnel_layer last_funnel_layer, 
            device_type most_device, 
            period most_period
        from web_event_integrated.fpc_basic_summary_d a, 
            (
            select *
            from web_event_integrated.pivot_domain_rank_d_tmnewa 
            where ranking = 1
            ) b, 
            (
            select *
            from web_event_integrated.pivot_source_rank_d_tmnewa 
            where ranking = 1
            ) c,     
            (
            select *
            from web_event_integrated.pivot_medium_rank_d_tmnewa 
            where ranking = 1
            ) d,         
            web_event_integrated.pivot_weekday_prop_d_tmnewa e, 
            web_event_integrated.pivot_funnel_last_d_tmnewa f, 
            (
            select *
            from web_event_integrated.pivot_device_rank_d_tmnewa 
            where ranking = 1
            ) g, 
            (
            select *
            from web_event_integrated.pivot_period_rank_d_tmnewa 
            where ranking = 1
            ) h    
        where a.fpc = b.fpc
            and b.fpc = c.fpc
            and c.fpc = d.fpc
            and d.fpc = e.fpc
            and e.fpc = f.fpc
            and f.fpc = g.fpc
            and g.fpc = h.fpc
    ; 
    
    
    drop table if exists summary.full_web_event_tmnewa; 
    
    create table summary.full_web_event_tmnewa as
        select * from web_event_integrated.full_web_event_tmnewa
    ; 
    
    alter table summary.full_web_event_tmnewa
        add primary key (fpc)
    ; 
    alter table web_event_integrated.full_web_event_tmnewa
        add primary key (fpc)
    ; 
	"

#mysql --login-path=$dest_login_path -e "$sql_query_1"
#mysql --login-path=$dest_login_path -e "$sql_query_2"
#mysql --login-path=$dest_login_path -e "$sql_query_3"
mysql --login-path=$dest_login_path -e "$sql_query_4"
mysql --login-path=$dest_login_path -e "$sql_query_5"
mysql --login-path=$dest_login_path -e "$sql_query_6"
mysql --login-path=$dest_login_path -e "$sql_query_7"
mysql --login-path=$dest_login_path -e "$sql_query_8"

