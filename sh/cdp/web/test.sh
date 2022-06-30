        export sql_111z="
                    select 
                        fpc, 
                        created_at, 
                        domain, 
                        behavior, 
                        session_pre, 
                        case 
                            when traffic_type = 'Ad' then 'Ad'
                            when referrer is null or referrer = '' or referrer REGEXP domain then 'Direct'
                            when referrer REGEXP '://' and referrer REGEXP 'google\\.|yahoo\\.|bing\\.|MSN\\.' then 'Organic'
                            when referrer REGEXP 'google|yahoo|bing|MSN' then 'Organic'
                            else 'Others'
                        end traffic_type, 
                        referrer,
                        if(traffic_type = 'Ad' and campaign is null, 'Others', campaign) campaign,
                        if(traffic_type = 'Ad' and source_medium is null, 'Others', source_medium) source_medium,
                        event_type, 
                        page_title, 
                        page_url, 
                        IF(@fpc = fpc, 
                            IF(session_pre = 1, @session_pre := @session_pre + 1, @session_pre), 
                                @session_pre := 1) session_break, 
                        @fpc := fpc
                    from ${project_name}.${type}_${table_name}_${org_id}_etl_b c,
                        (select @session_pre := 1, @fpc) d
            ;"
        echo ''
        echo [DELETE FROM ${project_name}.${type}_${table_name}_${org_id}_etl]
        echo $sql_111z
