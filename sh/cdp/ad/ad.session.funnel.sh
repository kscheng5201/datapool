#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 行銷漏斗
# Author: Benson Cheng
# Created_at: 2022-01-07
# Updated_at: 2022-01-07
# Note: 只有執行預設漏斗
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
            key idx_campaign_id (funnel_id), 
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
                    (select id from codebook_cdp.funnel where org_id = ${org_id} and channel = 'web' and is_default = 1) funnel_id, 
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
                        from codebook_cdp.funnel_config
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
                    from codebook_cdp.funnel_config
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
    
    
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo 'end: ' `date`
