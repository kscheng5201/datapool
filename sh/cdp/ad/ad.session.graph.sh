#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: kpi 趨勢圖
# Author: Benson Cheng
# Created_at: 2021-01-11
# Updated_at: 2021-01-11
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
    vDate=`date -d $1 +"%Y-%m-%d"`
    nvDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
    nvDate=`date -d "1 day ago" +"%Y%m%d"`    
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
`date -d "$(date +%Y-01-01) -1 day" +"%Y-%m-%d"`
"

while read org_id; 
do 
    echo ''
    echo [DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_graph]
    mysql --login-path=$dest_login_path -e "DROP TABLE IF EXISTS ${project_name}.${type}_${table_name}_${org_id}_graph;"
    
    export sql_0="
        CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_graph (
            serial int auto_increment NOT NULL COMMENT 'auto_increment Serial Number' unique, 
            tag_date date NOT NULL COMMENT '資料統計日', 
            span varchar(8) NOT NULL COMMENT 'daily/weekly/monthly/seasonal/yearly', 
            start_date date NOT NULL COMMENT '資料起始日',
            end_date date NOT NULL COMMENT '資料結束日',
            x_axis date NOT NULL COMMENT '供前後端工程師繪製趨勢圖時的時間座標',
            user_type varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '用戶類型：全部用戶(ALL); 新用戶(new); 舊用戶(old); 重複進站用戶(repeated)',            
            campaign_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign 編號',
            utm_id int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'campaign_utm 流水編號',
            click int NOT NULL DEFAULT '0' COMMENT '點擊次數：有點到 utm/campaign 的 page_url 紀錄',
            user int NOT NULL DEFAULT '0' COMMENT '進站人數：每天每人點到 utm/campaign 的次數',
            new_prop int NOT NULL DEFAULT '0' COMMENT '新用戶佔全部用戶的比例',
            stay_time int NOT NULL DEFAULT 0 COMMENT '有效工作階段平均停留秒數',
            bounce_rate int NOT NULL DEFAULT 0 COMMENT '跳出率(%)',
	    purchased int NOT NULL DEFAULT 0 COMMENT '完成購買次數（event = 14）',
            time_flag varchar(16) DEFAULT NULL COMMENT 'current: 本週; last: 上週',
            created_at datetime NOT NULL COMMENT '網頁瀏覽或事件觸發的原始時間戳記',
            updated_at timestamp NOT NULL DEFAULT current_timestamp on update current_timestamp COMMENT '更新時間',
            primary key (tag_date, span, start_date, end_date, x_axis, user_type, campaign_id, utm_id),
            key idx_tag_date (tag_date), 
            key idx_start_date (start_date),
            key idx_x_axis (x_axis), 
    	    key idx_campaign_id (campaign_id),
            key idx_utm_id (utm_id),
            key idx_user_type (user_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='綜合儀表板【長期統計】'
        ;"
    echo ''
    echo $sql_0
    echo [CREATE TABLE IF NOT EXISTS ${project_name}.${type}_${table_name}_${org_id}_graph]
    mysql --login-path=$dest_login_path -e "$sql_0" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_graph_sql_0.error        


    while read campaign_detail;
    do 
        campaign_start=`(date -d $(echo ${campaign_detail} | cut -d _ -f 2) +"%Y%m%d")`
        campaign_end=`(date -d $(echo ${campaign_detail} | cut -d _ -f 3) +"%Y%m%d")`    

        ###################################################
        # Integers can be compared with these operators:
        # Bash scripting cheatsheet: https://devhints.io/bash#conditionals
        #
        # -eq # Equal
        # -ne # Not equal
        # -lt # Less than
        # -le # Less than or equal
        # -gt # Greater than
        # -ge # Greater than or equal
        ###################################################
    
        if [ ${nvDate} -lt ${campaign_end} ];
        then
            export sql_1="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'weekly' span, 
                        STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') start_date,
                        STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 6 day end_date, 
                        start_date x_axis,
                        user_type,
                        campaign_id, 
                        utm_id, 
                        click,
                        user, 
                        new_prop,
                        stay_time, 
                        bounce_rate, 
                        purchased, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}
                    where start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') - interval 7 day
                        and start_date < STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 7 day
                        and span = 'daily'
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_graph AUTO_INCREMENT = 1
                ;"
            echo ''
            echo [${vDate} < $(echo ${campaign_detail} | cut -d _ -f 3)?]
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph]
            echo $sql_1
            mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_graph_sql_1.error        
            
            export sql_2="
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph
                SET time_flag = if(start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), 'current', 'last')
                WHERE span = 'weekly'
                ;"
            echo ''
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph]
            echo $sql_2
            mysql --login-path=$dest_login_path -e "$sql_2" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_graph_sql_2.error        


            for i in $(seq 0 6)
            do
                export sql_2a="
                    INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_graph
                        select 
                            null serial, 
                            tag_date, 
                            span, 
                            start_date, 
                            end_date, 
                            start_date + interval ${i} day x_axis, 
                            user_type, 
                            campaign_id, 
                            utm_id, 
                            null click, 
                            null user, 
                            null new_prop, 
                            null stay_time, 
                            null bounce_rate, 
                            null purchased, 
                            time_flag, 
                            now() created_at, 
                            now() updated_at
                        from ${project_name}.${type}_${table_name}_${org_id}_graph
                        where time_flag = 'current'
                            and span = 'weekly'
                    ;
                    ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_graph AUTO_INCREMENT = 1
                    ;"
                echo ''
                echo [INSERT IGNORE INTO ${project_name}.${type}_${table_name}_${org_id}_graph]
                echo $sql_2a
                mysql --login-path=$dest_login_path -e "$sql_2a" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_graph_sql_2a.error
            done 

        else
            export sql_3="
                INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph
                    select 
                        null serial, 
                        '${vDate}' + interval 1 day tag_date, 
                        'FULL' span, 
                        '${campaign_start}' start_date,
                        '${campaign_end}' end_date, 
                        start_date x_axis,
                        user_type,
                        campaign_id, 
                        utm_id, 
                        click,
                        user, 
                        new_prop,
                        stay_time, 
                        bounce_rate, 
                        purchased, 
                        null time_flag, 
                        now() created_at, 
                        now() updated_at
                    from ${project_name}.${type}_${table_name}_${org_id}
                    where start_date >= '${campaign_start}'
                        and start_date < '${campaign_end}' + interval 1 day
                        and span = 'daily'
                        and campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
                ;
                ALTER TABLE ${project_name}.${type}_${table_name}_${org_id}_graph AUTO_INCREMENT = 1
                ;"            
            echo ''
            echo [${vDate} >= $(echo ${campaign_detail} | cut -d _ -f 3)?]
            echo ''
            echo [INSERT INTO ${project_name}.${type}_${table_name}_${org_id}_graph]
            echo $sql_3
            mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_graph_sql_3.error        
            
            export sql_4="
                UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph
                SET time_flag = if(start_date >= STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), 'current', 'last')
                WHERE span = 'FULL'
                ;"
            echo ''
            echo [UPDATE ${project_name}.${type}_${table_name}_${org_id}_graph]
            echo $sql_4
            mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}_graph_sql_4.error        
        fi
        
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt

echo ''
echo 'end: ' `date`
