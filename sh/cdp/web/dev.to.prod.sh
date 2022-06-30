#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export src_login_path="cdp"
export src_login_true="datapool"

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


while read org_id; 
do 
    export sql_kpi="
        select *
        FROM web.session_kpi_${org_id}
        ;"
    echo $sql_kpi
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_kpi_${org_id}_new.txt]
    mysql --login-path=$src_login_true -e "$sql_kpi" > $export_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_new.txt' INTO TABLE web.session_kpi_${org_id}_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_kpi_${org_id}_new.error


    export sql_graph="
        select *
        FROM web.session_kpi_${org_id}_graph
        ;"
    echo $sql_graph
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_kpi_${org_id}_graph_new.txt]
    mysql --login-path=$src_login_true -e "$sql_graph" > $export_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_graph_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_graph_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_graph_new.txt' INTO TABLE web.session_kpi_${org_id}_graph_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_kpi_${org_id}_graph_new.error


    export sql_fpc="
        select *
        FROM web.session_kpi_${org_id}_fpc
        ;"
    echo $sql_fpc
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_kpi_${org_id}_fpc_new.txt]
    mysql --login-path=$src_login_true -e "$sql_fpc" > $export_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_fpc_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_fpc_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_kpi_${org_id}_fpc_new.txt' INTO TABLE web.session_kpi_${org_id}_fpc_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_kpi_${org_id}_fpc_new.error


    export sql_onliner="
        select *
        FROM web.session_onliner_${org_id}
        ;"
    echo $sql_onliner
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_onliner_${org_id}_new.txt]
    mysql --login-path=$src_login_true -e "$sql_onliner" > $export_dir/$src_login_path/$project_name/web.session_onliner_${org_id}_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_onliner_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_onliner_${org_id}_new.txt' INTO TABLE web.session_onliner_${org_id}_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_onliner_${org_id}_new.error


    export sql_p_onliner="
        select *
        FROM web.person_onliner_${org_id}
        ;"
    echo $sql_p_onliner
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_onliner_${org_id}_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_onliner" > $export_dir/$src_login_path/$project_name/web.person_onliner_${org_id}_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_onliner_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_onliner_${org_id}_new.txt' INTO TABLE web.person_onliner_${org_id}_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_onliner_${org_id}_new.error


    export sql_s_landing="
        select *
        FROM web.session_page_${org_id}_landing
        ;"
    echo $sql_s_landing
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_page_${org_id}_landing_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_landing" > $export_dir/$src_login_path/$project_name/web.session_page_${org_id}_landing_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_page_${org_id}_landing_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_page_${org_id}_landing_new.txt' INTO TABLE web.session_page_${org_id}_landing_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_page_${org_id}_landing_new.error


    export sql_s_traffic="
        select *
        FROM web.session_page_${org_id}_traffic
        ;"
    echo $sql_s_traffic
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_page_${org_id}_traffic_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_traffic" > $export_dir/$src_login_path/$project_name/web.session_page_${org_id}_traffic_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_page_${org_id}_traffic_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_page_${org_id}_traffic_new.txt' INTO TABLE web.session_page_${org_id}_traffic_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_page_${org_id}_traffic_new.error


    export sql_s_campaign="
        select *
        FROM web.session_page_${org_id}_campaign
        ;"
    echo $sql_s_campaign
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_page_${org_id}_campaign_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_campaign" > $export_dir/$src_login_path/$project_name/web.session_page_${org_id}_campaign_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_page_${org_id}_campaign_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_page_${org_id}_campaign_new.txt' INTO TABLE web.session_page_${org_id}_campaign_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_page_${org_id}_campaign_new.error


    export sql_s_medium="
        select *
        FROM web.session_page_${org_id}_medium
        ;"
    echo $sql_s_medium
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_page_${org_id}_medium_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_medium" > $export_dir/$src_login_path/$project_name/web.session_page_${org_id}_medium_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_page_${org_id}_medium_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_page_${org_id}_medium_new.txt' INTO TABLE web.session_page_${org_id}_medium_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_page_${org_id}_medium_new.error


    export sql_s_domain="
        select *
        FROM web.session_page_${org_id}_domain
        ;"
    echo $sql_s_domain
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_page_${org_id}_domain_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_domain" > $export_dir/$src_login_path/$project_name/web.session_page_${org_id}_domain_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_page_${org_id}_domain_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_page_${org_id}_domain_new.txt' INTO TABLE web.session_page_${org_id}_domain_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_page_${org_id}_domain_new.error


    export sql_s_title="
        select *
        FROM web.session_page_${org_id}_title
        ;"
    echo $sql_s_title
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_page_${org_id}_title_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_title" > $export_dir/$src_login_path/$project_name/web.session_page_${org_id}_title_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_page_${org_id}_title_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_page_${org_id}_title_new.txt' INTO TABLE web.session_page_${org_id}_title_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_page_${org_id}_title_new.error


    export sql_p_landing="
        select *
        FROM web.person_page_${org_id}_landing
        ;"
    echo $sql_p_landing
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_landing_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_landing" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_landing_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_landing_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_landing_new.txt' INTO TABLE web.person_page_${org_id}_landing_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_landing_new.error


    export sql_p_traffic="
        select *
        FROM web.person_page_${org_id}_traffic
        ;"
    echo $sql_p_traffic
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_traffic_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_traffic" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_traffic_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_traffic_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_traffic_new.txt' INTO TABLE web.person_page_${org_id}_traffic_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_traffic_new.error


    export sql_p_campaign="
        select *
        FROM web.person_page_${org_id}_campaign
        ;"
    echo $sql_p_campaign
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_campaign_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_campaign" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_campaign_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_campaign_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_campaign_new.txt' INTO TABLE web.person_page_${org_id}_campaign_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_campaign_new.error


    export sql_p_medium="
        select *
        FROM web.person_page_${org_id}_medium
        ;"
    echo $sql_p_medium
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_medium_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_medium" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_medium_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_medium_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_medium_new.txt' INTO TABLE web.person_page_${org_id}_medium_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_medium_new.error


    export sql_p_domain="
        select *
        FROM web.person_page_${org_id}_domain
        ;"
    echo $sql_p_domain
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_domain_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_domain" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_domain_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_domain_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_domain_new.txt' INTO TABLE web.person_page_${org_id}_domain_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_domain_new.error


    export sql_p_title="
        select *
        FROM web.person_page_${org_id}_title
        ;"
    echo $sql_p_title
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_title_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_title" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_title_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_title_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_title_new.txt' INTO TABLE web.person_page_${org_id}_title_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_title_new.error


    export sql_s_event="
        select *
        FROM web.session_event_${org_id}
        ;"
    echo $sql_s_event
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.session_event_${org_id}_new.txt]
    mysql --login-path=$src_login_true -e "$sql_s_event" > $export_dir/$src_login_path/$project_name/web.session_event_${org_id}_new.txt 2>>$error_dir/$src_login_path/$project_name/web.session_event_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.session_event_${org_id}_new.txt' INTO TABLE web.session_event_${org_id}_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.session_event_${org_id}_new.error


    export sql_p_event="
        select *
        FROM web.person_event_${org_id}
        ;"
    echo $sql_p_event
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_event_${org_id}_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_event" > $export_dir/$src_login_path/$project_name/web.person_event_${org_id}_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_event_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_event_${org_id}_new.txt' INTO TABLE web.person_event_${org_id}_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_event_${org_id}_new.error


    export sql_p_event_fpc="
        select *
        FROM web.person_event_${org_id}_fpc
        ;"
    echo $sql_p_event_fpc
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_event_${org_id}_fpc_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_event_fpc" > $export_dir/$src_login_path/$project_name/web.person_event_${org_id}_fpc_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_event_${org_id}_fpc_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_event_${org_id}_fpc_new.txt' INTO TABLE web.person_event_${org_id}_fpc_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_event_${org_id}_fpc_new.error


    export sql_p_traffic_fpc="
        select *
        FROM web.person_page_${org_id}_traffic_fpc
        ;"
    echo $sql_p_traffic_fpc
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_traffic_fpc_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_traffic_fpc" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_traffic_fpc_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_traffic_fpc_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_traffic_fpc_new.txt' INTO TABLE web.person_page_${org_id}_traffic_fpc_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_traffic_fpc_new.error


    export sql_p_campaign_fpc="
        select *
        FROM web.person_page_${org_id}_campaign_fpc
        ;"
    echo $sql_p_campaign_fpc
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_campaign_fpc_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_campaign_fpc" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_campaign_fpc_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_campaign_fpc_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_campaign_fpc_new.txt' INTO TABLE web.person_page_${org_id}_campaign_fpc_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_campaign_fpc_new.error

    export sql_p_domain_fpc="
        select *
        FROM web.person_page_${org_id}_domain_fpc
        ;"
    echo $sql_p_domain_fpc
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_domain_fpc_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_domain_fpc" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_domain_fpc_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_domain_fpc_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_domain_fpc_new.txt' INTO TABLE web.person_page_${org_id}_domain_fpc_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_domain_fpc_new.error


    export sql_p_title_fpc="
        select *
        FROM web.person_page_${org_id}_title_fpc
        ;"
    echo $sql_p_title_fpc
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to web.person_page_${org_id}_title_fpc_new.txt]
    mysql --login-path=$src_login_true -e "$sql_p_title_fpc" > $export_dir/$src_login_path/$project_name/web.person_page_${org_id}_title_fpc_new.txt 2>>$error_dir/$src_login_path/$project_name/web.person_page_${org_id}_title_fpc_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/web.person_page_${org_id}_title_fpc_new.txt' INTO TABLE web.person_page_${org_id}_title_fpc_new IGNORE 1 LINES;" 2>>$error_dir/$project_name/web.person_page_${org_id}_title_fpc_new.error





done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_org_id.txt
#done < /root/datapool/export_file/cdp/web/web.cdp_org_id.txt

echo ''
echo [end the ${vDate} data on `date`]
