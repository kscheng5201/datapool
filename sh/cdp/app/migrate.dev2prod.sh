#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
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
        FROM ${project_name}.session_kpi_${org_id}
        ;"
    echo $sql_kpi
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.session_kpi_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_kpi" > $export_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}.txt' INTO TABLE ${project_name}.session_kpi_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.session_kpi_${org_id}_new.error


    export sql_graph="
        select *
        FROM ${project_name}.session_kpi_${org_id}_graph
        ;"
    echo $sql_graph
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.session_kpi_${org_id}_graph.txt]
    mysql --login-path=$src_login_true -e "$sql_graph" > $export_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_graph.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_graph_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_graph.txt' INTO TABLE ${project_name}.session_kpi_${org_id}_graph IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.session_kpi_${org_id}_graph_new.error


    export sql_token="
        select *
        FROM ${project_name}.session_kpi_${org_id}_token
        ;"
    echo $sql_token
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.session_kpi_${org_id}_token.txt]
    mysql --login-path=$src_login_true -e "$sql_token" > $export_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_token.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_token_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.session_kpi_${org_id}_token.txt' INTO TABLE ${project_name}.session_kpi_${org_id}_token IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.session_kpi_${org_id}_token_new.error


    export sql_onliner="
        select *
        FROM ${project_name}.session_onliner_${org_id}
        ;"
    echo $sql_onliner
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.session_onliner_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_onliner" > $export_dir/$src_login_path/$project_name/${project_name}.session_onliner_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.session_onliner_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.session_onliner_${org_id}.txt' INTO TABLE ${project_name}.session_onliner_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.session_onliner_${org_id}_new.error


    export sql_p_onliner="
        select *
        FROM ${project_name}.person_onliner_${org_id}
        ;"
    echo $sql_p_onliner
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.person_onliner_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_p_onliner" > $export_dir/$src_login_path/$project_name/${project_name}.person_onliner_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.person_onliner_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.person_onliner_${org_id}.txt' INTO TABLE ${project_name}.person_onliner_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.person_onliner_${org_id}_new.error


    export sql_s_event="
        select *
        FROM ${project_name}.session_event_${org_id}
        ;"
    echo $sql_s_event
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.session_event_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_s_event" > $export_dir/$src_login_path/$project_name/${project_name}.session_event_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.session_event_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.session_event_${org_id}.txt' INTO TABLE ${project_name}.session_event_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.session_event_${org_id}_new.error


    export sql_p_event="
        select *
        FROM ${project_name}.person_event_${org_id}
        ;"
    echo $sql_p_event
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.person_event_${org_id}.txt]
    mysql --login-path=$src_login_true -e "$sql_p_event" > $export_dir/$src_login_path/$project_name/${project_name}.person_event_${org_id}.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.person_event_${org_id}_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.person_event_${org_id}.txt' INTO TABLE ${project_name}.person_event_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.person_event_${org_id}_new.error


    export sql_p_event_token="
        select *
        FROM ${project_name}.person_event_${org_id}_token
        ;"
    echo $sql_p_event_token
    #### Export Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [exporting data to ${project_name}.person_event_${org_id}_token.txt]
    mysql --login-path=$src_login_true -e "$sql_p_event_token" > $export_dir/$src_login_path/$project_name/${project_name}.person_event_${org_id}_token.txt 2>>$error_dir/$src_login_path/$project_name/${project_name}.person_event_${org_id}_token_new.error

    #### Import Data ####
    echo ''
    echo [start: date on ${vDate}]
    echo [import data to ${project_name}.${type_s}_${table_name}_${org_id}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${project_name}.person_event_${org_id}_token.txt' INTO TABLE ${project_name}.person_event_${org_id}_token IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.person_event_${org_id}_token_new.error



done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
#done < /root/datapool/export_file/cdp/web/${project_name}.cdp_org_id.txt

echo ''
echo [end the ${vDate} data on `date`]
