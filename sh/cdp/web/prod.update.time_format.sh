#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web"
export table_name_t="time_format"
export table_name_r="recommendation"  
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
`date -d "$(date +%Y-01-01) -1 day" +"%Y-%m-%d"`
"

export sql_3="
    UPDATE ${project_name}.${table_name_t}
    SET time_format = concat(span_display, '(', date_format('${vDate}', '%m/%d'), ')')
    WHERE span_display = '昨日'
    ;"
echo ''
#echo $sql_3
echo [UPDATE ${project_name}.${table_name_t} on ${vDate}]
mysql --login-path=$dest_login_path -e "$sql_3" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    


if [ ${vDateName} = Sun ];
then 
    export sql_4="
        UPDATE ${project_name}.${table_name_t}
        SET time_format = concat(
                span_display, '(', date_format(STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), '%m/%d'), 
                ' - ', date_format(STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 6 day, '%m/%d'), ')')
        WHERE span_display = '上一週'
	;
        UPDATE ${project_name}.${table_name_r}
        SET time_format = concat(
                '上一週(', date_format(STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W'), '%m/%d'), 
                ' - ', date_format(STR_TO_DATE(CONCAT(yearweek('${vDate}', 7),' Monday'), '%X%V %W') + interval 6 day, '%m/%d'), ')')
        WHERE time_format like '上一週%' 
        ;"
    echo ''
    #echo $sql_4
    echo [start: date on ${vDate}. Is ${vDateName} = Sun?]
    echo [UPDATE ${project_name}.${table_name_t} on ${vDate}]
    mysql --login-path=$dest_login_path -e "$sql_4" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    
else 
    echo [today is ${vDateName}, not Sun. No Need to do the weekly statistics.]
fi 


if [ ${vDate} = ${vMonthLast} ];
then 
    export sql_5="   
        UPDATE ${project_name}.${table_name_t}
        SET time_format = concat(
                span_display, '(', date_format('${vDate}' + interval 1 day - interval 1 month, '%m/%01'), 
                ' - ', date_format('${vDate}', '%m/%d'), ')')
        WHERE span_display = '上個月'
        ; 
        UPDATE ${project_name}.${table_name_r}
        SET time_format = concat(
                '上個月(', date_format('${vDate}' + interval 1 day - interval 1 month, '%m/%01'), 
                ' - ', date_format('${vDate}', '%m/%d'), ')')
        WHERE time_format like '上個月%'
        ;"
    echo ''
    echo $sql_5
    echo [start: date on ${vDate}. ${vDate} = ${vMonthLast}?]
    echo [UPDATE ${project_name}.${table_name_t} on ${vDate}]
    mysql --login-path=$dest_login_path -e "$sql_5" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    
else 
    echo [today is ${vDate}, not ${vMonthLast}. No Need to do the monthly statistics.]
fi 


for seasonDate in $seasonEnd
do
    if [ ${vDate} = ${seasonDate} ];
    then 
        export sql_6="
            UPDATE ${project_name}.${table_name_t}
            SET time_format = concat(
                    span_display, '(', date_format('${vDate}' + interval 1 day - interval 3 month, '%m/%01'), 
                    ' - ', date_format('${vDate}', '%m/%d'), ')')
            WHERE span_display = '上一季'
	    ;
            UPDATE ${project_name}.${table_name_r}
            SET time_format = concat(
                    '上一季(', date_format('${vDate}' + interval 1 day - interval 3 month, '%m/%01'), 
                    ' - ', date_format('${vDate}', '%m/%d'), ')')
            WHERE time_format like '上一季%'
            ;"
        echo ''
        # echo $sql_6
        echo [start: date on ${vDate}. ${vDate} = ${seasonDate}?]
        echo [UPDATE ${project_name}.${table_name_t} on ${vDate}]
        mysql --login-path=$dest_login_path -e "$sql_6" 2>>$error_dir/$src_login_path/$project_name/${project_name}.${type}_${table_name}_${org_id}.error    
    else 
        echo [today is ${vDate}, not ${seasonDate}. No Need to do the seasonal statistics.]
    fi 
done 
