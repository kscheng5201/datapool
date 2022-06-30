#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export table_name="accu_mapping" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### Get DateName (Mon to Sun)
if [ -n "$1" ]; 
then
    vDateName=`date -d $1 '+%a'`
else
    vDateName=`date -d "1 day ago" '+%a'`
fi

#### Get the First and Last Date of Month ####
if [ -n "$1" ]; 
then 
    vMonthFirst=`date -d $1 +"%Y-%m-01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y-%m-%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y-%m-01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y-%m-%d"`
fi


while read org_id; 
do 
    echo ''
    echo [Make member_id and accu_id as unique on org ${org_id}]
    export sql="
        UPDATE ${project_name}.${table_name}_${org_id} a
            INNER JOIN 
            (
            select *
            from (
                select 
                    accu_id, 
                    member_id, 
                    serial, 
                    row_number () over (partition by member_id order by serial) rid
                from ${project_name}.${table_name}_${org_id}
                where member_id is not null
                    and member_id <> ''
                ) bb
            where rid = 1
            ) b
            on a.member_id = b.member_id
        SET a.accu_id = b.accu_id
    	;"
    echo ''
    echo [UPDATE ${project_name}.${table_name}_${org_id} On Accu_id By Member_id]
    echo $sql
    #mysql --login-path=$dest_login_path -e "$sql" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${org_id}_sql.error   

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_org_id.txt
