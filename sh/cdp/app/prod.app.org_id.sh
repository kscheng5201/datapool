#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
export table_name="org_id" 
export src_login_path="cdp"
export src_login_true="cdp"

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


#### Get the org_id ####
export sql_0="
    select id
    from cdp_organization.organization
    ;"    
echo ''
echo [Get the org_id]
mysql --login-path=$src_login_true -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_${table_name}.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_${table_name}.txt

echo [the directory]
echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_${table_name}.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_prod_${table_name}.txt
echo [end at `date`]
