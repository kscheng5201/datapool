#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 基本資訊（org_id, campaign_detail, utm_detail）
# Created_at: 2022-01-04
# Updated_at: 2022-01-04
# Note: 
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="ad"
export table_name="org_id" 
export src_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
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
    vMonthFirst=`date -d $1 +"%Y%m01"`
    vMonthLast=`date -d "${vMonthFirst} +1 month -1 day" +"%Y%m%d"`
else 
    vMonthFirst=`date -d "1 day ago" +"%Y%m01"` 
    vMonthLast=`date -d "-$(date +%d) days +1 month" +"%Y%m%d"`
fi


#### Get the org_id ####
export sql_0="
    select org_id
    from cdp_organization.organization_domain
    where domain_type = 'web'
        and org_id = 3
    group by org_id
    ;"    
echo ''
echo [Get the org_id]
echo $sql_0
mysql --login-path=$src_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.txt

echo [the directory]
echo $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.txt
cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.txt


while read org_id; 
do
    export sql_2="
        select concat_ws('_', campaign_id, utm_id, replace(utm_start, '-', ''), replace(utm_end, '-', '')) utm_detail
        from ${project_name}.campaign_utm_${org_id}
        where utm_start <= '${vDate}'
            and utm_end>= '${vDate}'
        group by campaign_id, utm_id, replace(utm_start, '-', ''), replace(utm_end, '-', '')
        ;"
    echo ''
    echo [Get the utm_detail on web]
    echo $sql_2
    mysql --login-path=${dest_login_path} -e "$sql_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_utm_detail.txt

    export sql_3="
        select concat_ws('_', campaign_id, replace(campaign_start, '-', ''), replace(campaign_end, '-', '')) campaign_detail
        from ${project_name}.campaign_utm_${org_id}
        where campaign_start <= '${vDate}'
            and campaign_end >= '${vDate}'
        group by campaign_id
        ;"
    echo ''
    echo [Get the campaign_detail on web]
    echo $sql_3
    mysql --login-path=${dest_login_path} -e "$sql_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt

    export sql_4="
        select concat_ws('_', campaign_id, replace(campaign_start, '-', ''), replace(campaign_end, '-', ''), utm_id, replace(utm_start, '-', ''), replace(utm_end, '-', '')) campaign_utm
        from ${project_name}.campaign_utm_${org_id}
        where campaign_start <= '${vDate}'
            and campaign_end >= '${vDate}'
        group by campaign_id, replace(campaign_start, '-', ''), replace(campaign_end, '-', ''), utm_id, replace(utm_start, '-', ''), replace(utm_end, '-', '')
        ;"
    echo ''
    echo [Get the campaign_utm on web]
    echo $sql_4
    mysql --login-path=${dest_login_path} -e "$sql_4" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_utm.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.txt
echo [end at `date`]
