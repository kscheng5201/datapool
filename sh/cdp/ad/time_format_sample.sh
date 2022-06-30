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
    while read campaign_detail;
    do
	campaign_start=`(date -d $(echo ${campaign_detail} | cut -d _ -f 2) +"%Y%m%d")`
	campaign_end=`(date -d $(echo ${campaign_detail} | cut -d _ -f 3) +"%Y%m%d")`
	echo ''
	echo campaign_id = $(echo ${campaign_detail} | cut -d _ -f 1)
	echo campaign_start = $campaign_start
        echo campaign_end = $campaign_end

        if [ ${nvDate} -lt ${campaign_end} ];
        then 
		echo 'yes'
        else
		echo 'no'
        fi


    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt
    cat $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_campaign_detail.txt

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt





