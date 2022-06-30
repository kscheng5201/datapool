#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="fpc"
export src_login_path="tracker"
export table_name="cdp_fpc_mapping"


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

for i in $(seq 1 7)
# for i in 0
do

export sql_query_1="
    select 
        fpc, 
        title origin_fpc, 
        metaKeyword domain, 
        min(datetime) first_time
    from tracker.landing2_mapping
    where datetime >= date('${vDate}' + interval ${i} day)
        and datetime < date('${vDate}' + interval ${i}+1 day)  
        and metakeyword in (
        'www.accuhit.net',
        'blog.accuhit.net',
        'www.syntrend.com.tw',
        'www.coway-tw.com',
        'www.gewei-tw.com',
        'b2c.tmnewa.com.tw',
        'www.tmnewa.com.tw',
        'yahoo.ebo.tmnewa.com.tw',
        'www.rakuten.com.tw/insurance',
        'homeplusone.com.tw',
        'www.store-philips.tw',
        'events.lifestyler.com.tw',
        'www.lifestyler.com.tw',
        'crm.lifestyler.com.tw',
        'campaign.lifestyler.com.tw',
        'www.philips.com.tw',
        'bhouse.com.tw',
        'ocard.co',
        'ecvip.pchome.com.tw',
        'www.feds.com.tw',
        'www.sogo.com.tw',
        'www.febigcity.com',
        'www.fe-amart.com.tw',
        'www.citysuper.com.tw',
        'shopping.friday.tw'
        )
    group by 
        fpc, 
        title, 
        metaKeyword
    ;"



# Export Data
echo ''
echo 'start: ' `date`
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.error

# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.txt' IGNORE INTO TABLE ${src_login_path}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/${src_login_path}.${table_name}.error 
done

echo ''
echo 'end: ' `date`
