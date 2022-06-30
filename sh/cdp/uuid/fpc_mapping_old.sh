#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="tracker"
export table_name="cdp_fpc_mapping"
export fake_login_path="cdp"

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

export sql_query_1="
    select 
        fpc, 
        title origin_fpc, 
        metaKeyword domain, 
        min(datetime) first_time
    from tracker.landing2_mapping
    where datetime >= date('${vDate}' + interval 0 day)
        and datetime < date('${vDate}' + interval 0+1 day)  
        and metakeyword in (
        'www.accuhit.net',
        'blog.accuhit.net',
        'www.syntrend.com.tw',
        'www.coway-tw.com',
        'www.gewei-tw.com',
        'b2c.tmnewa.com.tw',
        'www.tmnewa.com.tw',
        'yahoo.ebo.tmnewa.com.tw',
        'www.rakuten.com.tw',
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
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$fake_login_path/$project_name/$project_name.${table_name}.txt 2>>$error_dir/$fake_login_path/$project_name/$project_name.${table_name}.error

# Import Data
echo ''
echo 'start: ' `date`
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$fake_login_path/$project_name/$project_name.${table_name}.txt' IGNORE INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$fake_login_path/$project_name/$project_name.${table_name}.error 

echo ''
echo 'end: ' `date`
