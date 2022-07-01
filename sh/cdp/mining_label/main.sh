#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


# 建立資料夾路徑
mkdir -p /root/datapool/sh/cdp/mining_label
mkdir -p /root/datapool/error_log/cdp/mining_label
mkdir -p /root/datapool/export_file/cdp/mining_label


sh org_id.sh  
sh fpc_event_raw_data.sh ${vDate}
sh fpc_raw_data.sh ${vDate}
sh member_unique_data.sh ${vDate}
sh rfm_streaming.sh
