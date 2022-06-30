#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


sh org_id.sh  
sh fpc_event_raw_data.sh ${vDate}
sh fpc_raw_data.sh ${vDate}
sh member_unique_data.sh ${vDate}
sh rfm_streaming.sh
