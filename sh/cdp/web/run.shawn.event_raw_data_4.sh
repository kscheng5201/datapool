#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


sh prod.shawn.session.both_4.sh ${vDate}
sh prod.shawn.both.event_4.sh ${vDate}
#sh prod.event_raw_data_4.sh ${vDate}
