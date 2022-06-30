#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


sh prod.shawn.app.session.sh ${vDate}
sh prod.shawn.app.event.sh ${vDate}
#sh prod.event_raw_data.sh
