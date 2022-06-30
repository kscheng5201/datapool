#!/etc/bin/bash
#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#sh /root/datapool/sh/cdp/app/prod.app.org_id.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.session.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.event.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.funnel.sh ${vDate}
#sh /root/datapool/sh/cdp/app/prod.update.time_format.sh ${vDate}

# NES model
#sh /root/datapool/sh/cdp/app/nes_model/run_daily_all.sh

