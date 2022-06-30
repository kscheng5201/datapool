#!/etc/bin/bash
#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi

# 清空所屬 error_log 以及 export_file
rm -rf /root/datapool/error_log/cdp/app/*
rm -rf /root/datapool/export_file/cdp/app/*

sh /root/datapool/sh/cdp/app/prod.app.org_id.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.session.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.event_raw_data.sh
sh /root/datapool/sh/cdp/app/prod.app.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.event.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.funnel.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.update.time_format.sh ${vDate}
sh /root/datapool/sh/cdp/app/prod.app.data_pipeline.sh ${vDate}

# NES model
#sh /root/datapool/sh/cdp/app/nes_model/run_daily_all.sh

