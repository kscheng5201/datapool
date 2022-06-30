#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


sh /root/datapool/sh/cdp/web_event/session.both_new.sh ${vDate}

sh /root/datapool/sh/cdp/web_event/session.kpi_new4.sh ${vDate}
sh /root/datapool/sh/cdp/web_event/session.page_new3.sh ${vDate}

sh /root/datapool/sh/cdp/web_event/person.onliner_new3.sh ${vDate}
sh /root/datapool/sh/cdp/web_event/person.page_new4.sh ${vDate}

sh /root/datapool/sh/cdp/web_event/both.event.sh ${vDate}
sh /root/datapool/sh/cdp/web_event/session.funnel.sh ${vDate}
