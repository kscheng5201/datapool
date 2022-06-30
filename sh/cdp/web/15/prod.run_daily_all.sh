#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

sh /root/datapool/sh/cdp/web/15/prod.session.both.sh ${vDate}

#sh /root/datapool/sh/cdp/web/15/prod.session.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/web/15/prod.session.page_1228.sh ${vDate}

#sh /root/datapool/sh/cdp/web/15/prod.both.onliner.sh ${vDate}
#sh /root/datapool/sh/cdp/web/15/prod.person.page_1228.sh ${vDate}

#sh /root/datapool/sh/cdp/web/15/prod.both.event.sh ${vDate}

#sh /root/datapool/sh/cdp/web/15/prod.session.funnel.sh ${vDate}

