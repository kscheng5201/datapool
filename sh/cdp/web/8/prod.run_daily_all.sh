#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

echo `date`
sh /root/datapool/sh/cdp/web/8/prod.session.both.sh ${vDate}
sh /root/datapool/sh/cdp/web/8/prod.session.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/web/8/prod.session.page_1126.sh ${vDate}
sh /root/datapool/sh/cdp/web/8/prod.both.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/web/8/prod.person.page_1126.sh ${vDate}
sh /root/datapool/sh/cdp/web/8/prod.both.event.sh ${vDate}
sh /root/datapool/sh/cdp/web/8/prod.session.funnel.sh ${vDate}
echo `date`
