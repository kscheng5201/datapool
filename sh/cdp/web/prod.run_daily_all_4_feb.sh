#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


echo `date` > /root/datapool/sh/cdp/web/time_4.txt


echo 4 > /root/datapool/export_file/cdp/web/web.cdp_prod_org_id_4.txt

sh /root/datapool/sh/cdp/web/prod.session.both_4.sh ${vDate}
sh /root/datapool/sh/cdp/web/prod.session.kpi_4_feb.sh ${vDate}
sh /root/datapool/sh/cdp/web/prod.both.onliner_4_feb.sh ${vDate}
sh /root/datapool/sh/cdp/web/prod.person.page_4_feb.sh ${vDate}
sh /root/datapool/sh/cdp/web/prod.session.page_4_feb.sh ${vDate}
sh /root/datapool/sh/cdp/web/prod.both.event_4_feb.sh ${vDate}

echo `date` >> /root/datapool/sh/cdp/web/time_4.txt
