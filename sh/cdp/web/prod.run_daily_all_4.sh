#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


echo `date` > /root/datapool/sh/cdp/web/time_4.txt


echo 4 > /root/datapool/export_file/cdp/web/web.cdp_prod_org_id_4.txt

#mysql --login-path=datapool_prod -e "UPDATE web.session_funnel_4 SET tag_date = date(now()) WHERE tag_date = date(now() - interval 1 day) and span = '90 days';"
sh /root/datapool/sh/cdp/web/prod.session.both_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.session.kpi_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.session.page_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.both.onliner_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.person.page_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.both.event_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.event_raw_data_4.sh

sh /root/datapool/sh/cdp/web/prod.session.funnel_4.sh ${vDate}


echo `date` >> /root/datapool/sh/cdp/web/time_4.txt
