#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

sh /root/datapool/sh/cdp/web/org_id.sh
sh /root/datapool/sh/cdp/web/session.both.sh ${vDate}

# copy data for Pei
mysql --login-path=datapool -e "INSERT INTO ad.session_both_3_src_hist select null, date(created_at) + interval 1 day,fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_3_src;"
mysql --login-path=datapool -e "INSERT INTO ad.session_both_3_etl_hist select null, date(created_at) + interval 1 day,fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_3_etl;"

sh /root/datapool/sh/cdp/web/session.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/web/session.page.sh ${vDate}

sh /root/datapool/sh/cdp/web/both.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/web/person.page.sh ${vDate}

sh /root/datapool/sh/cdp/web/both.event.sh ${vDate}

# 假資料，先維持舊資料使用
#sh /root/datapool/sh/cdp/web/renew.funnel_config.sh
# 先用新安東京海上的資料來顯示，資料預存到 2022-01
#sh /root/datapool/sh/cdp/web/session.funnel.sh ${vDate}

#sh /root/datapool/sh/cdp/web/update.time_format.sh ${vDate}
