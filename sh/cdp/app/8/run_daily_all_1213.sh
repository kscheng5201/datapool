#!/etc/bin/bash
#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#sh /root/datapool/sh/cdp/app/8/app.org_id.sh ${vDate}
sh /root/datapool/sh/cdp/app/8/app.session.sh ${vDate}
sh /root/datapool/sh/cdp/app/8/app.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/app/8/app.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/app/8/app.funnel_1210.sh ${vDate}
sh /root/datapool/sh/cdp/app/8/app.event.sh ${vDate}
# event 執行完就會把大表刪除

#sh /root/datapool/sh/cdp/app/8/nes_model/run_daily_all.sh ${vDate}
#sh /root/datapool/sh/cdp/app/8/update.time_format.sh ${vDate}


# copy org 11 to org 3
# copy org 11 to org 3
mysql --login-path=datapool -e "truncate table app.person_event_3; insert into app.person_event_3 select * from app.person_event_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.person_onliner_3; insert into app.person_onliner_3 select * from app.person_onliner_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.session_event_3; insert into app.session_event_3 select * from app.session_event_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.session_funnel_3; insert into app.session_funnel_3 select * from app.session_funnel_11 where tag_date = '${vDate}' + interval 1 day; update app.session_funnel_3 set funnel_id = funnel_id / 100 where funnel_id in (10100, 38000);"
mysql --login-path=datapool -e "truncate table app.session_kpi_3; insert into app.session_kpi_3 select * from app.session_kpi_11 where time_flag = 'last';"

mysql --login-path=datapool -e "truncate table app.session_kpi_3_graph; insert into app.session_kpi_3_graph select * from app.session_kpi_11_graph;"
mysql --login-path=datapool -e "truncate table app.session_onliner_3; insert into app.session_onliner_3 select * from app.session_onliner_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.nes_3; insert into app.nes_3 SELECT * FROM app.nes_11; delete from app.nes_3 where end_date = '${vDate}'; update app.nes_3 set start_date = '${vDate}' - interval 90 day, end_date = '${vDate}' where serial = 1;"





