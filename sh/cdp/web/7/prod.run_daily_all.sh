#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

sh /root/datapool/sh/cdp/web/7/prod.session.both.sh ${vDate}

#sh /root/datapool/sh/cdp/web/7/prod.session.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/web/7/prod.session.page_1228.sh ${vDate}

#sh /root/datapool/sh/cdp/web/7/prod.both.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/web/7/prod.person.page_1228.sh ${vDate}

#sh /root/datapool/sh/cdp/web/7/prod.both.event.sh ${vDate}

#sh /root/datapool/sh/cdp/web/7/prod.session.funnel.sh ${vDate}


# copy data from org 4 to org 1
#mysql --login-path=datapool_prod -e "truncate table web.person_event_1; insert into web.person_event_1 select * from web.person_event_4 where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_onliner_1; select * from web.person_onliner_4 where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_page_1_campaign; insert into web.person_page_1_campaign select * from web.person_page_4_campaign where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_page_1_domain; insert into web.person_page_1_domain select * from web.person_page_4_domain where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_page_1_landing; insert into web.person_page_1_landing select * from web.person_page_4_landing where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_page_1_medium; insert into web.person_page_1_medium select * from web.person_page_4_medium where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_page_1_title; insert into web.person_page_1_title select * from web.person_page_4_title where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.person_page_1_traffic; insert into web.person_page_1_traffic select * from web.person_page_4_traffic where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_event_1; insert into web.session_event_1 select * from web.session_event_4 where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_kpi_1; insert into web.session_kpi_1 select * from web.session_kpi_4 where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_kpi_1_graph; insert into web.session_kpi_1_graph select * from web.session_kpi_4_graph where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_onliner_1; insert into web.session_onliner_1 select * from web.session_onliner_4 where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_page_1_campaign; insert into web.session_page_1_campaign select * from web.session_page_4_campaign where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_page_1_domain; insert into web.session_page_1_domain select * from web.session_page_4_domain where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_page_1_landing; insert into web.session_page_1_landing select * from web.session_page_4_landing where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_page_1_medium; insert into web.session_page_1_medium select * from web.session_page_4_medium where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_page_1_title; insert into web.session_page_1_title select * from web.session_page_4_title where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_page_1_traffic; insert into web.session_page_1_traffic select * from web.session_page_4_traffic where time_flag is not null;"
#mysql --login-path=datapool_prod -e "truncate table web.session_funnel_1; insert into web.session_funnel_1 select null, tag_date, span, start_date, end_date, 1, funnel_name, layer_id, layer_name, user, conversion_rate, conversion_overall, created_at, updated_at from web.session_funnel_4 where tag_date = '${vDate}' and funnel_id = 4;"

