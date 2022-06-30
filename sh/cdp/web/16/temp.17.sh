
sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211220 
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 

sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211221
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 

sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211222 
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 

sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211223
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 

sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211224
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 

sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211225 
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 

sh /root/datapool/sh/cdp/web/16/prod.session.both.sh 20211226 
mysql --login-path=datapool -e "insert into web.session_both_17_etl_hist select null, fpc, created_at, domain, behavior, traffic_type, referrer, campaign, source_medium, event_type, page_title, page_url, session, updated_at from web.session_both_17_etl;" 
