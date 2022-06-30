#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="app"
export type="session"
export table_name="funnel" 
export src_login_path="cdp"


#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi



sh /root/datapool/sh/cdp/app/app.org_id.sh ${vDate}
sh /root/datapool/sh/cdp/app/app.session.sh ${vDate}
sh /root/datapool/sh/cdp/app/app.kpi.sh ${vDate}
sh /root/datapool/sh/cdp/app/app.onliner.sh ${vDate}
sh /root/datapool/sh/cdp/app/app.funnel_1210.sh ${vDate}
sh /root/datapool/sh/cdp/app/app.event.sh ${vDate}
# event 執行完就會把大表刪除(這部分已不會刪除)

sh /root/datapool/sh/cdp/app/nes_model/run_daily_all.sh ${vDate}
sh /root/datapool/sh/cdp/app/update.time_format.sh ${vDate}


# copy org 11 to org 3
# copy org 11 to org 3
mysql --login-path=datapool -e "truncate table app.person_event_3; insert into app.person_event_3 select * from app.person_event_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.person_onliner_3; insert into app.person_onliner_3 select * from app.person_onliner_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.session_event_3; insert into app.session_event_3 select * from app.session_event_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.session_funnel_3; insert into app.session_funnel_3 select * from app.session_funnel_11 where tag_date = '${vDate}' + interval 1 day; update app.session_funnel_3 set funnel_id = funnel_id / 100; UPDATE codebook_cdp.funnel a INNER JOIN ( select * from codebook_cdp.funnel where channel = 'app' ) b ON a.channel = b.channel and a.name = b.name SET a.status = b.status WHERE a.org_id = 3;"
mysql --login-path=datapool -e "truncate table app.session_kpi_3; insert into app.session_kpi_3 select * from app.session_kpi_11 where time_flag = 'last';"

mysql --login-path=datapool -e "truncate table app.session_kpi_3_graph; insert into app.session_kpi_3_graph select * from app.session_kpi_11_graph;"
mysql --login-path=datapool -e "truncate table app.session_onliner_3; insert into app.session_onliner_3 select * from app.session_onliner_11 where time_flag = 'last';"
mysql --login-path=datapool -e "truncate table app.nes_3; insert into app.nes_3 SELECT * FROM app.nes_11; delete from app.nes_3 where end_date = '${vDate}'; update app.nes_3 set start_date = '${vDate}' - interval 90 day, end_date = '${vDate}' where serial = 1;"




    export sql_21="
        select concat_ws('-', org_id, id / 100, status) funnel_status
        from codebook_cdp.funnel
        where org_id = 11
            and channel = '${project_name}'
        ;"
    echo ''
    echo [Get the funnel_status FROM codebook_cdp.funnel]
    echo $sql_21
    mysql --login-path=$dest_login_path -e "$sql_21" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt


    while read funnel_status; 
    do 
        export sql_22="
            UPDATE cdp_data_team.funnel
            SET status = $(echo ${funnel_status} | cut -d - -f 3)
            WHERE org_id = 3
                and id = $(echo ${funnel_status} | cut -d - -f 2)
                and channel = '${project_name}' 
            ;"
        echo ''
        echo [UPDATE cdp_data_team.funnel at CDP prod]
	echo $sql_22
        mysql --login-path=cdp_dev -e "$sql_22" 2>>$error_dir/$src_login_path/$project_name/cdp_data_team.funnel.error
    
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_funnel_status.txt
