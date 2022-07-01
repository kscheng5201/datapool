#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

# 建立資料夾路徑
mkdir -p /root/datapool/sh/cdp/web
mkdir -p /root/datapool/error_log/cdp/web
mkdir -p /root/datapool/export_file/cdp/web


# 清空所屬 error_log 以及 export_file
rm -rf /root/datapool/error_log/cdp/web/*
#rm -rf /root/datapool/export_file/cdp/web/*

# 全部廠商
sh /root/datapool/sh/cdp/web/prod.org_id.sh
sh /root/datapool/sh/cdp/web/prod.monitor.sh ${vDate}

# 排除PChome與新安東京海上
sh /root/datapool/sh/cdp/web/prod.org_id_n4.sh

sh /root/datapool/sh/cdp/web/prod.session.both.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.session.kpi.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.session.page.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.both.onliner.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.person.page.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.both.event.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.event_raw_data.sh

sh /root/datapool/sh/cdp/web/prod.renew.funnel_config.sh

sh /root/datapool/sh/cdp/web/prod.session.funnel.sh ${vDate}

sh /root/datapool/sh/cdp/web/prod.update.time_format.sh ${vDate}

# org_id = 4 ONLY
sh /root/datapool/sh/cdp/web/prod.run_daily_all_4.sh ${vDate}

# 紀錄全部廠商 session_both_etl 已完成
sh /root/datapool/sh/cdp/web/prod.org_id.sh
sh /root/datapool/sh/cdp/web/prod.data_pipeline.sh ${vDate}


# 刪除 error_log 中的空白檔案
find /root/datapool/error_log/cdp/web/ -empty -type f -delete

# 讀取 error_log 中的訊息，送到 JanDi
sh /root/datapool/sh/cdp/web/prod.error.send.sh
