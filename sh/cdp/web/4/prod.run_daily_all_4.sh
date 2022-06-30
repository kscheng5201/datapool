#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


echo `date` > /root/datapool/sh/cdp/web/4/time_4.txt

# 清空所屬 error_log 以及 export_file
#rm -rf /root/datapool/error_log/cdp/web/4/*
#rm -rf /root/datapool/export_file/cdp/web/4/*

echo 4 > /root/datapool/export_file/cdp/web/web.cdp_prod_org_id_4.txt

mysql --login-path=datapool_prod -e "UPDATE web.session_funnel_4 SET tag_date = date(now()) WHERE tag_date = date(now() - interval 1 day) and span = '90 days';"
sh /root/datapool/sh/cdp/web/4/prod.session.both_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/4/prod.session.kpi_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/4/prod.session.page_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/4/prod.both.onliner_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/4/prod.person.page_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/4/prod.both.event_4.sh ${vDate}

sh /root/datapool/sh/cdp/web/4/prod.session.funnel_4.sh ${vDate}


## 發送訊息到 JanDi
# 小房間
/root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— CDP/ 互動行為分析/ Web' '' "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
# public room
/root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— CDP/ 互動行為分析/ Web' '' "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"


# 刪除 error_log 中的空白檔案
find /root/datapool/error_log/cdp/web/4/ -empty -type f -delete

# 讀取 error_log 中的訊息，送到 JanDi
sh /root/datapool/sh/cdp/web/4/prod.error.send.sh


echo `date` >> /root/datapool/sh/cdp/web/4/time_4.txt
