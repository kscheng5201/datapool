#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


# 清空所屬 error_log 以及 export_file
rm -rf /root/datapool/error_log/cdp/uuid/*
rm -rf /root/datapool/export_file/cdp/uuid/*


sh /root/datapool/sh/cdp/uuid/org_id.sh ${vDate}
sleep 1s
sh /root/datapool/sh/cdp/uuid/fpc_mapping.sh ${vDate}
sleep 1s
sh /root/datapool/sh/cdp/uuid/tracker2.cdp_fpc_mapping.sh ${vDate}

sleep 1s
sh /root/datapool/sh/cdp/uuid/prod.fpc_mapping.sh ${vDate}
sleep 1s
sh /root/datapool/sh/cdp/uuid/tracker2.prod.cdp_fpc_mapping.sh ${vDate}


# accu_id 建立邏輯
#sh /root/datapool/sh/cdp/uuid/accu_mapping.fpc.each.sh ${vDate}
#sh /root/datapool/sh/cdp/uuid/accu_mapping.crm.each.sh ${vDate}
#sh /root/datapool/sh/cdp/uuid/accu_mapping.over.sh ${vDate}

sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.fpc.sh ${vDate}
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.app.sh ${vDate}
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.crm.sh ${vDate}
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.lineTK.sh ${vDate}
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.member.sh
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.over.sh ${vDate}


# 送紀錄到 data_pipeline 
sh /root/datapool/sh/cdp/uuid/prod.data_pipeline.sh ${vDate}

# 讀取 error_log 中的訊息，送到 JanDi
sh /root/datapool/sh/cdp/uuid/prod.error.send.sh
