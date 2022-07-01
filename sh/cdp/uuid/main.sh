#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


# 建立資料夾路徑
mkdir -p /root/datapool/sh/cdp/uuid
mkdir -p /root/datapool/error_log/cdp/uuid
mkdir -p /root/datapool/export_file/cdp/uuid

# 清空所屬 error_log 以及 export_file
rm -rf /root/datapool/error_log/cdp/uuid/*
rm -rf /root/datapool/export_file/cdp/uuid/*

# 取得所有 org_id
sh /root/datapool/sh/cdp/uuid/org_id.sh ${vDate}
sleep 1s
# 從 tracker 1.0 拿到 fpc 與 browser_fpc 的比對資料(理想上應該已經再無更新資料)
sh /root/datapool/sh/cdp/uuid/prod.fpc_mapping.sh ${vDate}
sleep 1s
# 從 tracker 2.0 拿到 fpc 與 browser_fpc 的比對資料
sh /root/datapool/sh/cdp/uuid/tracker2.prod.cdp_fpc_mapping.sh ${vDate}


## accu_id 建立邏輯
# 處理 fpc mapping
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.fpc.sh ${vDate}
# 處理 app mapping
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.app.sh ${vDate}
# 處理 crm mapping
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.crm.sh ${vDate}
# 處理 lineToken mapping
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.lineTK.sh ${vDate}
# 處理 memberId 整合
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.member.sh
# 處理 跨組織的 browser_fpc mapping
sh /root/datapool/sh/cdp/uuid/prod.accu_mapping.over.sh ${vDate}


# 送紀錄到 data_pipeline 
sh /root/datapool/sh/cdp/uuid/prod.data_pipeline.sh ${vDate}

# 讀取 error_log 中的訊息，送到 JanDi
sh /root/datapool/sh/cdp/uuid/prod.error.send.sh
