#!/usr/bin/bash
####################################################
# Project: accu_id 更新，每日程式監控
# Branch: 一切都是為了 accu_id 更新
# Author: Benson Cheng
# Created_at: 2022-02-09
# Updated_at: 2022-02-09
####################################################

export export_dir="/root/datapool/export_file/cdp"
export error_dir="/root/datapool/error_log/cdp"


# 刪除空白的 error_log: uuid
find ${error_dir}/uuid/ -empty -type f -delete
# 寫入有內容的 error_log 名: uuid
ls ${error_dir}/uuid/ > ${export_dir}/uuid/error_list.txt

echo [print 現有的 error_log 名]
cat ${export_dir}/uuid/error_list.txt

## 發送完成訊息到 JanDi
if [ -s ${export_dir}/uuid/error_list.txt ]; 
then 
    while read error_list;
    do 
        # personal use
        /root/anaconda3/bin/python /root/LogReport.py 'error' ${error_list} ${error_dir}/uuid/$error_list "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
        
        # public use
        /root/anaconda3/bin/python /root/LogReport.py 'error' '執行錯誤--愛酷ID (fpc & crm)' ${error_dir}/uuid/$error_list "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
    done < ${export_dir}/uuid/error_list.txt

else
    # 小房間
    /root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— 愛酷ID (fpc & crm)' '' "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
    # public room
    /root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— 愛酷ID (fpc & crm)' '' "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
fi

    # public room
    /root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— 愛酷ID (fpc & crm)' '' "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"


# 清空所有 error_log
rm -rf ${error_dir}/uuid/*
