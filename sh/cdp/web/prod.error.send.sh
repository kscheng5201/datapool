#!/usr/bin/bash
####################################################
# Project: Web 儀表板，每日程式監控
# Branch: 包含 NES Model
# Author: Benson Cheng
# Created_at: 2022-02-09
# Updated_at: 2022-02-09
####################################################

export export_dir="/root/datapool/export_file/cdp"
export error_dir="/root/datapool/error_log/cdp"


# 刪除空白的 error_log: web
find ${error_dir}/web/ -empty -type f -delete
# 寫入有內容的 error_log 名: web
ls ${error_dir}/web/ > ${export_dir}/web_error_list.txt
ls ${error_dir}/web/ > ${export_dir}/error_list.txt

# 刪除空白的 error_log: nes_model
find ${error_dir}/nes_model/ -empty -type f -delete
# 寫入有內容的 error_log 名: nes_model
ls ${error_dir}/nes_model > ${export_dir}/nes_error_list.txt
ls ${error_dir}/nes_model >> ${export_dir}/error_list.txt

echo [print 現有的 error_log 名]
cat ${export_dir}/error_list.txt

## 發送完成訊息到 JanDi
if [ -s ${export_dir}/error_list.txt ]; 
then 
    while read web_error_list;
    do 
        # personal use
        /root/anaconda3/bin/python /root/LogReport.py 'error' ${web_error_list} ${error_dir}/web/$web_error_list "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
        
        # public use
        #/root/anaconda3/bin/python /root/LogReport.py 'error' '執行錯誤--WEB 儀表板' ${error_dir}/web/$web_error_list "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
    done < ${export_dir}/web_error_list.txt
    
    while read nes_error_list;
    do 
        # personal use
        /root/anaconda3/bin/python /root/LogReport.py 'error' ${nes_error_list} ${error_dir}/nes_model/$nes_error_list "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
        
        # public use
        #/root/anaconda3/bin/python /root/LogReport.py 'error' '執行錯誤--NES Model' ${error_dir}/nes_model/$nes_error_list "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
    done < ${export_dir}/nes_error_list.txt
else
    # 小房間
    /root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— CDP/ 互動行為分析/ Web（含NES Model）' '' "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
    # public room
    #/root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— CDP/ 互動行為分析/ Web（含NES Model）' '' "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
fi

    /root/anaconda3/bin/python /root/LogReport.py 'info' '執行完畢— CDP/ 互動行為分析/ Web（含NES Model）' '' "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"

# 清空所有 error_log
rm -rf ${error_dir}/web/*
rm -rf ${error_dir}/nes_model/*
