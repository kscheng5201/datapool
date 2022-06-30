
ls /root/datapool/error_log/cdp/web/ > error_list.txt
cat error_list.txt

while read error_list;
do 
    # personal use
    /root/anaconda3/bin/python /root/LogReport.py 'error' '執行錯誤--WEB 儀表板' /root/datapool/error_log/cdp/web/$error_list "https://wh.jandi.com/connect-api/webhook/24388692/ee2e4b5b0c9c253124e25736bb7b89ac"
    
    # public use
    /root/anaconda3/bin/python /root/LogReport.py 'error' '執行錯誤--WEB 儀表板' /root/datapool/error_log/cdp/web/$error_list "https://wh.jandi.com/connect-api/webhook/24388692/0519e98616dc494e32fd508ea062491b"
done < /root/datapool/sh/cdp/web/error_list.txt

