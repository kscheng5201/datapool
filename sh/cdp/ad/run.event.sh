
#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: 
# Created_at: 2022-01-04
# Updated_at: 2022-01-04
# Note: 
#####################################################

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
    nvDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
    nvDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


sh /root/datapool/sh/cdp/ad/ad.org_id.sh ${vDate}
sh /root/datapool/sh/cdp/ad/ad.both.event.sh ${vDate}
