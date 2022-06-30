# 19700101 - 20200101


#sh /root/datapool/sh/cdp/uuid/accu_mapping.each_1970.sh
#sh /root/datapool/sh/cdp/uuid/accu_mapping.over.sh

#sh /root/datapool/sh/cdp/uuid/accu_mapping.each_2000.sh
#sh /root/datapool/sh/cdp/uuid/accu_mapping.over.sh


#!/usr/bin/bash
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

sh /root/datapool/sh/cdp/uuid/accu_mapping.each.sh ${vDate}
sh /root/datapool/sh/cdp/uuid/accu_mapping.over.sh ${vDate}
