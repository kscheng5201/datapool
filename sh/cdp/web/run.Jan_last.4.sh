#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=$1
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


#sh /root/datapool/sh/cdp/web/session.both.sh 20220126
#sh /root/datapool/sh/cdp/web/session.kpi.sh 20220126

#sh /root/datapool/sh/cdp/web/session.both.sh 20220127
#sh /root/datapool/sh/cdp/web/session.kpi.sh 20220127

#sh /root/datapool/sh/cdp/web/session.both.sh 20220128
#sh /root/datapool/sh/cdp/web/session.kpi.sh 20220128

sh /root/datapool/sh/cdp/web/session.both.sh 20220129
sh /root/datapool/sh/cdp/web/session.kpi.sh 20220129

#sh /root/datapool/sh/cdp/web/session.both.sh 20220130
#sh /root/datapool/sh/cdp/web/session.kpi.sh 20220130

#sh /root/datapool/sh/cdp/web/session.both.sh 20220131
#sh /root/datapool/sh/cdp/web/session.kpi.sh 20220131


