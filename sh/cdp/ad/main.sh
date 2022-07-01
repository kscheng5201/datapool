#!/usr/bin/bash
####################################################
# Project: ad effect analysis 廣告成效分析
# Branch: ALL JOB on cron
# Author: Benson Cheng
# Created_at: 2022-01-07
# Updated_at: 2022-01-13
# Note: 
#####################################################

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


# 建立資料夾路徑
mkdir -p /root/datapool/sh/cdp/ad
mkdir -p /root/datapool/error_log/cdp/ad
mkdir -p /root/datapool/export_file/cdp/ad

# 清空所屬 error_log 以及 export_file
rm -rf /root/datapool/error_log/cdp/ad/*
rm -rf /root/datapool/export_file/cdp/ad/*


# org_id, campaign, utm and each period
sh /root/datapool/sh/cdp/ad/ad.org_id.sh ${vDate}

# CDP 原始資料
sh /root/datapool/sh/cdp/ad/ad.session.both.sh ${vDate}

# 重要指標統計
sh /root/datapool/sh/cdp/ad/ad.session.kpi.sh ${vDate}

# 趨勢圖
sh /root/datapool/sh/cdp/ad/ad.session.graph.sh ${vDate}

# 進站前路徑分析: 流量種類分佈
sh /root/datapool/sh/cdp/ad/ad.both.landing.sh ${vDate}
# 進站前路徑分析: 路徑細節
sh /root/datapool/sh/cdp/ad/ad.both.traffic.sh ${vDate}

# 進站後路徑分析：所有頁面瀏覽
sh /root/datapool/sh/cdp/ad/ad.session.title_0105.sh ${vDate}

# 事件分析
sh /root/datapool/sh/cdp/ad/ad.both.event.sh ${vDate}

# 行銷漏斗: 消費漏斗
sh /root/datapool/sh/cdp/ad/ad.session.funnel.b.sh ${vDate}
# 行銷漏斗: 客製漏斗
sh /root/datapool/sh/cdp/ad/ad.session.funnel.c.sh ${vDate}


# 刪除 error_log 中的空白檔案
find /root/datapool/error_log/cdp/ad/ -empty -type f -delete
