#!/bin/sh


# 建立資料夾路徑
mkdir -p /root/datapool/sh/cdp/line
mkdir -p /root/datapool/error_log/cdp/line
mkdir -p /root/datapool/export_file/cdp/line

sh /root/datapool/sh/cdp/line/line.org_id.sh
sh /root/datapool/sh/cdp/line/line.user.sh

