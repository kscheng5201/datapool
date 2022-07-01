#!/bin/bash

# 建立資料夾路徑
mkdir -p /root/datapool/sh/cdp/codebook_cdp
mkdir -p /root/datapool/error_log/cdp/codebook_cdp
mkdir -p /root/datapool/export_file/cdp/codebook_cdp


sh /root/datapool/sh/cdp/codebook_cdp/prod.organization.sh
sh /root/datapool/sh/cdp/codebook_cdp/prod.organization_domain.sh
sh /root/datapool/sh/cdp/codebook_cdp/prod.events_main.sh
sh /root/datapool/sh/cdp/codebook_cdp/prod.events_function.sh