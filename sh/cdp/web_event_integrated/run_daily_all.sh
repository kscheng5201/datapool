#!/usr/bin/bash

sh /root/datapool/sh/cdp/web_event_integrated/src_fpc_mapping_tmnewa.sh
sh /root/datapool/sh/cdp/web_event_integrated/src_web_event_tmnewa.sh
# sh /root/datapool/sh/cdp/web_event_integrated/full_sp_20210825.sh


# mysql --login-path=datapool -e "call web_event_integrated.sp_full_web_event_tmnewa;"

