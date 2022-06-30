#!/usr/bin/python
 
/root/anaconda3/bin/python /root/datapool/sh/cdp/nes_model/CDP_create_NES_table123_Daily.py 2>> /root/datapool/error_log/cdp/nes_model/CDP_create_NES_table123_Daily.error 

# update the 5 prop columns
sh /root/datapool/sh/cdp/nes_model/prop_update.sh
