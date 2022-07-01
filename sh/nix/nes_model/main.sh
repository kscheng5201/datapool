#!/usr/bin/python

/root/anaconda3/bin/python /root/datapool/sh/nix/nes_model/nix_check_have_db_or_not.py 2>>/root/datapool/error_log/nix/nes_model/nix_check_have_db_or_not.error 
/root/anaconda3/bin/python /root/datapool/sh/nix/nes_model/nix_table_NES_Model_daily.py 2>>/root/datapool/error_log/nix/nes_model/nix_table_NES_Model_daily.error 

sh /root/datapool/sh/nix/nes_model/nix_NES_Model_prop_updated.sh 2>>/root/datapool/error_log/nix/nes_model/nix_NES_Model_prop_updated.error 

