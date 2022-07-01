#!/usr/bin/python


# 建立資料夾路徑
mkdir -p /root/datapool/sh/nix/nes_model
mkdir -p /root/datapool/export_file/nix/nes_model
mkdir -p /root/datapool/error_log/nix/nes_model


/root/anaconda3/bin/python /root/datapool/sh/nix/nes_model/nix_check_have_db_or_not.py 2>>/root/datapool/error_log/nix/nes_model/nix_check_have_db_or_not.error 
/root/anaconda3/bin/python /root/datapool/sh/nix/nes_model/nix_table_NES_Model_daily.py 2>>/root/datapool/error_log/nix/nes_model/nix_table_NES_Model_daily.error 

sh /root/datapool/sh/nix/nes_model/nix_NES_Model_prop_updated.sh 2>>/root/datapool/error_log/nix/nes_model/nix_NES_Model_prop_updated.error 

