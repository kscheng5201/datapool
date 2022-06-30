#!/usr/bin/bash


# New triggered tag System
sh /root/datapool/sh/cdp/tag/triggered.org_id.sh	
sh /root/datapool/sh/cdp/tag/triggered.etl.sh

# Old triggered tag System
sh /root/datapool/sh/cdp/tag/fpc_unique.sh
sh /root/datapool/sh/cdp/tag/user_tag_src.sh
sh /root/datapool/sh/cdp/tag/user_tag_final.sh
sh /root/datapool/sh/cdp/tag/user_tag_prod.sh
