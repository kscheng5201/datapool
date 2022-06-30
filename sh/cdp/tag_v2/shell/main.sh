#!/usr/bin/bash
####################################################
# Project: 觸發型標籤
# Branch: 原始上游
# Author: Benson Cheng
# Created_at: 2022-04-29
# Updated_at: 2022-04-29
# Note: 
#####################################################

## ALL path directory
export dir=$(pwd)
log_dir=$(dirname $dir)/log
uti_dir=$dir/utilities

## Source the needs
source $uti_dir/common.sh


## Name the parameter
dest_login_path=datapool_prod
src_login_path=cdp
project_name=tag_v2
tag_type=trigger

echo $dest_login_path
echo $src_login_path
echo $project_name
echo $tag_type

all_org=`get_org_id`
echo $all_org

while read -r org_id
do 
	echo ''
	echo $org_id
done <<< "$all_org" 



#mysql --login-path=datapool -e "select now();"
