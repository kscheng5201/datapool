#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="web_event_integrated"
export stakeholder="tmnewa" # 新安東京海上產險
export src_login_path='cdp'

# Before work
mysql --login-path=$dest_login_path -e "truncate table $project_name.src_web_event_${stakeholder}; "

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### loop by db_id ####
for i in 5 6 7 8
do 
	export sql_query_1="
        select 
            fpc, 
            source domain, 
            '網頁瀏覽' behavior, 
            '網頁瀏覽' type,   
            if(referrer is null or referrer = '', 'direct', 
                substring_index(substring_index(referrer, '//', -1), '/', 1)) referrer,
            case 
                when page_url like '%complete.aspx%'           	
			# 汽機車險
                    or page_url like '%#%'
                	# 住火險
                    or page_url like '%finish%'
	         	# 旅遊險、傷害險
                then 5
                when page_url like '%writedata.aspx%'		
			# 汽機車險
                    or page_url like '%house-fill-data.aspx%'	
			# 住火險
                    or page_url like '%start-apply%'
			# 旅遊險
                    or page_url like '%fill-in%'
			# 傷害險
                then 4
                when page_url like '%calculate.aspx%'		
			# 汽機車險
                    or page_url like '%house-build.aspx%'
			# 住火險
                    or page_url like '%travel/index%'
			# 旅遊險
                    or page_url like '%accident/index%'
			# 傷害險
                then 3
                when page_url like '%/product/%'
                then 2
                else 1
            end funnel_layer, 
            case 
                # each phone
                when user_agent like '%android%' and user_agent like '%mobile%' then 'mobile'
                when user_agent not like '%windows%' and user_agent like '%iphone%' then 'mobile'
                when user_agent like '%ipod%' then 'mobile'    
                when user_agent like '%windows%' and user_agent like '%phone%' then 'mobile'
                when user_agent like '%blackberry%' and user_agent not like '%tablet%' then 'mobile'    
                when user_agent like '%fxos%' and user_agent like '%mobile%' then 'mobile'        
                when user_agent like '%meego%' then 'mobile' 
                # each tablet
                when user_agent like '%ipad%' then 'tablet'      
                when user_agent like '%android%' and user_agent not like '%mobile%' then 'tablet'
                when user_agent like '%blackberry%' and user_agent like '%tablet%' then 'tablet'
                when user_agent like '%windows%' and (user_agent like '%touch%' and user_agent not like (user_agent like '%windows%' and user_agent like '%phone%')) then 'tablet'
                when user_agent like '%fxos%' and user_agent like '%tablet%' then 'tablet' 
                # desktop
                when user_agent not like '%tablet%' and user_agent not like '%mobile%' then 'desktop'
                else null
            end device_type,
            case 
                when user_agent REGEXP 'iphone|ipad|ipod' then 'ios'
                when user_agent like '%android%' and user_agent not like '%windows%' then 'android'
                when user_agent like '%blackberry%' or user_agent like '%bb10%' then 'blackberry'
                when user_agent like '%mac%' then 'macos'
                when user_agent like '%windows%' then 'windows'    
                when (user_agent REGEXP 'mobile|tablet') and user_agent like '% rv:%' then 'fxos'
                when user_agent like '%meego%' then 'meego'                
                when user_agent like '%television%' then 'television'  
                else 'others'
            end device_os,    
            page_url, 
            from_unixtime(created_at, '%Y-%m-%d %H:%m:%s') + interval 8 hour create_time
        from cdp_web_${i}.fpc_raw_data
        where created_at >= UNIX_TIMESTAMP('${vDate}' - interval 8 hour) 
            and created_at < UNIX_TIMESTAMP('${vDate}' - interval 8 hour + interval 1 day)
              
    union all
    
        select 
            fpc, 
            domain, 
            '事件觸發' behavior,
            case type
                when  1 then '點擊banner'
                when  2 then '全站搜尋'
                when  3 then '商品列表頁'
                when  4 then '商品瀏覽'
                when  5 then '商品列表頁'
                when  6 then '文章瀏覽'
                when  7 then '活動頁'
                when  8 then '點擊熱門關鍵字'
                when  9 then '點擊NAV行為'
                when 10 then '註冊會員'
                when 11 then '試算工具'
                when 12 then '撥打電話'
                when 13 then '跨渠道綁定'
                when 14 then '購物行為新增'
                when 15 then '購物車商品新增'
                when 16 then '購物車商品移除'
                when 17 then '保固登錄'
                when 18 then '活動登錄'
                when 19 then '商品退貨行為'
                else null
            end type, 
            null referrer, 
            case type 
                when 14 then 5              
		# 購物行為新增
                when 11 then 3
                # 試算工具
                when type in (3, 4) then 2  
		# 商品瀏覽 or 商品列表頁'
                else 1
            end funnel_layer,
            null device_type,
            null device_os, 
            null page_url, 
            from_unixtime(a.created_at, '%Y-%m-%d %H:%m:%s') + interval 8 hour date 
        from cdp_web_${i}.fpc_event_raw_data a, 
            cdp_web_${i}.fpc_unique b
        where a.fpc_unique_id = b.id
            and a.created_at >= UNIX_TIMESTAMP('${vDate}' - interval 8 hour)
            and a.created_at < UNIX_TIMESTAMP('${vDate}' - interval 8 hour + interval 1 day)
        ;"

echo $sql_query_1


# Export Data
echo ''
echo 'start: ' `date`
echo 'exporting data from cdp_web_'${i} 
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.src_web_event_${stakeholder}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.src_web_event_${stakeholder}.error

# Import Data
echo ''
echo 'start: ' `date`
echo 'importing data from cdp_web_'${i} 
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.src_web_event_${stakeholder}.txt' INTO TABLE ${project_name}.src_web_event_${stakeholder} IGNORE 1 LINES;" 2>>$error_dir/$project_name.src_web_event_${stakeholder}.error 
echo 'notice: '$project_name.src_web_event_${stakeholder} 'only keep 1 day data'
done


# Further work
echo ''
echo 'working on the sp in '$dest_login_path
echo ''
# mysql --login-path=$dest_login_path -e "call ${project_name}.sp_etl_web_event_tmnewa;"


echo ''
echo 'end: ' `date`

