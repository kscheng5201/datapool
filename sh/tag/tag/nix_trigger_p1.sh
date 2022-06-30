#!/usr/bin/bash
#從 Nix 上面拉東西下來
export src_login_path=$1
export db_type=$2
export tag_date=$3
export db_id=$4
export export_dir=$5
export error_dir=$6
export project_name="tag_v2"
export tag_type='trigger'


export sql_query_2="
    SET NAMES utf8mb4;
    select
        null serial, 
        date(a.created_at) + interval 1 day tag_date,
        user_token,
        b.name tag,
        from_type,
        'event' as origin,
        (case when from_type=3 then 'keyword' 
        when from_type=4 then 'referral' 
        when from_type=9 then 'channel_link' 
        when from_type=10 then 'coupon_campaign_gift' 
        when from_type=11 then 'coupon_verification_code' 
        when from_type=12 then 'redirect' 
        when from_type=15 then 'richmenu' 
        when from_type=16 then 'member_registered' 
        end) origin_desc, 
        count(*) tag_freq,   	
        max(a.created_at) last_at,
        now() created_at,
        now() updated_at 
    from accunix_v2_log.linebot_${db_id}_tag_histories a
        left join accunix_v2_log.linebot_${db_id}_tags b
            on a.tag_id = b.id
        left join accunix_v2_log.linebot_${db_id}_users c
            on a.user_id = c.id
    where a.created_at >= date('${tag_date}')
        and a.created_at < date('${tag_date}' + interval 1 day)
        and action < 3
        and b.deleted_at is null
        and from_type in (3,4,9,10,11,12,15,16)
    group by
        user_token,
        from_type,
        b.name;   
    "
# echo $sql_query_2

export sql_query_3="
    create table if not exists ${project_name}.nix_${tag_type}_linebot_${db_id}_etl (
        serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號',
        tag_date date NOT NULL COMMENT '報表資料計算日',
        user_token varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT  '用戶Token',
        tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
        from_type int(11) NOT NULL DEFAULT '1' COMMENT 'Nix標籤來源編號', 
        origin varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '標籤來源: event/API 註Nix這邊非觸發的都直接當作是自定義，即歸類在API',
        origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明這邊主要是為了event 對應到的選項填寫的不過由於Nix 非使用者行為 envent 被歸類在api所以也要補上相對應的名稱',
        tag_freq int NOT NULL COMMENT '標籤累積次數',
        last_at timestamp NOT NULL COMMENT '最後一次貼標時間',
        created_at timestamp NOT NULL COMMENT '此列資訊被創建時間',
        updated_at timestamp NOT NULL COMMENT '此列資訊最後被更新時間(目前設定是和創建時間相同)', 
        UNIQUE KEY serial (serial),
        KEY idx_tag_date (tag_date),
        KEY idx_from_type (from_type),
        KEY idx_tag (tag),
        KEY idx_user_token (user_token)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='近 90 天的 user tag 每日紀錄情形'
    ;"
# echo $sql_query_2

# Export Data
echo ''
echo 'tag_date' ${tag_date} ' and start: ' `date` 
echo 'export data to '$export_dir/${tag_type}_linebot_${db_id}_etl.txt
    mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/${tag_type}_linebot_${db_id}_etl.txt 2>>$error_dir/${tag_type}_linebot_${db_id}_etl.error
## 將每日收錄的資料統一放到txt檔

# Import Data
echo 'create table if not exists ' ${project_name}.nix_${tag_type}_linebot_${db_id}_etl
mysql --login-path=$db_type -e "$sql_query_3" 2>>$error_dir/${tag_type}_linebot_${db_id}_etl.error

echo 'import data to '${project_name}.nix_${tag_type}_linebot_${db_id}_etl
mysql --login-path=$db_type -e "LOAD DATA LOCAL INFILE '$export_dir/${tag_type}_linebot_${db_id}_etl.txt' INTO TABLE ${project_name}.nix_${tag_type}_linebot_${db_id}_etl IGNORE 1 LINES;" 2>>$error_dir/${tag_type}_linebot_${db_id}_etl.error
# 這邊把數據帶入資料庫的表