#!/usr/bin/bash
export db_type=$1
export tag_date=$2
export db_id=$3
export error_dir=$4
export project_name="tag_v2"
export tag_type='self'



export sql_query_1="
    create table if not exists ${project_name}.nix_${tag_type}_linebot_${db_id}_taggables (
        serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號',
        tag_date date NOT NULL COMMENT '報表資料計算日',
        span varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '計算週期（比如說 90 days)',
        start_date date NOT NULL COMMENT '計算週期起始日',
        end_date date NOT NULL COMMENT '計算週期最終日通常也會是資料統計日的前一天',
        user_token varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT  '用戶Token',
        tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
        from_type int(11) NOT NULL DEFAULT '1' COMMENT 'Nix標籤來源編號', 
        origin varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '標籤來源: event/API 註Nix這邊非觸發的都直接當作是自定義，即歸類在API',
        origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明這邊主要是為了event 對應到的選項填寫的不過由於Nix 非使用者行為 envent 被歸類在api所以也要補上相對應的名稱',
        action int NOT NULL COMMENT '計算週期內該用戶觸發此種標籤之累積次數',
        last_at timestamp NOT NULL COMMENT '最後一次貼標時間',
        created_at timestamp NOT NULL COMMENT '此列資訊被創建時間',
        updated_at timestamp NOT NULL COMMENT '此列資訊最後被更新時間(目前設定是和創建時間相同)',
        UNIQUE KEY serial (serial),
        KEY idx_tag_date (tag_date),
        KEY idx_from_type (from_type),
        KEY idx_tag (tag),
        KEY idx_user_token (user_token)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='user tag 每日紀錄情形';
    "

export sql_query_2="        
    TRUNCATE TABLE ${project_name}.nix_${tag_type}_linebot_${db_id}_taggables;
        INSERT INTO ${project_name}.nix_${tag_type}_linebot_${db_id}_taggables
        select a.*
        from (
            select
                null serial,
                date('${tag_date}') + interval 1 day tag_date,
                '90 days' as span,
                date('${tag_date}' - interval 89 day) start_date,
                date('${tag_date}') end_date,
                user_token,
                tag,
                from_type,
                origin,
                origin_desc,
                action,
                max(last_at) last_at,
                now() created_at,
                now() updated_at 
            from ${project_name}.nix_${tag_type}_linebot_${db_id}_etl 
            group by user_token, tag, from_type, action
        ) a
        inner join(
            select
                null serial,
                date('${tag_date}') + interval 1 day tag_date,
                '90 days' as span,
                date('${tag_date}' - interval 89 day) start_date,
                date('${tag_date}') end_date,
                user_token,
                tag,
                from_type,
                origin,
                origin_desc,
                action,
                max(last_at) last_at,
                now() created_at,
                now() updated_at 
            from ${project_name}.nix_${tag_type}_linebot_${db_id}_etl 
            group by user_token, tag, from_type
        ) b
        on a.user_token = b.user_token
            and a.tag = b.tag
            and a.from_type = b.from_type
            and a.last_at = b.last_at
            ;
        "


echo 'create table if not exists ' ${project_name}.nix_${tag_type}_linebot_${db_id}_taggables
    mysql --login-path=$db_type -e "$sql_query_1" 2>>$error_dir/${tag_type}_linebot_${db_id}_taggables.error
  
echo 'TRUNCATE TABLE IF EXISTS' ${project_name}.nix_${tag_type}_linebot_${db_id}_taggables
echo 'INSERT INTO' ${project_name}.nix_${tag_type}_linebot_${db_id}_taggables
    mysql --login-path=$db_type -e "$sql_query_2" 2>>$error_dir/${tag_type}_linebot_${db_id}_taggables.error


