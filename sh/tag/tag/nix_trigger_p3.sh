#!/usr/bin/bash
export db_type=$1
export tag_date=$2
export db_id=$3
export error_dir=$4
export project_name="tag_v2"
export tag_type='trigger'

export sql_query_1="
    create table if not exists ${project_name}.nix_${tag_type}_linebot_${db_id} (
        serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號',
        tag_date date NOT NULL COMMENT '報表資料計算日',
        span varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '計算週期（比如說 90 days)',
        start_date date NOT NULL COMMENT '計算週期起始日',
        end_date date NOT NULL COMMENT '計算週期最終日通常也會是資料統計日的前一天',
        created_at timestamp NOT NULL COMMENT '此列資訊被創建時間',
        origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明這邊主要是為了event 對應到的選項填寫的不過由於Nix 非使用者行為 envent 被歸類在api所以也要補上相對應的名稱',
        tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
        tag_freq int NOT NULL COMMENT '計算週期內此種標籤被觸發總次數',
        user_count int NOT NULL COMMENT '計算週期內此種標籤被多少人觸發',
        origin varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '標籤來源: event/API 註Nix這邊非觸發的都直接當作是自定義，即歸類在API',
        channel varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來自渠道（此表皆為 line）',
        UNIQUE KEY serial (serial),
        KEY idx_tag_date (tag_date),
        KEY idx_tag (tag)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT= '近 90 天的標籤觸發次數以及人數之總表'
    ;"


export sql_query_2="
    TRUNCATE TABLE ${project_name}.nix_${tag_type}_linebot_${db_id};
    INSERT INTO ${project_name}.nix_${tag_type}_linebot_${db_id}
        select
            null serial,
            date('${tag_date}') + interval 1 day tag_date,
            '90 days' as span,
            date('${tag_date}' - interval 89 day) start_date,
            date('${tag_date}') end_date,
            now() created_at,
            origin_desc,
            tag,
            sum(tag_freq) tag_freq,
            count(user_token) user_count,
            case origin
                when 'event' then '事件'
                else null
            end origin,
            'line' as channel 
        from (
            select *
            from tag_v2.nix_${tag_type}_linebot_${db_id}_taggables
            where tag_date = date('${tag_date}') + interval 1 day
            ) a
        group by tag, from_type;
        "

echo 'create table if not exists ' ${project_name}.nix_${tag_type}_linebot_${db_id}

    mysql --login-path=$db_type -e "$sql_query_1" 2>>$error_dir/${tag_type}_linebot_${db_id}.error

echo 'TRUNCATE TABLE IF EXISTS' ${project_name}.nix_${tag_type}_linebot_${db_id}
echo 'INSERT INTO' ${project_name}.nix_${tag_type}_linebot_${db_id}
    mysql --login-path=$db_type -e "$sql_query_2" 2>>$error_dir/${tag_type}_linebot_${db_id}.error