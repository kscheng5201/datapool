#!/usr/bin/bash
export db_type=$1
export tag_date=$2
export org_id=$3
export error_dir=$4
export project_name="tag_v2"
export tag_type='trigger'




export sql_1="
    create table if not exists ${project_name}.cdp_${tag_type}_${org_id} (
        serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號' unique,
        tag_date date NOT NULL COMMENT '資料運算日',
        channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web/line/messenger/app/crm',
        tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
        tag_freq int unsigned NOT NULL DEFAULT '0' COMMENT '貼標次數',
        user_count int unsigned NOT NULL DEFAULT '0' COMMENT '貼標人數',
        origin varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '廣告/事件/網站頁面/自定義',
        origin_desc varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '點擊UTM/(事件名稱)/頁面瀏覽/(-)',
        created_at timestamp NOT NULL COMMENT '創建時間',
        KEY idx_tag_date (tag_date),
        KEY idx_channel (channel),
        KEY idx_tag (tag),
        KEY idx_origin (origin),
        KEY idx_behavior (origin_desc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='編號 ${org_id} 的客戶，近 90 天的觸發型標籤總表'
    ;
    "

export sql_2="
    TRUNCATE TABLE ${project_name}.cdp_${tag_type}_${org_id};
        INSERT INTO ${project_name}.cdp_${tag_type}_${org_id}
            select
                null serial,
                date('${tag_date}') + interval 1 day tag_date,
                channel,
                tag,
                sum(tag_freq) tag_freq,
                count(distinct fpc) user_count,
                case origin
                    when 'campaign'     then '廣告'
                    when 'event'        then '事件'
                    when 'page_view'    then '網站頁面'
                    when 'API'          then '自定義'
                    else null
                end origin,
                case origin
                    when 'campaign'     then '點擊UTM'
                    when 'event'        then origin_desc
                    when 'page_view'    then '頁面瀏覽'
                    when 'API'          then '-'
                    else null
                end origin_desc,
                now() created_at
            from ${project_name}.cdp_${tag_type}_${org_id}_etl
            where origin in ('campaign', 'event', 'page_view')
                and tag is not null
                and tag <> ''
                and tag_date > date('${tag_date}') - interval 90 day
            group by
                channel,
                tag,
                origin
        ;
        ALTER TABLE ${project_name}.cdp_${tag_type}_${org_id} AUTO_INCREMENT = 1
        ;"

echo 'create table if not exists' ${project_name}.cdp_${tag_type}_${org_id}
mysql --login-path=$db_type -e "$sql_1" 2>>$error_dir/${tag_type}_${org_id}.error
echo "INSERT INTO ${project_name}.cdp_${tag_type}_${org_id}"
mysql --login-path=$db_type -e "$sql_2" 2>>$error_dir/${tag_type}_${org_id}.error

