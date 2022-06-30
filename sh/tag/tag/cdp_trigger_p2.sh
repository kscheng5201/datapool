
#!/usr/bin/bash

export db_type=$1 #datapool
export tag_date=$2
export org_id=$3
export error_dir=$4
export update_way=$5 #@1
export tag_type="trigger"
export project_name="tag_v2"


export sql_1="
    create table if not exists ${project_name}.cdp_${tag_type}_${org_id}_taggables (
        serial int unsigned NOT NULL AUTO_INCREMENT COMMENT '流水號' unique,
        tag_date date NOT NULL COMMENT '資料運算日',
        span varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '計算週期（比如說 90 days)',
        start_date date NOT NULL COMMENT '90 天起始日',
        end_date date NOT NULL COMMENT '90 天最末日',
        fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '指紋碼',
        channel varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'web/line/messenger/app/crm',
        tag varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤名稱',
        origin varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '標籤來源: event/API/campaign/page_review',
        origin_desc varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '標籤來源的細節說明這邊主要是為了event 對應到的選項填寫的',            
        tag_freq int unsigned NOT NULL DEFAULT '0' COMMENT '貼標次數',
        ranking int NOT NULL COMMENT '標籤濃度',
        last_at datetime NOT NULL COMMENT '最近貼標時間',
        created_at timestamp NOT NULL COMMENT '創建時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
        KEY idx_tag_date (tag_date),
        KEY idx_channel (channel),
        KEY idx_tag (tag),
        KEY idx_fpc (fpc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='編號 ${org_id} 的客戶，近 90 天的用戶標籤表現'
    ;"

export sql_2="
    TRUNCATE TABLE ${project_name}.cdp_${tag_type}_${org_id}_taggables;
        INSERT INTO ${project_name}.cdp_${tag_type}_${org_id}_taggables
            select
                null serial,
                date('${tag_date}') + interval 1 day tag_date,
                '90 days' span,
                date('${tag_date}') - interval 89 day start_date,
                date('${tag_date}') end_date,
                fpc,
                channel,
                tag,
                origin,
                origin_desc,                   
                tag_freq,
                floor((1 - rid / sum_over) * 100) ranking,
                datetime last_at,
                now() created_at,
                now() updated_at
            from (
                select
                    fpc,
                    min(datetime) datetime,
                    channel,
                    origin,
                    origin_desc,                    
                    replace(tag, ' ', '') tag,
                    sum(tag_freq) tag_freq,
                    sum(1) over (partition by tag) sum_over,
                    row_number () over (partition by tag order by sum(tag_freq) desc, min(datetime) desc) rid
                from ${project_name}.cdp_${tag_type}_${org_id}_etl
                where origin in ('campaign', 'event', 'page_view')
                    and tag is not null
                    and tag <> ''  
                    and tag_date > date('${tag_date}') - interval 90 day              
                group by
                    fpc,
                    channel,
                    tag
                ) a
        ;
    ALTER TABLE ${project_name}.cdp_${tag_type}_${org_id}_taggables AUTO_INCREMENT = 1
    ;"

export sql_3=""




echo 'tag_date:' ${tag_date}
echo 'create table if not exists' ${project_name}.cdp_${tag_type}_${org_id}_taggables
mysql --login-path=$db_type -e "$sql_1" 2>>$error_dir/${tag_type}_${org_id}_taggables.error


if [ $update_way=='first' ]; then 
echo 'TRUNCATE TABLE' ${project_name}.cdp_${tag_type}_${org_id}_taggables
echo 'INSERT INTO' ${project_name}.cdp_${tag_type}_${org_id}_taggables
mysql --login-path=$db_type -e "$sql_2" 2>>$error_dir/${tag_type}_${org_id}_taggables.error
else
echo 'It still working'
fi 




