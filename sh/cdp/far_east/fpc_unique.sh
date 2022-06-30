#!/usr/bin/bash
##############################################################
# Project: 給遠東集團查詢 browser_fpc 在跨組織的表現
# Branch: 
# Author: Benson Cheng
# Created_at: 2022-05-13
# Updated_at: 2022-05-16
# Note: 此寫法為固定一個 org 一個 web domain，若有變動則可能出錯
###############################################################

src_login_path="cdp"
dest_login_path="datapool_prod"
project_name="far_east"
export_dir="/root/datapool/export_file"
error_dir="/root/datapool/error_log"
sh_dir="/root/datapool/sh"
table_name="fpc_unique"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
    vDateName=`date -d $1 '+%a'`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
    vDateName=`date -d "1 day ago" '+%a'`
fi


sql_0="
    CREATE TABLE IF NOT EXISTS ${project_name}.${table_name} (
        id int unsigned NOT NULL COMMENT '原始流水號',
        fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'domain 指紋碼', 
        browser_fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'browser 指紋碼', 
        org_id int unsigned NOT NULL COMMENT '組織 id',
        domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '網域',
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間', 
        PRIMARY KEY (id, fpc),
        KEY idx_browser_fpc (browser_fpc), 
        KEY idx_created_at (created_at), 
        KEY idx_org_id (org_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='${table_name} 原表擴充'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.fpc_stat (
        serial int unsigned AUTO_INCREMENT NOT NULL COMMENT '流水號',
        tag_date date NOT NULL COMMENT '資料運算日',
        end_date date NOT NULL COMMENT '資料最末日',
        total int unsigned NOT NULL DEFAULT '0' COMMENT 'domain fpc 總數',        
        feds int unsigned NOT NULL DEFAULT '0' COMMENT 'www.feds.com.tw - 遠東百貨; 其累積至今的 domain fpc 總數',
        sogo int unsigned NOT NULL DEFAULT '0' COMMENT 'www.sogo.com.tw	- SOGO; 其累積至今的 domain fpc 總數',
        febigcity int unsigned NOT NULL DEFAULT '0' COMMENT 'www.febigcity.com - 遠東巨城; 其累積至今的 domain fpc 總數',
        feamart int unsigned NOT NULL DEFAULT '0' COMMENT 'www.fe-amart.com.tw - 愛買; 其累積至今的 domain fpc 總數',
        citysuper int unsigned NOT NULL DEFAULT '0' COMMENT 'www.citysuper.com.tw - citysuper; 其累積至今的 domain fpc 總數',
        friday int unsigned NOT NULL DEFAULT '0' COMMENT 'shopping.friday.tw - friday購物; 其累積至今的 domain fpc 總數',
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間', 
        UNIQUE KEY (serial),
        PRIMARY KEY (tag_date, end_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='各 BU 之 domain fpc 歷史結算表'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.cross_mapping (
        id int unsigned AUTO_INCREMENT NOT NULL COMMENT '組合編號',
        cross_org varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '跨組織的組合細節(org_id)', 
        cross_name varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '跨組織的組合細節(nickname)', 
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間', 
        PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='跨組織的組合 mapping 表'
    ;
    CREATE TABLE IF NOT EXISTS ${project_name}.cross_stat (
        serial int unsigned AUTO_INCREMENT NOT NULL COMMENT '流水號',
        tag_date date NOT NULL COMMENT '資料運算日',
        end_date date NOT NULL COMMENT '資料最末日資料最末日',        
        mapping_id int unsigned NOT NULL COMMENT '組合編號',
        browser_fpc int unsigned NOT NULL COMMENT 'unique browser fpc 總數',
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '原始建立時間',
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '原始更新時間', 
        PRIMARY KEY (serial)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='各 BU 之 browser fpc 歷史結算表'
    ;"
echo ''
echo [CREATE TABLE IF NOT EXISTS ${project_name}.${table_name} and others]
echo $sql_0
mysql --login-path=${dest_login_path} -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_0.error



for db_id in $(seq 21 26)
do
    sql_1="
        select 
            id, 
            fpc, 
            null browser_fpc, 
            (select org_id from cdp_organization.organization_domain where domain_type = 'web' and db_id = ${db_id}) org_id, 
            (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = ${db_id}) domain, 
            from_unixtime(created_at), 
            updated_at
        from cdp_web_${db_id}.${table_name}
        where created_at >= unix_timestamp('${vDate}')
            and created_at < unix_timestamp('${vDate}' + interval 1 day)
        ;"
    echo ''
    echo [from cdp_web_${db_id}.${table_name}]
    echo $sql_1
    mysql --login-path=${src_login_path} -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${db_id}_${table_name}.txt
    mysql --login-path=${dest_login_path} -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${db_id}_${table_name}.txt' INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES (id, domain_fpc, browser_fpc, org_id, domain, created_at, updated_at);"
done


for org_id in $(seq 9 14)
do 
    sql_2="
        UPDATE ${project_name}.${table_name} a
            INNER JOIN uuid.accu_mapping_${org_id} b
            ON a.domain_fpc = b.id
        SET a.browser_fpc = b.browser_fpc
        WHERE a.org_id = ${org_id}
            and b.id_type = 'fpc'
            and (a.browser_fpc = ''
             or a.browser_fpc = 'NULL')
        ;"
    echo ''
    echo [UPDATE ${project_name}.${table_name}]
    echo $sql_2
    mysql --login-path=${dest_login_path} -e "$sql_2" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_2.error
done 


sql_3="
    TRUNCATE TABLE ${project_name}.cross_mapping
    ;
    INSERT INTO ${project_name}.cross_mapping 
        (cross_org, cross_name)
    VALUES 
        ('10,11,12,13', 'SOGO,遠東巨城,愛買,citysuper'),
        ('10,11,12', 'SOGO,遠東巨城,愛買'),
        ('10,11,13', 'SOGO,遠東巨城,citysuper'),
        ('10,12,13', 'SOGO,愛買,citysuper'),
        ('11,12,13', '遠東巨城,愛買,citysuper'),
        ('10,11', 'SOGO,遠東巨城'),
        ('10,12', 'SOGO,愛買'),
        ('10,13', 'SOGO,citysuper'),
        ('11,12', '遠東巨城,愛買'),
        ('11,13', '遠東巨城,citysuper'),
        ('12,13', '愛買,citysuper')
    ;
    
    ALTER TABLE ${project_name}.cross_mapping AUTO_INCREMENT = 1
    ;"
    
    #### 來源 SQL 
    # select concat('(', quote(group_concat(org_id)), ', ', quote(group_concat(nickname)), '),')
    # from cdp_organization.organization_domain
    # where domain_type = 'web'
    #   and org_id between 10 and 13
    #### 未來 org_id in (9, 14) 出現資料時，要再新增 cross_mapping
    
echo ''
echo [INSERT INTO ${project_name}.cross_mapping]
echo $sql_3
#######mysql --login-path=${dest_login_path} -e "$sql_3" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_3.error



if [ ${vDateName} = Sun ];
then 
    sql_4="
        INSERT INTO ${project_name}.fpc_stat    
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                sum(total) total, 
                max(feds) feds, 
                max(sogo) sogo, 
                max(febigcity) febigcity, 
                max(feamart) feamart, 
                max(citysuper) citysuper, 
                max(friday) friday, 
                now() created_at, 
                now() updated_at
            from (
                select 
                    domain,
                    count(*) total, 
                    (sum(if(org_id =  9, 1, 0))) feds, 
                    (sum(if(org_id = 10, 1, 0))) sogo, 
                    (sum(if(org_id = 11, 1, 0))) febigcity, 
                    (sum(if(org_id = 12, 1, 0))) feamart, 
                    (sum(if(org_id = 13, 1, 0))) citysuper, 
                    (sum(if(org_id = 14, 1, 0))) friday
                from ${project_name}.${table_name}
                group by domain
                ) a
        ;
        
        ALTER TABLE ${project_name}.fpc_stat AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.fpc_stat]
    echo $sql_4
    mysql --login-path=${dest_login_path} -e "$sql_4" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_4.error
   

    for org_id in $(seq 10 13)
    do 
        sql_5="
            CREATE TABLE IF NOT EXISTS ${project_name}.${table_name}_${org_id}_b ( 
                browser_fpc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                domain varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, 
                PRIMARY KEY (browser_fpc) 
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ;
            
            INSERT IGNORE INTO ${project_name}.${table_name}_${org_id}_b
                select browser_fpc, domain
                from ${project_name}.${table_name}
                where org_id = ${org_id}
                    and browser_fpc <> ''
                    and browser_fpc <> 'NULL'
                group by browser_fpc, domain
            ;"
        echo ''
        echo [INSERT INTO ${project_name}.${table_name}_${org_id}_b]
        echo $sql_5
        mysql --login-path=${dest_login_path} -e "$sql_5" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_5.error
    done 


    sql_6="
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                1 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_11_b b, 
                ${project_name}.${table_name}_12_b c, 
                ${project_name}.${table_name}_13_b d
            where a.browser_fpc = b.browser_fpc
                and c.browser_fpc = b.browser_fpc
                and c.browser_fpc = d.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                2 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_11_b b, 
                ${project_name}.${table_name}_12_b c
            where a.browser_fpc = b.browser_fpc
                and c.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
            
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                3 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_11_b b, 
                ${project_name}.${table_name}_13_b d
            where a.browser_fpc = b.browser_fpc
                and d.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                4 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_12_b b, 
                ${project_name}.${table_name}_13_b d
            where a.browser_fpc = b.browser_fpc
                and d.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                5 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_11_b b, 
                ${project_name}.${table_name}_12_b c, 
                ${project_name}.${table_name}_13_b d
            where c.browser_fpc = b.browser_fpc
                and c.browser_fpc = d.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                6 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_11_b b
            where a.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                7 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_12_b b
            where a.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                8 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_10_b a, 
                ${project_name}.${table_name}_13_b b
            where a.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                9 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_11_b a, 
                ${project_name}.${table_name}_12_b b
            where a.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                10 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_11_b a, 
                ${project_name}.${table_name}_13_b b
            where a.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;
        
        INSERT INTO ${project_name}.cross_stat
            select 
                null serial,
                '${vDate}' + interval 1 day tag_date,
                '${vDate}' end_date,
                11 mapping_id, 
                count(*) browser_fpc,                 
                now() created_at, 
                now() updated_at
            from ${project_name}.${table_name}_12_b a, 
                ${project_name}.${table_name}_13_b b
            where a.browser_fpc = b.browser_fpc
        ;
        ALTER TABLE ${project_name}.cross_stat AUTO_INCREMENT = 1
        ;"
    echo ''
    echo [INSERT INTO ${project_name}.cross_stat]
    echo $sql_6
    mysql --login-path=${dest_login_path} -e "$sql_6" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_6.error

    sql_7="
        UPDATE ${project_name}.${table_name} a
            INNER JOIN 
            (
            select 
                domain_fpc, 
                domain, created_at, 
                row_number () over (order by created_at, domain, domain_fpc) + (select max(serial) from ${project_name}.${table_name}) serial
            from ${project_name}.${table_name}
            where serial = 0
            ) b
            ON a.domain_fpc = b.domain_fpc
                and a.domain = b.domain
                and a.created_at = b.created_at
        SET a.serial = b.serial
        WHERE a.serial = 0
        ;"
    echo ''
    echo [UPDATE ${project_name}.${table_name}]
    echo $sql_7
    mysql --login-path=${dest_login_path} -e "$sql_7" 2>> $error_dir/$src_login_path/$project_name/$project_name.${table_name}_sql_7.error

    echo ''
    echo [Remove the old file]
    rm ${sh_dir}/${src_login_path}/${project_name}/*.csv

    echo ''
    echo [Make the csv file]
    mysql --login-path=datapool_prod -e "select serial, domain_fpc, browser_fpc, org_id, domain, created_at, updated_at from ${project_name}.${table_name} order by serial;" > ${sh_dir}/${src_login_path}/${project_name}/${table_name}_${vDate}.csv

    echo ''
    echo [replace the file format from Tab Separated Values to Comma Separated Values]
    sed -i 's/\t/,/g' ${table_name}_${vDate}.csv

    echo ''
    echo [count the total line number, included the header]
    wc -l ${table_name}_${vDate}.csv

    echo ''
    echo 'The following command have to execute on the local site, not EC2 environment'
    echo scp -P 222 root@ec2-35-73-67-223.ap-northeast-1.compute.amazonaws.com:/root/datapool/sh/cdp/far_east/${table_name}_${vDate}.csv /Users/kok-singtien/Desktop/AccuHit/far_east/


else 
    echo [today is ${vDateName}, not Sun. No Need to do the weekly statistics.]
fi 


echo [end: `date`]
