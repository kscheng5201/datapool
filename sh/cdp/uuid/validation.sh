#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="cdp"
export table_name="validation"

#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi


export sql_1="
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 1) 
        from cdp_web_1.fpc_unique a, cdp_web_1.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 10) 
        from cdp_web_10.fpc_unique a, cdp_web_10.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 11) 
        from cdp_web_11.fpc_unique a, cdp_web_11.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 12) 
        from cdp_web_12.fpc_unique a, cdp_web_12.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 13) 
        from cdp_web_13.fpc_unique a, cdp_web_13.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 14) 
        from cdp_web_14.fpc_unique a, cdp_web_14.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 15) 
        from cdp_web_15.fpc_unique a, cdp_web_15.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 16) 
        from cdp_web_16.fpc_unique a, cdp_web_16.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 17) 
        from cdp_web_17.fpc_unique a, cdp_web_17.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 18) 
        from cdp_web_18.fpc_unique a, cdp_web_18.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 19) 
        from cdp_web_19.fpc_unique a, cdp_web_19.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 2) 
        from cdp_web_2.fpc_unique a, cdp_web_2.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 20) 
        from cdp_web_20.fpc_unique a, cdp_web_20.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 21) 
        from cdp_web_21.fpc_unique a, cdp_web_21.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 22) 
        from cdp_web_22.fpc_unique a, cdp_web_22.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 23) 
        from cdp_web_23.fpc_unique a, cdp_web_23.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 24) 
        from cdp_web_24.fpc_unique a, cdp_web_24.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 25) 
        from cdp_web_25.fpc_unique a, cdp_web_25.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 26) 
        from cdp_web_26.fpc_unique a, cdp_web_26.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 27) 
        from cdp_web_27.fpc_unique a, cdp_web_27.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 28) 
        from cdp_web_28.fpc_unique a, cdp_web_28.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 29) 
        from cdp_web_29.fpc_unique a, cdp_web_29.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 3) 
        from cdp_web_3.fpc_unique a, cdp_web_3.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 30) 
        from cdp_web_30.fpc_unique a, cdp_web_30.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 31) 
        from cdp_web_31.fpc_unique a, cdp_web_31.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 32) 
        from cdp_web_32.fpc_unique a, cdp_web_32.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 33) 
        from cdp_web_33.fpc_unique a, cdp_web_33.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 4) 
        from cdp_web_4.fpc_unique a, cdp_web_4.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 5) 
        from cdp_web_5.fpc_unique a, cdp_web_5.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 6) 
        from cdp_web_6.fpc_unique a, cdp_web_6.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 7) 
        from cdp_web_7.fpc_unique a, cdp_web_7.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 8) 
        from cdp_web_8.fpc_unique a, cdp_web_8.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 9) 
        from cdp_web_9.fpc_unique a, cdp_web_9.fpc_unique_data b
        where a.id = b.fpc_unique_id
        UNION ALL
    select 
        fpc, 
        (select domain from cdp_organization.organization_domain where domain_type = 'web' and db_id = 9999) 
        from cdp_web_9999.fpc_unique a, cdp_web_9999.fpc_unique_data b
        where a.id = b.fpc_unique_id
    ;"
echo ''
echo [Export ${vData} Data from ${src_login_path}.landing2_mapping at `date`]
echo $sql_1
mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}.error

echo ''
echo [Import ${vData} Data IGNORE INTO TABLE ${project_name}.${table_name}]
mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${table_name}.txt' IGNORE INTO TABLE ${project_name}.${table_name} IGNORE 1 LINES;" 2>>$error_dir/$src_login_path/$project_name/$project_name.${table_name}.error 
