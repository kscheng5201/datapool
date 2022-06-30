#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="tracker"
export src_login_path="cdp"
export table_name="uuid_unique"


#### Get Date ####
if [ -n "$1" ]; then
vDate=$1
else
vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

#### loop by db_id ####
for i in $(seq 1 14)
do 
    export sql_query_1="
        select db_id
        from cdp_organization.organization_domain
        where org_id = ${i}
	    and domain_type = 'web'
	;"
mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_db_id_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_db_id_${i}.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_db_id_${i}.txt

    while read p ;
    do
        export sql_query_2="
            select domain_type
            from cdp_organization.organization_domain
            where org_id = ${i}
                and db_id = ${p}
		and domain_type = 'web'
        ;"
    mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_type_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_type_${i}.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_type_${i}.txt
    
        while read q ;
        do
            export sql_query_3="
		SET NAMES utf8mb4
		; 
                select 
                    null uuid, 
                    fpc_unique_id id, 
                    'unique_id' id_type, 
                    '${q}' channel_type, 
                    (select domain from cdp_organization.organization_domain where org_id = ${i} and domain_type = '${q}') domain,
                    FROM_UNIXTIME(fpc_unique_created_at) created_at, 
                    FROM_UNIXTIME(updated_at) updated_at
                from cdp_${q}_${p}.fpc_unique_data
                where fpc_unique_id is not null
                    and fpc_unique_id <> ''
                    and ((fpc_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
                
                union all
                
                select 
                    null uuid, 
                    member_id id, 
                    'member_id' id_type, 
                    '${q}' channel_type, 
                    (select domain from cdp_organization.organization_domain where org_id = ${i} and domain_type = '${q}') domain,
                    FROM_UNIXTIME(fpc_unique_created_at) created_at, 
                    FROM_UNIXTIME(updated_at) updated_at
                from cdp_${q}_${p}.fpc_unique_data
                where member_id is not null
                    and member_id <> ''
                    and ((fpc_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
                
                union all
                
                select 
                    null uuid, 
                    identity id, 
                    'identity' id_type, 
                    '${q}' channel_type, 
                    (select domain from cdp_organization.organization_domain where org_id = ${i} and domain_type = '${q}') domain,
                    FROM_UNIXTIME(fpc_unique_created_at) created_at, 
                    FROM_UNIXTIME(updated_at) updated_at
                from cdp_${q}_${p}.fpc_unique_data
                where identity is not null
                    and identity <> ''
                    and ((fpc_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
                
                union all
                
                select 
                    null uuid, 
                    fpc id, 
                    'fpc' id_type, 
                    '${q}' channel_type, 
                    (select domain from cdp_organization.organization_domain where org_id = ${i} and domain_type = '${q}') domain,
                    FROM_UNIXTIME(created_at) created_at, 
                    updated_at
                from cdp_${q}_${p}.fpc_unique
                where fpc is not null
                    and fpc <> ''
                    and ((created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (updated_at >= '${vDate}'
                    and updated_at < '${vDate}' + interval 1 day))
            ;"
	# echo $sql_query_3
	mysql --login-path=$src_login_path -e "$sql_query_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}_${p}_${q}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}_${p}_${q}.error
	mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}_${i}_${p}_${q}.txt' REPLACE INTO TABLE ${project_name}.${src_login_path}_${table_name}_${i} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${table_name}_${i}.error 

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_type_${i}.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_db_id_${i}.txt
done  
