#!/usr/bin/bash
export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="uuid"
export src_login_path="cdp"

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
            select domain
            from cdp_organization.organization_domain
            where org_id = ${i}
                and db_id = ${p}
            and domain_type = 'web'
        ;"
    mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_${i}.error
    sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_${i}.txt
    
        while read q ;
        do
            export sql_query_3="
                SET NAMES utf8mb4
                ; 
                select 
                    if(mod(identity, 2) = 1, identity, fpc) uuid,  
                    fpc_unique_id id, 
                    'unique_id' id_type, 
                    'web' channel_type, 
                    '${q}' domain,
                    FROM_UNIXTIME(fpc_unique_created_at) created_at, 
                    FROM_UNIXTIME(a.updated_at) updated_at
                from cdp_web_${p}.fpc_unique_data a, 
                    cdp_web_${p}.fpc_unique b 
                where a.fpc_unique_id = b.id
                    and fpc_unique_id is not null
                    and fpc_unique_id <> ''
                    and ((fpc_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (a.updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
            
                union all
                
                select 
                    if(mod(identity, 2) = 1, identity, fpc) uuid,  
                    member_id id, 
                    'member_id' id_type, 
                    'web' channel_type, 
                    '${q}' domain,
                    FROM_UNIXTIME(fpc_unique_created_at) created_at, 
                    FROM_UNIXTIME(a.updated_at) updated_at
                from cdp_web_${p}.fpc_unique_data a, 
                    cdp_web_${p}.fpc_unique b 
                where a.fpc_unique_id = b.id
                    and member_id is not null
                    and member_id <> ''
                    and ((fpc_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (a.updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
                
                union all
                
                select 
                    if(mod(identity, 2) = 1, identity, fpc) uuid,  
                    identity id, 
                    'identity' id_type, 
                    'web' channel_type, 
                    '${q}' domain,
                    FROM_UNIXTIME(fpc_unique_created_at) created_at, 
                    FROM_UNIXTIME(a.updated_at) updated_at
                from cdp_web_${p}.fpc_unique_data a, 
                    cdp_web_${p}.fpc_unique b 
                where a.fpc_unique_id = b.id
                    and identity is not null
                    and identity <> ''
                    and ((fpc_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and fpc_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (a.updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
                
                union all
                
                select 
                    if(mod(identity, 2) = 1, identity, fpc) uuid,  
                    fpc id, 
                    'fpc' id_type, 
                    'web' channel_type, 
                    '${q}' domain,
                    FROM_UNIXTIME(created_at) created_at, 
                    b.updated_at
                from cdp_web_${p}.fpc_unique_data a, 
                    cdp_web_${p}.fpc_unique b 
                where a.fpc_unique_id = b.id
                    and fpc is not null
                    and fpc <> ''
                    and ((created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (b.updated_at >= '${vDate}'
                    and b.updated_at < '${vDate}' + interval 1 day))
            ;"
	echo ''
	echo ${vDate}
	# echo $sql_query_3
	mysql --login-path=$src_login_path -e "$sql_query_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${i}.error
	mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${i}.txt' REPLACE INTO TABLE ${project_name}.${src_login_path}_${i} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${i}.error 

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_${i}.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_db_id_${i}.txt
done  


# Further work
for org in $(seq 1 14)
do
    export sql_query_4="
        UPDATE uuid.cdp_${org} a
            LEFT JOIN uuid.cdp_fpc_mapping b
            on a.id = b.origin_fpc and a.domain = b.domain
        SET a.uuid = if(right(a.uuid, 1) = 1, a.uuid, ifnull(b.fpc, a.uuid)) 
        WHERE a.id_type = 'fpc'
            and ((a.created_at >= UNIX_TIMESTAMP('${vDate}') 
            and a.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
            or (a.updated_at >= '${vDate}'
            and a.updated_at < '${vDate}' + interval 1 day)) 
        ;"
    mysql --login-path=$dest_login_path -e "$sql_query_4" 
done

