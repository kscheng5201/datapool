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
for org in $(seq 1 14)
do 
    export sql_query_1="
        select id
        from cdp_organization.organization_domain
        where org_id = ${org}
            and domain_type <> 'web'
	;"
	# echo $sql_query_1

	mysql --login-path=$src_login_path -e "$sql_query_1" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_id_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_id_${i}.error
    	sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_id_${i}.txt

    while read id ;
    do
        export sql_query_2="
            select concat(domain_type, '_', db_id)
            from cdp_organization.organization_domain
            where id = ${id}
        ;"
	# echo $sql_query_2

        mysql --login-path=$src_login_path -e "$sql_query_2" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_db_${i}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_db_${i}.error
        sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_db_${i}.txt
    
    
        while read q ;
        do
            export sql_query_3="
                SET NAMES utf8mb4
                ; 
                select 
                    if(mod(identity, 2) = 1, identity, concat(upper('$(echo ${q} | cut -d _ -f 1)'), '|', $(echo ${q} | cut -d _ -f 1)_unique_id)) uuid, 
                    $(echo ${q} | cut -d _ -f 1)_unique_id id, 
                    'unique_id' id_type, 
                    '$(echo ${q} | cut -d _ -f 1)' channel_type, 
                    (select domain from cdp_organization.organization_domain where id = ${id}) domain,
                    FROM_UNIXTIME($(echo ${q} | cut -d _ -f 1)_unique_created_at) created_at, 
                    FROM_UNIXTIME(a.updated_at) updated_at
                from cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique_data a, 
                    cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique b
                where a.$(echo ${q} | cut -d _ -f 1)_unique_id = b.id
                    and $(echo ${q} | cut -d _ -f 1)_unique_id is not null
                    and $(echo ${q} | cut -d _ -f 1)_unique_id <> ''
                    and (($(echo ${q} | cut -d _ -f 1)_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and $(echo ${q} | cut -d _ -f 1)_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (a.updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))
                
                union all
                
                select 
                    if(mod(identity, 2) = 1, identity, concat(upper('$(echo ${q} | cut -d _ -f 1)'), '|', $(echo ${q} | cut -d _ -f 1)_unique_id)) uuid, 
                    member_id id, 
                    'member_id' id_type, 
                    '$(echo ${q} | cut -d _ -f 1)' channel_type, 
                    (select domain from cdp_organization.organization_domain where id = ${id}) domain,
                    FROM_UNIXTIME($(echo ${q} | cut -d _ -f 1)_unique_created_at) created_at, 
                    FROM_UNIXTIME(a.updated_at) updated_at
                from cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique_data a, 
                    cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique b
                where a.$(echo ${q} | cut -d _ -f 1)_unique_id = b.id
                    and member_id is not null
                    and member_id <> ''
                    and (($(echo ${q} | cut -d _ -f 1)_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and $(echo ${q} | cut -d _ -f 1)_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (a.updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))                
                
                union all
                
                select 
                    if(mod(identity, 2) = 1, identity, concat(upper('$(echo ${q} | cut -d _ -f 1)'), '|', $(echo ${q} | cut -d _ -f 1)_unique_id)) uuid, 
                    identity id, 
                    'identity' id_type,
                    '$(echo ${q} | cut -d _ -f 1)' channel_type, 
                    (select domain from cdp_organization.organization_domain where id = ${id}) domain,
                    FROM_UNIXTIME($(echo ${q} | cut -d _ -f 1)_unique_created_at) created_at, 
                    FROM_UNIXTIME(a.updated_at) updated_at
                from cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique_data a, 
                    cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique b
                where a.$(echo ${q} | cut -d _ -f 1)_unique_id = b.id
                    and identity is not null
                    and identity <> ''
                    and (($(echo ${q} | cut -d _ -f 1)_unique_created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and $(echo ${q} | cut -d _ -f 1)_unique_created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (a.updated_at >= UNIX_TIMESTAMP('${vDate}')
                    and a.updated_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day)))                
                
                union all
                
                select 
                    if(mod(identity, 2) = 1, identity, concat(upper('$(echo ${q} | cut -d _ -f 1)'), '|', $(echo ${q} | cut -d _ -f 1)_unique_id)) uuid, 
                    $(echo ${q} | cut -d _ -f 1) id, 
                    '$(echo ${q} | cut -d _ -f 1)_id' id_type,
                    '$(echo ${q} | cut -d _ -f 1)' channel_type, 
                    (select domain from cdp_organization.organization_domain where id = ${id}) domain,
                    FROM_UNIXTIME(b.created_at) created_at, 
                    b.updated_at
                from cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique_data a, 
                    cdp_${q}.$(echo ${q} | cut -d _ -f 1)_unique b
                where a.$(echo ${q} | cut -d _ -f 1)_unique_id = b.id
                    and $(echo ${q} | cut -d _ -f 1) is not null
                    and $(echo ${q} | cut -d _ -f 1) <> ''
                    and ((b.created_at >= UNIX_TIMESTAMP('${vDate}') 
                    and b.created_at < UNIX_TIMESTAMP('${vDate}' + interval 1 day))
                    or (b.updated_at >= '${vDate}'
                    and b.updated_at < '${vDate}' + interval 1 day))  
                ;"
	echo ''
	echo ${vDate}
	# echo $sql_query_3
	mysql --login-path=$src_login_path -e "$sql_query_3" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${id}.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${id}.error
	mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${id}.txt' REPLACE INTO TABLE ${project_name}.${src_login_path}_${id} IGNORE 1 LINES;" 2>>$error_dir/$project_name.${src_login_path}_${id}.error 

        done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_domain_db_${i}.txt
    done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_id_${i}.txt
done  
