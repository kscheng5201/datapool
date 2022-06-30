#!/usr/bin/bash
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export src_login_path_0="nix"
export src_login_path_1="datapool_dev"
export dest_login_path="datapool_prod"
export project_name="tag"
export table_name="src"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y-%m-%d"`
else
    vDate=`date -d "1 day ago" +"%Y-%m-%d"`
fi

# IM: Instant Messenger
IM="fbmessenger line"


for cur_im in $IM;
do
    export sql_query_1="
        select id
        from bots_lifespan.${cur_im}bots
        ;"
    mysql --login-path=$src_login_path_1 -e "$sql_query_1" > ${export_dir}/${src_login_path_0}/${project_name}/${project_name}.${src_login_path_0}_${cur_im}bots_id.txt 2>> $error_dir/$src_login_path_0/$project_name/$project_name.${src_login_path_0}_${cur_im}bots_id.error
    sed -i '1d' $export_dir/$src_login_path_0/$project_name/$project_name.${src_login_path_0}_${cur_im}bots_id.txt
        
    while read p ;
    do
        export sql_query_2="
	    delete 
	    from ${project_name}.${src_login_path_0}_${cur_im}bot_${p}_${table_name}
            where stat_date < '${vDate}' - interval 90 day
	    ; 
	    TRUNCATE TABLE summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}; 
            INSERT INTO summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p} 
                select 
                    null serial, 
                    date('${vDate}') stat_date, 
                    date('${vDate}' - interval 89 day)start_date, 
                    date('${vDate}') end_date, 
                    user_token, 
                    tag, 
                    tag_freq, 
                    last_at, 
                    floor((1 - rid / sum_over) * 100) ranking, 
                    now() created_at, 
                    now() updated_at
                from (
                    select *, 
                        sum(1) over (partition by tag) sum_over, 
                        row_number () over (partition by tag order by tag_freq desc, last_at desc) rid
                    from ${project_name}.${src_login_path_0}_${cur_im}bot_${p}_${table_name}
                    ) a
            ;"
             echo $sql_query_2

    # Processing Data
    echo ''
    echo [start the ${vDate} data on `date`]
    echo [delete * where stat_date < '${vDate}' - interval 90 day]
    echo [TRUNCATE TABLE IF EXISTS summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}]
    echo [INSERT INTO summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}]
    mysql --login-path=$src_login_path_1 -e "$sql_query_2"


    export sql_query_3="
        select * 
        from summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}
        ;"
    # Export Data
    echo ''
    echo [start ${vDate} data at `date`]
    echo [export data to $export_dir/$src_login_path_0/$project_name/${project_name}.summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}.txt]
    mysql --login-path=$src_login_path_1 -e "$sql_query_3" > $export_dir/$src_login_path_0/$project_name/${project_name}.summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}.txt 2>>$error_dir/$src_login_path_0/$project_name/$project_name.summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}.error       
    echo [truncate table summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}]
    mysql --login-path=$dest_login_path -e "truncate table summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p};"
    echo [import data to ${project_name}.summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/$src_login_path_0/$project_name/$project_name.summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}.txt' INTO TABLE summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p} IGNORE 1 LINES;" 2>>$error_dir/$project_name.summary_${src_login_path_0}.${project_name}_${cur_im}bot_${p}.error 

    done < $export_dir/$src_login_path_0/$project_name/$project_name.${src_login_path_0}_${cur_im}bots_id.txt
done 

echo ''
echo 'end: ' `date`


