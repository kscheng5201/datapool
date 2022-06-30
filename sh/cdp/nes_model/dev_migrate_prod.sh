#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="nes_model"
export src_login_path_name="cdp"
export src_login_path="datapool"

for org_id in $(seq 7)
do
    export sql_1="
        select 
            null serial,
            uuid, 
            first_at, 
            timestamp, 
            created_at, 
            updated_at
        from ${project_name}.cdp_${org_id}_crosschannel_src
        ;"
    echo ''
    echo [copy ${project_name}.cdp_${org_id}_crosschannel_src]
    mysql --login-path=$src_login_path -e "$sql_1" > $export_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_src.txt 2>>$error_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_src.error
    
    echo ''
    echo [insert into from $project_name.${src_login_path_name}_${org_id}_src.txt ]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_src.txt' INTO TABLE web.nes_${org_id}_src IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_src.error
    
    
    export sql_2="
        select 
            null serial,
            uuid, 
            first_at, 
            sum_count, 
            observation_olddate, 
            observation_newdate, 
            observation_interval, 
            create_interval, 
            cycle_time, 
            Recency, 
            rt_ratio, 
            kind_of_person, 
            created_at, 
            updated_at
        from nes_model.cdp_${org_id}_crosschannel_etl
        ;"
    echo ''
    echo [copy ${project_name}.cdp_${org_id}_crosschannel_etl]
    mysql --login-path=$src_login_path -e "$sql_2" > $export_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_etl.txt 2>>$error_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_etl.error
    
    echo ''
    echo [insert into from $project_name.${src_login_path_name}_${org_id}_etl.txt ]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_etl.txt' INTO TABLE web.nes_${org_id}_etl IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_etl.error
    
        
    export sql_3="         
        select 
            null serial,
            start_date, 
            End_date, 
            E0_sum, 
            S1_sum, 
            S2_sum, 
            S3_sum, 
            N_sum, 
            E0_prop, 
            S1_prop, 
            S2_prop, 
            S3_prop, 
            N_prop, 
            created_at, 
            updated_at
        from nes_model.cdp_${org_id}_crosschannel_history
        ;"
    echo ''
    echo [copy ${project_name}.cdp_${org_id}_crosschannel_history]
    mysql --login-path=$src_login_path -e "$sql_3" > $export_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_history.txt 2>>$error_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_history.error
    
    echo ''
    echo [insert into from $project_name.${src_login_path_name}_${org_id}_history.txt ]
    mysql --login-path=$dest_login_path -e "LOAD DATA LOCAL INFILE '$export_dir/${src_login_path_name}/$project_name/$project_name.${src_login_path_name}_${org_id}_history.txt' INTO TABLE web.nes_${org_id} IGNORE 1 LINES;" 2>>$error_dir/$project_name/${project_name}.${type}_${table_name}_${org_id}_history.error

done

echo ''
echo [end the job at `date`]
