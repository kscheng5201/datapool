#!/usr/bin/bash
export dest_login_path="datapool_prod"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="nes_model"
export src_login_path="nix"

#### Get Date ####
if [ -n "$1" ]; 
then
    vDate=`date -d $1 +"%Y%m%d"`
else
    vDate=`date -d "1 day ago" +"%Y%m%d"`
fi


#### Get the table list ####
export sql_0="
    select concat_ws('.', table_schema, table_name)
    from information_schema.tables
    where table_schema = 'summary_nix'
        and table_name like '%history'
    ;"    
echo ''
echo [Get the table list]
mysql --login-path=$dest_login_path -e "$sql_0" > $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_table_list.txt 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_table_list.error
sed -i '1d' $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_table_list.txt


while read table_name; 
do 
    export sql_1="
        UPDATE ${table_name}
        SET E0_prop = ifnull(round(100 * E0_sum / (E0_sum + S1_sum + S2_sum + S3_sum + N_sum)), 0),
            S1_prop = ifnull(round(100 * S1_sum / (E0_sum + S1_sum + S2_sum + S3_sum + N_sum)), 0),
            S2_prop = ifnull(round(100 * S2_sum / (E0_sum + S1_sum + S2_sum + S3_sum + N_sum)), 0),
            S3_prop = ifnull(round(100 * S3_sum / (E0_sum + S1_sum + S2_sum + S3_sum + N_sum)), 0),
             N_prop = ifnull(round(100 * N_sum / (E0_sum + S1_sum + S2_sum + S3_sum + N_sum)), 0)
        WHERE end_date = '${vDate}'
        ;"
        
    echo ''
    echo [UPDATE ${table_name} on ${vDate} data]
    echo $sql_1
    mysql --login-path=$dest_login_path -e "$sql_1" 2>>$error_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${table_name}.error

done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_table_list.txt

echo ''
echo [end the ${vDate} data at `date`]
