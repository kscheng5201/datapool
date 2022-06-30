#!/usr/bin/bash
####################################################
# Project: Streaming Web
# Branch: 取得 table_list
# Author: Benson Cheng
# Created_at: 2022-02-16
# Updated_at: 2022-02-16
# Note:
#####################################################

export dest_login_path="datapool"
export export_dir="/root/datapool/export_file"
export error_dir="/root/datapool/error_log"
export project_name="stream"
export src_login_path="cdp"

while read org_id;
do
     while read db_list;
     do
         echo ''
         echo [ rm $export_dir/$src_login_path/$project_name/${db_list}.schema.txt ]
         #rm $export_dir/$src_login_path/$project_name/${db_list}.schema.txt

         while read table_list;
         do

sql_1=$(cat <<EOF
select concat('CREATE DATABASE IF NOT EXISTS ', table_schema,  'LOCATION \"s3a://accuemrs3/hdfs_data/', table_schema, '.db//\"; DROP TABLE IF EXISTS ', table_schema, '.', table_name, ';
                     CREATE EXTERNAL TABLE IF NOT EXISTS ', table_schema, '.', table_name, ' (')
                     
                 from information_schema.tables
                 where table_schema = '${db_list}'
                     and table_name = '${table_list}'

                 UNION ALL

                 (
                 select data_type
                 from (
                     select concat(COLUMN_NAME, space(1), DATA_TYPE, '
COMMENT ', ifnull(quote(COLUMN_COMMENT), '\"\"'), ',') data_type,
row_number () over (order by ordinal_position) rid
                     from information_schema.columns
                     where table_schema = '${db_list}'
                         and table_name = '${table_list}'
                         and ordinal_position < (select
max(ordinal_position) from information_schema.columns where table_schema
= '${db_list}' and table_name = '${table_list}')
                     ) a
                 order by rid
                 )

                 UNION ALL

                 select concat(COLUMN_NAME, space(1), DATA_TYPE, '
COMMENT ', ifnull(quote(COLUMN_COMMENT), '\"\"'))
                 from information_schema.columns
                 where table_schema = '${db_list}'
                     and table_name = '${table_list}'
                     and ordinal_position = (select max(ordinal_position)
from information_schema.columns where table_schema = '${db_list}' and
table_name = '${table_list}')

                 UNION ALL

                 select concat('
                     )
                     COMMENT ', ifnull(quote(TABLE_COMMENT), '\"\"'), '
                     ROW FORMAT DELIMITED
                     FIELDS TERMINATED BY "\\t"
                     STORED AS TEXTFILE;
                     );')
                 from information_schema.tables
                 where table_schema = '${db_list}'
                     and table_name = '${table_list}'
                 ;
EOF
)
            export ${sql_1}

                 
             echo ''
             echo [Get the table_list on web]
             echo $sql_1
             #mysql --login-path=${src_login_path} -e "$sql_1" >
#$export_dir/$src_login_path/$project_name/${db_list}.${table_list}_schema.txt
#2>>$error_dir/$src_login_path/$project_name/${db_list}.${table_list}_schema.error
             #sed -i '1d'
#$export_dir/$src_login_path/$project_name/${db_list}.${table_list}_schema.txt
             #echo
#$export_dir/$src_login_path/$project_name/${db_list}.${table_list}_schema.txt
             #cat
#$export_dir/$src_login_path/$project_name/${db_list}.${table_list}_schema.txt
             #echo ''
             #cat
#$export_dir/$src_login_path/$project_name/${db_list}.${table_list}_schema.txt
# >> $export_dir/$src_login_path/$project_name/${db_list}.schema.txt

        done < $export_dir/$src_login_path/$project_name/$project_name.${org_id}_${db_list}_table_list.txt

         #echo ''
         #echo
#$export_dir/$src_login_path/$project_name/${db_list}.schema.txt
         #cat
#$export_dir/$src_login_path/$project_name/${db_list}.schema.txt

     done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_${org_id}_all_db.txt
done < $export_dir/$src_login_path/$project_name/$project_name.${src_login_path}_org_id.txt
