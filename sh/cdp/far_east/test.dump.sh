
mysql --login-path=datapool_prod -e "select * from far_east.fpc_stat;" > fpc_stat.tsv
echo 11111111
cat fpc_stat.tsv
