export sql="
replace(json_array(page_keyword), ', ', '\", \"') keyword
;"

echo $sql
