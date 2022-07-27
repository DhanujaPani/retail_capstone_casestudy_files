dir="/home/unextjun12/retail/Retail"
echo "use unextjun12;">>./loadmysql.sql
for i in $(ls $dir) 
do                                                                                                                                             
    tn=$(echo $i|cut -d. -f 1)                                                                                                                 
    path=$(echo $dir/$i)                                                                                                         
    echo "load data local infile '"$path"' into table "$tn" fields terminated by '|' lines terminated by '\n'  ignore 1 rows;">>./loadmysql.sql                   
done
mysql -u unextjun12 -p <./retail_mysql_schema.sql
mysql -u unextjun12 -p <./loadmysql.sql