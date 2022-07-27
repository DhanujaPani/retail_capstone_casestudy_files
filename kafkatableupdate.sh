dir="/home/unextjun12/kafkafiles/files"
echo "use dhanuja_hive;">>./kafkatable.sql
for i in $(ls $dir) 
do                                                                                                                                             
    tn=$(echo $i|cut -d. -f 1)                                                                                                                 
    path=$(echo $dir/$i)                                                                                                         
    echo "load data local inpath '"$path"' into table "$tn" fields terminated by '|' lines terminated by '\n';">>./kafkatable.sql                  
done
cd  /home/unextjun12/kafkafiles
hive -f hivekafkaschema.sql
cd /home/unextjun12/kafkafiles/files
hive -f kafkatable.sql