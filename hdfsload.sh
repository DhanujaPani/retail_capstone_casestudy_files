dir="/home/unextjun12/retail/Retail"
hdfs dfs -mkdir retail
for i in $(ls $dir) 
do                                                                                                                                             
    hdfs dfs -copyFromLocal "$dir"/"$i" retail                  
done