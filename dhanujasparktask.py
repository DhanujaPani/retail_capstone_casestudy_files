from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Dhanuja Spark Task").master("yarn").config("spark.sql.autoBroadcastJoinThreshold", "2").config("spark.sql.defaultSizeInBytes", "100000").config("spark.memory.fraction","0.5").enableHiveSupport().getOrCreate()


#####  Spark Task ###################################

from pyspark.sql.window import Window;

from pyspark.storagelevel import *;

from pyspark.sql.functions import *;

from pyspark.sql.types import *;

from pyspark.sql.types import *;

################################    Creating User defined Schema       #####################################################################
lineitem_schema=StructType([StructField('L_ORDERKEY',IntegerType()),StructField('L_PARTKEY',IntegerType()),StructField('L_SUPPKEY',IntegerType()),StructField('L_LINENUMBER',IntegerType()),StructField('L_QUANTITY',FloatType()),StructField('L_EXTENDEDPRICE',FloatType()),StructField('L_DISCOUNT',FloatType()),StructField('L_TAX',FloatType()),StructField('L_RETURNFLAG ',StringType()),StructField('L_LINESTATUS',StringType()),StructField('L_SHIPDATE',DateType()),StructField('L_COMMITDATE',DateType()),StructField('L_RECEIPTDATE',DateType()),StructField('L_SHIPINSTRUCT',StringType()),StructField('L_SHIPMODE',StringType()),StructField('L_COMMENT',StringType())]);

################################    Reading dataframe from csv       ######################################################################

lineitem=spark.read.options(delimiter='|').csv('/user/unextjun12/retail/lineitem.csv',header=True,schema=lineitem_schema);


################################    Using dateformat       ####################################################################

lineitem.select(date_format(col("l_commitdate"),"MMM-dd-yyyy")).fillna(value=0000-00-00,subset="l_commitdate").show();

################################    Using JDBC for dataframe creation    #############################################################


###                                                                                                                                partitionColumn is a column which should be used to determine partitions.                                                            lowerBound and upperBound determine range of values to be fetched.                                                                     numPartitions determines number of partitions to be created.                                                                                  Range between lowerBound and upperBound is divided into numPartitions each with stride equal to:                                                         Stride=upperBound / numPartitions - lowerBound / numPartitions
###

lineitemul=spark.read.format("jdbc").option("url", "jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/unextjun12?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "lineitem").option("lowerBound", "1").option("upperBound", "12000").option("numPartitions", "5").option("partitionColumn","l_orderkey").option("user", "unextjun12").option("password", "BdhData123").load();

################################   Writing dataframe into a Hive table  using partition and bucket  ########################################


spark.sql("drop table if exists unextjun12.lineitemul")

lineitemul.write.partitionBy("l_shipmode").bucketBy(5,"l_shipdate").saveAsTable("unextjun12.lineitemul");


################################    Performing transformation on a df ####################################################################

lineitemtransformations=spark.read.format("jdbc").option("url", "jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/unextjun12?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "lineitem").option("lowerBound", "1").option("upperBound", "12000").option("numPartitions", "5").option("partitionColumn","l_orderkey").option("user", "unextjun12").option("password", "BdhData123").load();

########### withna ,dropna filter,count,repace,denserank,withColumn,withColumnRenamed,alias,Window,orderBy,select,groupBy,desc ############
########## Finding ship mode with most no.of orders     ########################################################

lineitemtransformationsnew=lineitemtransformations.withColumn('l_shipmodenew',when(length('l_shipmode')==0,None).otherwise(col('l_shipmode'))).dropna(how='any',subset='l_shipmodenew').replace(['AIR','FOB','MAIL','RAIL','REG AIR','SHIP'],['A-AIR','F-FOB','M-MAIL','R-RAIL','RE-REG AIR','S-SHIP'],'l_shipmodenew').groupBy('l_shipmodenew').count().select('l_shipmodenew',col('count').alias('cnt')).withColumn('DNR',dense_rank().over(Window.orderBy(desc('cnt')))).filter("DNR=1").withColumnRenamed('DNR','Shipmode with most orders');


################################### Joins and Optimisation ##########################################################################
orders_schema=StructType([StructField('O_ORDERKEY',IntegerType()),StructField('O_CUSTKEY',IntegerType()),StructField('O_ORDERSTATUS',StringType()),StructField('O_TOTALPRICE',FloatType()),StructField('O_ORDERDATE',DateType()),StructField('O_ORDERPRIORITY',StringType()),StructField('O_CLERK',StringType()),StructField('O_SHIPPRIORITY',StringType()),StructField('O_COMMENT',StringType())]);

orders=spark.read.options(delimiter='|').csv('/user/unextjun12/retail/orders.csv',header=True,schema=orders_schema);

dfinner=lineitem.join(orders,lineitem.L_ORDERKEY==orders.O_ORDERKEY,"inner");

dfright=lineitem.join(orders,lineitem.L_ORDERKEY==orders.O_ORDERKEY,"right");

dfsemi=lineitem.join(orders,lineitem.L_ORDERKEY==orders.O_ORDERKEY,"leftsemi");

dfinner=lineitem.join(orders,lineitem.L_ORDERKEY==orders.O_ORDERKEY,"leftanti");


#########Broadcast Join #############################################################

dfbroadcast=lineitem.join(broadcast(orders),lineitem.L_ORDERKEY==orders.O_ORDERKEY,"left");
dfbroadcast.explain();



###  different file formats and codecs   ###################################


lineitem_schema=StructType([StructField('L_ORDERKEY',IntegerType()),StructField('L_PARTKEY',IntegerType()),StructField('L_SUPPKEY',IntegerType()),StructField('L_LINENUMBER',IntegerType()),StructField('L_QUANTITY',FloatType()),StructField('L_EXTENDEDPRICE',FloatType()),StructField('L_DISCOUNT',FloatType()),StructField('L_TAX',FloatType()),StructField('L_RETURNFLAG ',StringType()),StructField('L_LINESTATUS',StringType()),StructField('L_SHIPDATE',DateType()),StructField('L_COMMITDATE',DateType()),StructField('L_RECEIPTDATE',DateType()),StructField('L_SHIPINSTRUCT',StringType()),StructField('L_SHIPMODE',StringType()),StructField('L_COMMENT',StringType())]);
lineitem=spark.read.options(delimiter='|').csv('/user/unextjun12/retail/lineitem.csv',header=True,inferSchema=True);
lineitem.drop("L_RETURNFLAG");


###########   BZIP2 ##############################
lineitem.repartition(1).write.format("csv").option("codec","bzip2").mode("overwrite").save("retail/datasets/compressed/lineitembzip2/csv");
lineitem.repartition(1).write.format("avro").option("codec","bzip2").mode("overwrite").save("retail/datasets/compressed/lineitembzip2/avro");
lineitem.repartition(1).write.format("orc").option("codec","bzip2").mode("overwrite").save("retail/datasets/compressed/lineitembzip2/orc");
lineitem.repartition(1).write.format("parquet").option("codec","bzip2").mode("overwrite").save("retail/datasets/compressed/lineitembzip2/parquet");


###########   GZIP ##############################
lineitem.repartition(1).write.format("csv").option("codec","gzip").mode("overwrite").save("retail/datasets/compressed/lineitemgzip/csv");
lineitem.repartition(1).write.format("avro").option("codec","gzip").mode("overwrite").save("retail/datasets/compressed/lineitemgzip/avro");
lineitem.repartition(1).write.format("orc").option("codec","gzip").mode("overwrite").save("retail/datasets/compressed/lineitemgzip/orc");
lineitem.repartition(1).write.format("parquet").option("codec","gzip").mode("overwrite").save("retail/datasets/compressed/lineitemgzip/parquet");


###########   SNAPPY ##############################
lineitem.repartition(1).write.format("csv").option("codec","snappy").mode("overwrite").save("retail/datasets/compressed/lineitemsnappy/csv");
lineitem.repartition(1).write.format("avro").option("codec","snappy").mode("overwrite").save("retail/datasets/compressed/lineitemsnappy/avro");
lineitem.repartition(1).write.format("orc").option("codec","snappy").mode("overwrite").save("retail/datasets/compressed/lineitemsnappy/orc");
lineitem.repartition(1).write.format("parquet").option("codec","snappy").mode("overwrite").save("retail/datasets/compressed/lineitemsnappy/parquet");


###  timetravel  ###################################


customerdelta = spark.read.options(delimiter='|').csv("retail/customer.csv",header=True,inferSchema=True);
customerdelta.write.format("delta").mode("overwrite").save("datasets/delta/customerdeltanew1");

df = spark.read.options(delimiter='|').csv("retail/customernew.csv",header=True,inferSchema=True);
df.write.format("delta").mode("overwrite").save("datasets/delta/customerdeltanew1");

spark.read.format("delta").option("versionAsOf", 0).load("datasets/delta/customerdeltanew1").show();
spark.read.format("delta").option("versionAsOf", 1).load("datasets/delta/customerdeltanew1").show();

### persist/cache   ###################################


customerpersist_m_o= spark.read.options(delimiter='|').csv("retail/customer.csv",header=True,inferSchema=True);
customerpersist_m_o.rdd.persist(StorageLevel.MEMORY_ONLY);
print(customerpersist_m_o.rdd.getStorageLevel());



customerpersist_m_o_2= spark.read.options(delimiter='|').csv("retail/customer.csv",header=True,inferSchema=True);
customerpersist_m_o_2.rdd.persist(StorageLevel.MEMORY_ONLY_2);
print(customerpersist_m_o_2.rdd.getStorageLevel());


lineitempersist_m_d= spark.read.options(delimiter='|').csv("retail/lineitem.csv",header=True,inferSchema=True);
lineitempersist_m_d.rdd.persist(StorageLevel.MEMORY_AND_DISK);
print(lineitempersist_m_d.rdd.getStorageLevel());


