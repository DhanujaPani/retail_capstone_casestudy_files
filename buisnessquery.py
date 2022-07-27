from pyspark.sql.window import Window;

from pyspark.sql.functions import *;

from pyspark.sql.types import *;


lineitem_schema=StructType([StructField('L_ORDERKEY',IntegerType()),StructField('L_PARTKEY',IntegerType()),StructField('L_SUPPKEY',IntegerType()),StructField('L_LINENUMBER',IntegerType()),StructField('L_QUANTITY',FloatType()),StructField('L_EXTENDEDPRICE',FloatType()),StructField('L_DISCOUNT',FloatType()),StructField('L_TAX',FloatType()),StructField('L_RETURNFLAG ',StringType()),StructField('L_LINESTATUS',StringType()),StructField('L_SHIPDATE',DateType()),StructField('L_COMMITDATE',DateType()),StructField('L_RECEIPTDATE',DateType()),StructField('L_SHIPINSTRUCT',StringType()),StructField('L_SHIPMODE',StringType()),StructField('L_COMMENT',StringType())]);

lineitem=spark.read.options(delimiter='|').csv('/user/unextjun12/retail/lineitem.csv',header=True,schema=lineitem_schema);


lineitemnew=lineitem.withColumn('L_DISCOUNTAMT',col('L_DISCOUNT')*col('L_EXTENDEDPRICE')).withColumn('L_TAXAMT',col('L_TAX')*col('L_EXTENDEDPRICE')).withColumn('L_DISCOUNT_EXTENDED_PRICE',col('L_EXTENDEDPRICE')*col('L_DISCOUNTAMT')).withColumn('L_DISCOUNT_EXTENDEDPRICE_PLUS_ TAX',col('L_DISCOUNT_EXTENDED_PRICE')*col('L_TAXAMT')).withColumn('L_MAXSHIPDATE',max('L_SHIPDATE').over(Window.orderBy(desc('L_SHIPDATE'))));

lineitemnew=lineitemnew.filter(datediff(col('L_MAXSHIPDATE'),col('L_SHIPDATE'))<=120)



lineitemnew1=lineitemnew.withColumn('TOT_EXTENDEDPRICE',sum('L_EXTENDEDPRICE').over(Window.partitionBy('L_RETURNFLAG ','L_LINESTATUS'))).withColumn('TOT_DISCOUNT_EXTENDED_PRICE',sum('L_DISCOUNT_EXTENDED_PRICE').over(Window.partitionBy('L_RETURNFLAG ','L_LINESTATUS'))).withColumn('TOT_DISCOUNT_EXTENDEDPRICE_PLUS_TAX',sum('L_DISCOUNT_EXTENDEDPRICE_PLUS_ TAX').over(Window.partitionBy('L_RETURNFLAG ','L_LINESTATUS'))).withColumn('AVG_QUANTITY',avg('L_QUANTITY').over(Window.partitionBy('L_RETURNFLAG ','L_LINESTATUS'))).withColumn('AVG_EXTENDEDPRICE',avg('L_EXTENDEDPRICE').over(Window.partitionBy('L_RETURNFLAG ','L_LINESTATUS'))).withColumn('AVG_DISCOUNT',avg('L_DISCOUNT').over(Window.partitionBy('L_RETURNFLAG ','L_LINESTATUS')));

lineitem_all=lineitemnew1.select('L_LINESTATUS','L_RETURNFLAG ','TOT_EXTENDEDPRICE','TOT_DISCOUNT_EXTENDED_PRICE','TOT_DISCOUNT_EXTENDEDPRICE_PLUS_TAX','AVG_QUANTITY','AVG_EXTENDEDPRICE','AVG_DISCOUNT').distinct()

lineitem_cnt=lineitemnew1.select('L_RETURNFLAG ','L_LINESTATUS').groupBy('L_RETURNFLAG ','L_LINESTATUS').count()


lineitem_joined=lineitem_all.join(lineitem_cnt,['L_RETURNFLAG ','L_LINESTATUS'],'left')


lineitem_report=lineitem_joined.select('L_RETURNFLAG ','L_LINESTATUS',col('TOT_EXTENDEDPRICE').cast('bigint'),col('TOT_DISCOUNT_EXTENDED_PRICE').cast('bigint'),col('TOT_DISCOUNT_EXTENDEDPRICE_PLUS_TAX').cast('bigint'),col('AVG_QUANTITY').cast('bigint'),col('AVG_EXTENDEDPRICE').cast('bigint'),col('AVG_DISCOUNT').cast('bigint'),'count')


lineitem_report.show()