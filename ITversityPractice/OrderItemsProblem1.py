from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import functions as func
import datetime as dt

def initValue(x):
	items = list()
	items.append(x[1])
	return (x[0], items)

def mergeValue(x, y):
	x[1].append(y[1])
	return (x[0] + y[0], x[1])

def mergeCombiner(x, y):
	x[1].append(y[1])
	return (x[0] + y[0], x[1])
#
# http://apache-spark-user-list.1001560.n3.nabble.com/How-to-add-jars-to-standalone-pyspark-program-td22685.html
# http://alvincjin.blogspot.com/2015/03/spark-overwrite-csv-file.html
#

conf = SparkConf().setAppName("OrderProcessor").setMaster("local") \
						.set("spark.jars", "/Users/serranm1/apps/spark-2.0.0-bin-hadoop2.7/jars/spark-avro_2.11-4.0.0.jar") \
						.set("spark.hadoop.validateOutputSpecs", "false")

sc = SparkContext(conf = conf)

sqlContext = SQLContext(sc)

orders = sqlContext.read.format("com.databricks.spark.avro")\
						.load("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem1/orders/part-m-00000.avro")

orderItems = sqlContext.read.format("com.databricks.spark.avro")\
						.load("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem1/order_items/part-m-00000.avro")

orderJoin = orders.join(orderItems, orders["order_id"] == orderItems["order_item_order_id"])


print dt.date.fromtimestamp(1375070400000/1000).strftime('%Y-%m-%d')

dataFrameResult = orderJoin.groupBy(func.to_date(func.from_unixtime(func.col("order_date") / 1000)).alias("order_date"), "order_status")\
			.agg(func.sum("order_item_subtotal").alias("total_amount"), func.countDistinct("order_id").alias("total_orders"))\
			.orderBy(func.desc("order_date"), "order_status", func.desc("total_amount"), "total_orders")

dataFrameResult.show()

print "-----------------------------------------------------------------------------------------------------------------"
orderJoin.registerTempTable("order_joined");

sqlResult = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, " +
									"cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders " +
									"from order_joined group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status " +
									"order by order_formatted_date desc,order_status,total_amount desc, total_orders");
sqlResult.show()

print "-----------------------------------------------------------------------------------------------------------------"

## map( (order_date, order_status), (price, order_id) )
combByKeyResult = orderJoin.rdd.map(lambda x : ((str(x[1]), str(x[3])), (float(str(x[8])), str(x[0])))) \
		  .combineByKey(
										initValue,
										mergeValue,
										mergeCombiner
									) \
		  .map(lambda x : (x[0][0], x[0][1], x[1][0], len(x[1][1])) ) \
		  .toDF() \
		  .orderBy(func.desc("_1"), func.col("_2"), func.desc("_3"), func.col("_4"))

combByKeyResult.show()

print "-----------------------------------------------------------------------------------------------------------------"
print "Save result as CVS"

dataFrameResult.rdd.coalesce(1, True).map(lambda x: str(x[0]) + "," + str(x[1]) + "," + str(x[2]) + "," + str(x[3])).saveAsTextFile("dataFrameResult.cvs")
sqlResult.rdd.coalesce(1, True).map(lambda x: str(x[0]) + "," + str(x[1]) + "," + str(x[2]) + "," + str(x[3])).saveAsTextFile("sqlResult.cvs")
combByKeyResult.rdd.coalesce(1, True).map(lambda x : str(x[0]) + "," + str(x[1]) + "," + str(x[2]) + "," + str(x[3])).saveAsTextFile("combByKeyResult.cvs")

print "-----------------------------------------------------------------------------------------------------------------"
print "Compress result as GZIP"

sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
dataFrameResult.coalesce(1).write.mode("overwrite").parquet("dataFrameResult.gzip")
sqlResult.coalesce(1).write.mode("overwrite").parquet("sqlResult.gzip")
combByKeyResult.coalesce(1).write.mode("overwrite").parquet("combByKeyResult.gzip")

print "-----------------------------------------------------------------------------------------------------------------"
print "Compress result as SNAPPY"
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
dataFrameResult.coalesce(1).write.mode("overwrite").parquet("dataFrameResult-snappy")
sqlResult.coalesce(1).write.mode("overwrite").parquet("sqlResult-snappy")
combByKeyResult.coalesce(1).write.mode("overwrite").parquet("combByKeyResult-snappy")

print "-----------------------------------------------------------------------------------------------------------------"


'''
var comByKeyResult = 
joinedOrderDataDF.
map(x=> ((x(1).toString,x(3).toString),(x(8).toString.toFloat,x(0).toString))).
combineByKey(
					(x:(Float, String)) => (x._1, Set(x._2)),
					(x:(Float, Set[String]), y:(Float, String)) => (x._1 + y._1, x._2 + y._2),
			   	(x:(Float, Set[String]), y:(Float, Set[String])) => (x._1 + y._1, x._2 ++ y._2)
			   ).
map(x => (x._1._1, x._1._2, x._2._1, x._2._2.size)).
toDF().
orderBy(col("_1").desc, col("_2"), col("_3").desc, col("_4"));

comByKeyResult.show();
'''
print "C'est fini !!"

