from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as func

class Product(object):
	def __init__(self, productID, productCatID, productName, productDesc, productPrice, productImage):
		self.productID = productID
		self.productCatID = productCatID
		self.productName = productName
		self.productDesc = productDesc
		self.productPrice = productPrice
		self.productImage = productImage

	def __str__(self):
		return self.toString()


def parseFile(line):
	fields = line.split("|")
	##return (int(fields[0]), int(fields[1]), fields[2].encode("utf8"), fields[3].encode("utf8"), float(fields[4]), fields[5].encode("utf8"))
	return Product(int(fields[0]), int(fields[1]), fields[2].encode("utf8"), fields[3].encode("utf8"), float(fields[4]), fields[5].encode("utf8"))

schema = StructType( [
	StructField('productID', IntegerType()),
	StructField('productCatID', IntegerType()),
	StructField('productName', StringType()),
	StructField('productDesc', StringType()),
	StructField('productPrice', FloatType()),
	StructField('productImage', StringType())
	]
)

schemaRDDResults = StructType( [
	StructField('productCatID', IntegerType()),
	StructField('max_price', FloatType()),
	StructField('tot_products', IntegerType()),
	StructField('avg_price', FloatType()),
	StructField('min_price', FloatType())
	]
)

conf = SparkConf().setAppName("Products Problem 2").setMaster("local") \
						.set("spark.jars", "/Users/serranm1/apps/spark-2.0.0-bin-hadoop2.7/jars/spark-avro_2.11-4.0.0.jar") \
						.set("spark.hadoop.validateOutputSpecs", "false")

sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

products = sc.textFile("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem2/products/part-m-00000").map(parseFile)
productsDF = sqlContext.createDataFrame(products, schema)

dataFrameResult = productsDF.filter("productPrice < 100") \
										.groupBy(func.col("productCatID")) \
										.agg( \
											func.max(func.col("productPrice")).alias("max_price"), \
											func.countDistinct(func.col("productID")).alias("tot_products"), \
											func.round(func.avg(func.col("productPrice")), 2).alias("avg_price"), \
											func.min(func.col("productPrice")).alias("min_price")
										) \
										.orderBy(func.col("productCatID"))

dataFrameResult.show()

'''
+------------+---------+------------+---------+---------+
|productCatID|max_price|tot_products|avg_price|min_price|
+------------+---------+------------+---------+---------+
|           2|    99.99|          11|    66.81|    29.97|
|           3|     99.0|          19|    55.73|      0.0|
|           4|    99.95|          10|    55.89|    21.99|
|           5|    99.99|          13|    57.99|     14.0|
|           6|    99.99|          19|    43.94|     14.0|
|           7|    99.98|          18|    47.49|     14.0|
|           8|    99.98|          19|    41.67|    21.99|

var dataFrameResult = productsDF.filter("productPrice < 100")
											.groupBy(col("productCategory"))
											.agg(
												max(col("productPrice")).alias("max_price"),
												countDistinct(col("productID")).alias("tot_products"),
												round(avg(col("productPrice")), 2).alias("avg_price"),
												min(col("productPrice")).alias("min_price")
												)
											.orderBy(col("productCategory"));
scala> dataFrameResult.show();
'''
productsDF.registerTempTable("products")
sqlResult = sqlContext.sql("select productCatID, max(productPrice) as maximum_price," \
									+ " count(distinct(productId)) as total_products," \
									+ " cast(avg(productPrice) as decimal(10, 2)) as average_price," \
									+ " min(productPrice) as minimum_price" \
									+ " from products" \
									+ " where productPrice < 100" \
									+ " group by productCatId" \
									+ " order by productCatId")
sqlResult.show()
'''
productsDF.registerTempTable("products");
var sqlResult = sqlContext.sql("select product_category_id, max(product_price) as maximum_price, 
													count(distinct(product_id)) as total_products, 
													cast(avg(product_price) as decimal(10,2)) as average_price, 
													min(product_price) as minimum_price 
											 from products 
											where product_price <100 
										   group by product_category_id 
										   order by product_category_id desc");
sqlResult.show();
'''
## filter by productPrice < 100 -> map (productCatId, (productId, productCatId, productPrice)
## -> agregateByKey by productCatId ( max (productPrice), productId total, productPrice total, min(productPrice) )
## -> map (productCatId, maxProductPrice, total num of ProductId, totalProductPrice / total num of ProductId as average, minProductPrice
rddResult = productsDF.rdd.filter(lambda x : float(x[4] < 100))\
	                        .map(lambda x : (int(x[1]), (int(x[0]), int(x[1]), float(x[4])) ) ) \
									.aggregateByKey( (0, 0, 0, 999999) , \
														  lambda x, y : (max(x[0], y[2]), x[1] + 1, x[2] + y[2], min(x[3], y[2])), \
														  lambda x, y : (max(x[0], y[2]), x[1] + y[1], x[2] + y[2], min(x[3], y[2])) \
														) \
									.map(lambda x : (int(x[0]), float(x[1][0]), int(x[1][1]), float(x[1][2] / x[1][1]), float(x[1][3])))

rddResultDF = sqlContext.createDataFrame(rddResult, schemaRDDResults)

for item in rddResult.collect():
	print item
'''

									.aggregateByKey( 0, 0, 0, 0 ) \
														( \
															lambda (x, y) : ( max(x[2], y), x[0] + y, x[2] + 1, min(x[2], y) ), \
															lambda (x, y) : ( max(x[2], y[2]), x[0] + y[0], x[2] + y[2], min(x[2], y[2]) ), \
														) \
	                        .map(lambda x : (x[1], x[2][1], (x[2][2] / x[2][3]), x[2][3], x[2][4] ) )


var rddResult = productsDF.map(x => ( x(1).toString.toInt, x(4).toString.toDouble) )
									.aggregateByKey( (0.0, 0.0, 0, 9999999999999.0) )
									               (
															(x, y) => ( math.max(x._1, y), x._2 + y, x._3 + 1, math.min(x._4, y) ),
															(x, y) => ( math.max(x._1, y._1), x._2 + y._2, x._3 + y._3, math.min(x._4, y._4) ) 
														)
									.map(x => (x._1, x._2._1, (x._2._2 / x._2._3), x._2._3, x._2._4) )
									.sortBy(_._1, false);
rddResult.collect().foreach(println);
'''

print "Saving files ..."

sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
dataFrameResult.coalesce(1).write.mode("overwrite").parquet("dataFrameResult-snappy")
sqlResult.coalesce(1).write.mode("overwrite").parquet("sqlResult-snappy")
rddResultDF.coalesce(1).write.mode("overwrite").parquet("rddResult-snappy")

print "Cest fini !!"