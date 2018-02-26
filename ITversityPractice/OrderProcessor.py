from pyspark import SparkConf, SparkContext

def parseNames(line):
    fields = line.split(',')
	 ## order_id -> (order_date, order_customer_id, order_status)
    return (int(fields[0]), ( fields[1].encode("utf8"), int(fields[2]), fields[3].encode("utf8") ))

def parseDateByYearMonth(line):
	fields = line.split(' ')
	fechas = fields[0].split('-')
	return fechas[0] + "-" + fechas[1]

def parseDateByYear(line):
	fields = line.split(' ')
	fechas = fields[0].split('-')
	return fechas[0]

def printReportByYearSortedByStatus(line):
	llave = line[1]
	print "Fecha {0}, Total {1}".format(line[0], line[1][0][0])
	myTuple = []
	for item in llave:
		myTuple.append(item[1])

	llaveSorted = sorted(myTuple, key=getValue, reverse=True)
	for item in llaveSorted:
		print "\t\t {0:20} \t {1}".format(item[0], item[1])
	print "---------------------------------------------"


def getKey(item):
	return item[0]

def getValue(item):
	return item[1]

def orderStatusTop100(orderRDD):
	## map(order_status, 1) -> reduceByKey -> map(counter, order_status) -> sortByKey descending
	orderStatus = orderRDD.map(lambda x: (x[1][2], 1)) \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda x: (x[1], x[0]))

	for os in orderStatus.sortByKey(False).collect():
		print os
	print "---------------------------------------------------"

def orderDate(orderRDD):
	# map (order_date, 1) -> reduceByKey
	orderDate = orderRDD.map(lambda x: (x[1][0], 1))\
		.reduceByKey(lambda x, y : x + y)

	#for od in orderDate.sortByKey(False).collect():
	#	print od
	#print "---------------------------------------------------"

	# map ( (order_date, order_status), 1) -> reduceByKey -> map (order_date, (order_status, counter) )
	orderDateStatus = orderRDD.map(lambda x : ((x[1][0], x[1][2]), 1))\
		.reduceByKey(lambda x, y : x + y)\
		.map(lambda x : (x[0][0], (x[0][1], x[1])))

	#for ods in orderDateStatus.sortByKey(False).collect():
	#	print ods
	#print "---------------------------------------------------"

	# join
	# orderJoin = orderDate.join(orderDateStatus)

	# join -> filter by 2014-07
	orderJoin = orderDate.join(orderDateStatus).filter(lambda x : "2014-07" in x[0])
	for oj in orderJoin.sortByKey(False).collect():
		print oj
	print "---------------------------------------------------"

def orderDateByMonth(orderRDD):
	# map by YYYY-MM -> reduceByKey
	orderDate = orderRDD.map(lambda x : (parseDateByYearMonth(x[1][0]), 1))\
								.reduceByKey(lambda x, y : x + y)

	# for od in orderDate.sortByKey(False).collect():
	#	print od
	# print "---------------------------------------------------"

	# map by (YYYY-MM, order_status) -> reduceByKey -> map (date, (order_status, counter))
	orderDateStatus = orderRDD.map(lambda x : ((parseDateByYearMonth(x[1][0]), x[1][2]), 1))\
										.reduceByKey(lambda x, y : x + y)\
										.map(lambda x : (x[0][0], (x[0][1], x[1])))

	# for ods in orderDateStatus.sortByKey(False).collect():
	# 	print ods
	# print "---------------------------------------------------"

	orderJoin = orderDate.join(orderDateStatus)

	# filter by 2014-07
	# orderJoin = orderDate.join(orderDateStatus).filter(lambda x : "2014-07" in x[0])

	'''
	bandera = ""
	for oj in orderJoin.sortByKey(False).collect():
		if bandera != oj[0]:
			bandera = oj[0]
			print "Fecha : {0}, Total {1}".format(oj[0], oj[1][0])

		print "\t\t {0:20} \t {1}".format(oj[1][1][0], oj[1][1][1])
	'''

	orderJoin.groupByKey().sortByKey(False).mapValues(list).foreach(printReportByYearSortedByStatus)

	print "---------------------------------------------------"

def orderDateByYear(orderRDD):
	# map by YYYY -> reduceByKey
	orderDate = orderRDD.map(lambda x : (parseDateByYear(x[1][0]) , 1))\
								.reduceByKey(lambda x, y : x + y)

	# map by (YYYY, order_status) -> reduceByKey -> map (date, (order_status, counter))
	orderDateStatus = orderRDD.map(lambda x : ((parseDateByYear(x[1][0]), x[1][2]), 1))\
										.reduceByKey(lambda x, y : x + y)\
										.map(lambda x : (x[0][0], (x[0][1], x[1])))

	orderJoin = orderDate.join(orderDateStatus)

	'''
	bandera = ""
	for oj in orderJoin.sortByKey(False).collect():
	 	if bandera != oj[0]:
			bandera = oj[0]
			print "Fecha : {0}, Total {1}".format(oj[0], oj[1][0])
		print "\t\t {0:20} \t {1}".format(oj[1][1][0], oj[1][1][1])
	'''

	orderJoin.groupByKey()\
				.sortByKey(False)\
				.mapValues(list)\
				.foreach(printReportByYearSortedByStatus)

	print "---------------------------------------------------"



###--------------------------------------------------------------------------------
###--------------------------------------------------------------------------------

conf = SparkConf().setAppName("OrderProcessor").setMaster("local")
sc = SparkContext(conf = conf)

orders = sc.textFile("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/orders.txt")

orderRDD = orders.map(parseNames)

# orderStatusTop100(orderRDD)

# orderDate(orderRDD)

orderDateByMonth(orderRDD)

orderDateByYear(orderRDD)