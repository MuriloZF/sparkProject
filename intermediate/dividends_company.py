import findspark
findspark.init()

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DividendsCompany").setMaster("local[*]")
context = SparkContext.getOrCreate(conf = conf)

rdd = context.textFile("stock_details_5_years.csv")
rddFiltered = rdd.filter(lambda line : ",Close" not in line)
columns = rddFiltered.map(lambda line : line.split(","))
mapper = columns.map(lambda columns : ((columns[8], columns[0].split("-")[0]), (float(columns[6]), 1)))
sumDividends = mapper.reduceByKey(lambda a, b : (a[0] + b[0], a[1] + b[1]))
countAvg = sumDividends.mapValues(lambda a : (a[0], a[0] / a[1]))
reducerFormatted = countAvg.mapValues(lambda a : f"Total Dividends: ${a[0]:.2f}, Avg per Day: ${a[1]:.4f}")
reducerSorted = reducerFormatted.sortByKey()

reducerSorted.saveAsTextFile("output")