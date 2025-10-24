import findspark
findspark.init()

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("VolatilityPerMonth").setMaster("local[*]")
context = SparkContext.getOrCreate(conf = conf)

rdd = context.textFile("stock_details_5_years.csv")
rddFiltered = rdd.filter(lambda line : ",Close" not in line)
rddColumns = rddFiltered.map(lambda line : line.split(","))

mapper = rddColumns.map(lambda column : ((column[8], f"{column[0].split('-')[0]}-{column[0].split('-')[1]}"), (float(column[4]), float(column[4]))))

reducerMinMax = mapper.reduceByKey(lambda a, b : (max(a[0], b[0]), min(a[1],b[1])))
reducerVolatility = reducerMinMax.mapValues(lambda values : f"Volatility: {values[0] - values[1]:.2f}")

reducerSort = reducerVolatility.sortByKey()

reducerSort.saveAsTextFile("output")
