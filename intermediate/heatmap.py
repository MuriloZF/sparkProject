from pyspark import SparkConf, SparkContext
from datetime import datetime
import os
import shutil

config = SparkConf().setAppName("heatmap").setMaster("local")
sparkContext = SparkContext.getOrCreate(conf = config)
rdd = sparkContext.textFile("../stock_details_5_years.csv")
rddF = rdd.filter(lambda line : not "Close" in line)

def mapper(line):
    cols = line.split(",")
    date = datetime.strptime(cols[0][:10], "%Y-%m-%d")
    month = date.strftime("%B")
    day = date.strftime("%A")
    company = cols[8]
    volume = int(cols[5])
    return [
        (("Month", company, month), volume),
        (("Day",   company, day),   volume),
    ]

pairs = rddF.flatMap(mapper)
totals = pairs.reduceByKey(lambda a, b : a + b)
sumCount = pairs.mapValues(lambda v : (v, 1)).reduceByKey(lambda a, b : (a[0] + b[0], a[1] + b[1]))
averages = sumCount.mapValues(lambda sc: sc[0] /
float(sc[1]))
result = averages.map(lambda kv: (
    f"{kv[0][0]}: {kv[0][2]} | Company: {kv[0][1]} |",
    f" Average: {kv[1]:.2f}"
))

def removeDir(directory):
	if os.path.exists(directory):
		shutil.rmtree(directory)

directory :str = "outputHeatmap"
removeDir(directory)
result.saveAsTextFile(directory)
