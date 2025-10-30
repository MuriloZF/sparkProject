from pyspark import SparkConf, SparkContext
import os
import shutil

config = SparkConf().setAppName("allOcurrencesVale").setMaster("local")
sparkContext = SparkContext.getOrCreate(conf = config)
rdd = sparkContext.textFile("../stock_details_5_years.csv")
rddF = rdd.filter(lambda line : not "Close" in line)
rdd_cols = rddF.map(lambda line : line.split(","))
rdd_vale = rdd_cols.filter(lambda cols : cols[8].upper() == "VALE").map(lambda cols : ",".join(cols))
def removeDir(directory):
	if os.path.exists(directory):
		shutil.rmtree(directory)

directory :str = "outputAllVale"

removeDir(directory)
rdd_vale.saveAsTextFile(directory)
