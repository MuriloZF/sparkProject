from pyspark import SparkConf, SparkContext
import os
import shutil

config = SparkConf().setAppName("allOcurrencesVale").setMaster("local")
sparkContext = SparkContext.getOrCreate(conf = config)
rdd = sparkContext.textFile("../stock_details_5_years.csv")
rddF = rdd.filter(lambda line : not "CLOSE" in line)
rddMap = rddF.map(lambda line : line.split(",")[3])

def montaTupla(line):
	columns = line.split(",")
	model : str = columns[8]

