from pyspark import SparkConf, SparkContext
import os
import shutil

config = SparkConf().setAppName("Rank").setMaster("local")
sparkContext = SparkContext.getOrCreate(conf=config)
rdd = sparkContext.textFile("../stock_details_5_years.csv")

rddF = rdd.filter(lambda line: not line.startswith("Date") and ",Close" not in line)

def parse_line(line):
    cols = line.split(",")
    try:
        company = cols[8].strip()
        open_price = float(cols[1])
        close_price = float(cols[4])
        if open_price == 0:
            return None
        ratio = close_price / open_price
        return (company, ratio)
    except (IndexError, ValueError, ZeroDivisionError):
        return None

pairs = rddF.map(parse_line).filter(lambda x: x is not None)   
sum_count = pairs.map(lambda kv: (kv[0], (kv[1], 1))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

averages = sum_count.mapValues(lambda sc: sc[0] / float(sc[1]))  
ranked = averages.sortBy(lambda kv: kv[1], ascending=False).zipWithIndex().map(lambda kv_idx: (kv_idx[1] + 1, (kv_idx[0][0], kv_idx[0][1])))

def removeDir(directory):
	if os.path.exists(directory):
		shutil.rmtree(directory)

directory : str = "outputRank"

removeDir(directory)
ranked.map(lambda kv: f"{kv[0]}\t{kv[1][0]}\t{kv[1][1]:.6f}").saveAsTextFile(directory)
