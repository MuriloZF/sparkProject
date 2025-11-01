import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import shutil, os

conf = SparkConf().setAppName("MinMaxClose").setMaster("local[*]")
context = SparkContext.getOrCreate(conf = conf)

if os.path.exists("outputMinMaxClose"):
    shutil.rmtree("outputMinMaxClose")

# lê o arquivo
rdd = context.textFile("stock_details_5_years.csv")

# remove cabeçalho
rddFiltered = rdd.filter(lambda line: "CLOSE" not in line.upper())

# separa colunas
rddColumns = rddFiltered.map(lambda line: line.split(","))

# cria pares (empresa, (close, data))
mapper = rddColumns.map(lambda c: (c[8], (float(c[4]), c[0])))

# reduz para menor e maior fechamento
reducer = mapper.reduceByKey(lambda a, b: (
    a if a[0] < b[0] else b,  # menor
    a if a[0] > b[0] else b   # maior
))

# formata resultado
result = reducer.mapValues(lambda v: f"Min close: {v[0][0]} em {v[0][1]}; Max close: {v[1][0]} em {v[1][1]}")

# salva resultado
result.saveAsTextFile("outputMinMaxClose")
