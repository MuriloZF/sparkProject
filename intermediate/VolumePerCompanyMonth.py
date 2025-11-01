import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import shutil, os

conf = SparkConf().setAppName("VolumePerCompanyMonth").setMaster("local[*]")
context = SparkContext.getOrCreate(conf=conf)

# remove pasta antiga
if os.path.exists("outputVolumePerCompanyMonth"):
    shutil.rmtree("outputVolumePerCompanyMonth")

# lê o arquivo
rdd = context.textFile("stock_details_5_years.csv")

# remove cabeçalho
rddFiltered = rdd.filter(lambda line: not line.startswith("Date"))

# separa colunas
rddColumns = rddFiltered.map(lambda line: line.split(","))

# cria pares ((empresa, ano-mes), volume)
def parse_line(c):
    try:
        date = c[0]
        volume = int(c[5].replace(",", ""))   # remove vírgulas do volume
        company = c[-1]
        year_month = date[:7]                 # YYYY-MM
        return ((company, year_month), volume)
    except:
        return None

mapper = rddColumns.map(parse_line).filter(lambda x: x is not None)

# soma volumes por empresa e mês
reducer = mapper.reduceByKey(lambda a, b: a + b)

# formata resultado
result = reducer.map(lambda kv: f"{kv[0][0]}\t{kv[0][1]}\tTotal Volume: {kv[1]}")

# salva saída
result.saveAsTextFile("outputVolumePerCompanyMonth")
