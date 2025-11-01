import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import shutil, os

# Configuração Spark
conf = SparkConf().setAppName("MinMaxPricePerCompanyYear").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# Remove saída antiga
saida = "outputMinMaxPrice"
if os.path.exists(saida):
    shutil.rmtree(saida)

# Lê CSV
rdd = sc.textFile("stock_details_5_years.csv")

# Remove cabeçalho
rddFiltered = rdd.filter(lambda line: not line.startswith("Date"))

# Separa colunas
rddColumns = rddFiltered.map(lambda line: line.split(","))

# Cria pares ((empresa, ano), (min_price, max_price))
def parse_line(c):
    try:
        date = c[0]
        year = date[:4]
        close_price = float(c[4])
        company = c[-1]
        return ((company, year), (close_price, close_price))
    except:
        return None

mapper = rddColumns.map(parse_line).filter(lambda x: x is not None)

# Reduz para achar min e max por (empresa, ano)
reducer = mapper.reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1])))

# Formata resultado
result = reducer.map(lambda kv: f"{kv[0][0]}\t{kv[0][1]}\tMin: {kv[1][0]:.2f}\tMax: {kv[1][1]:.2f}")

# Salva resultado
result.saveAsTextFile(saida)
