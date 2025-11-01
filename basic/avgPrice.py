from pyspark import SparkContext, SparkConf

# Configuração básica do Spark
conf = SparkConf().setAppName("PrecoMedioPorEmpresa").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

# Carregar o arquivo CSV
rdd = sc.textFile("stock_details_5_years.csv")

# Remover o cabeçalho
header = rdd.first()
dados = rdd.filter(lambda linha: linha != header)

# Função para extrair (empresa, (soma, contagem))
def extrair_campos(linha):
    col = linha.split(",")
    try:
        high = float(col[2])
        low = float(col[3])
        close = float(col[4])
        open_ = float(col[1])
        empresa = col[8]
        total = high + low + close + open_
        return (empresa, (total, 4))
    except:
        return None

# Mapear e remover erros
pares = dados.map(extrair_campos).filter(lambda x: x is not None)

# Somar totais e contagens por empresa
soma = pares.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Calcular média
media = soma.mapValues(lambda x: x[0] / x[1])

# Mostrar resultados
for empresa, preco_medio in media.collect():
    print(f"{empresa}: Preço médio = ${preco_medio:.2f}")

# Parar contexto
sc.stop()
