from pyspark import SparkConf, SparkContext
import os, shutil

conf = SparkConf().setAppName("BestThreeClose").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

def remove_dir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

saida = "outputBestThreeClose"
remove_dir(saida)

rdd = sc.textFile("stock_details_5_years.csv")
cabecalho = rdd.first()
rdd_f = rdd.filter(lambda line: line != cabecalho and ",Close" not in line)

# separa colunas e cria chave (empresa, ano-mes)
def parse_line(line):
    try:
        cols = line.split(',')
        empresa = cols[8].strip()
        close = float(cols[4])
        data = cols[0]
        ano, mes = data.split('-')[0], data.split('-')[1]
        ano_mes = f"{ano}-{mes}"
        return ((empresa, ano_mes), close)
    except:
        return None

pares = rdd_f.map(parse_line).filter(lambda x: x is not None)

# média mensal
soma_cont = pares.mapValues(lambda c: (c, 1)) \
                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

media_close = soma_cont.mapValues(lambda sc: sc[0] / sc[1])

# agrupa por empresa
agrupado = media_close.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                      .groupByKey()

# top 3 meses
def top3(valores):
    ordenado = sorted(valores, key=lambda v: v[1], reverse=True)
    top = ordenado[:3]
    return [f"{ym} -> Média: {avg:.2f}" for ym, avg in top]

melhores3 = agrupado.mapValues(top3)

# salva resultado
melhores3.map(lambda kv: f"{kv[0]}\t" + ", ".join(kv[1])).saveAsTextFile(saida)

