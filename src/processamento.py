from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum, avg, count, desc, round
import matplotlib.pyplot as plt

spark = SparkSession.builder\
.appName("Bolsa Familia")\
.config("spark.driver.memory", "8g") \
.config("spark.executor.memory", "8g") \
.config("spark.sql.shuffle.partitions", "50")\
.getOrCreate()

caminho_csv ="dados/pagamentos.csv"

df = spark.read\
.option ("header", True)\
.option ("inferSchema", True)\
.option ("sep", ";")\
.option ("encoding","ISO-8859-1")\
.csv(caminho_csv)

df.show(5)
df.printSchema()


######################################
#  PADRONIZAÇÃO DE DADOS 1º MÉTODO
######################################

df_tratado = df.withColumnRenamed(
    "MÊS COMPETÊNCIA", "mes_competencia")\
   .withColumnRenamed(
    "MÊS REFERÊNCIA", "mes_referencia")\
    .withColumnRenamed(
    "UF", "uf")\
    .withColumnRenamed(
    "CÓDIGO MUNICÍPIO SIAFI", "codigo_municipio_siafi")\
    .withColumnRenamed(
    "NOME MUNICÍPIO", "nome_municipio")\
    .withColumnRenamed(
    "CPF FAVORECIDO", "cpf_favorecido")\
    .withColumnRenamed(
    "NIS FAVORECIDO", "nis_favorecido")\
    .withColumnRenamed(
    "NOME FAVORECIDO", "nome_favorecido")\
    .withColumnRenamed(
    "VALOR PARCELA", "valor_parcela"
    )


######################################
#  PADRONIZAÇÃO DE DADOS 2º MÉTODO
######################################
#Criando Dicionário

# Usado para poucas colunas

colunas_padrao = {
    "MÊS COMPETÊNCIA": "mes_competencia",
    "MÊS REFERÊNCIA": "mes_referencia",
    "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio",
    "UF": "uf",
    "NOME MUNICÍPIO": "nome_municipio",
    "CPF FAVORECIDO": "cpf_favorecido",
    "NIS FAVORECIDO" : "nis_favorecido",
    "NOME FAVORECIDO": "nome_favorecido",
    "VALOR PARCELA": "valor_parcela"
}

for antiga, nova in colunas_padrao.items():
    df_tratado = df_tratado.withColumnRenamed(antiga,nova)


##################################################
#  PADRONIZAÇÃO DE DADOS - TRATAMENTO AUTOMÁTICO
##################################################

# Usado para muitas colunas

import unicodedata
import re

def padronizar_nome(col):
    col = col.lower()
    col = unicodedata.normalize("NFD", col)
    col = col.encode("ascii","ignore").decode("utf-8")
    col = re.sub(r"[^a-z0-9]+","_", col)
    col = col.strip("_")
    return col

df_tratado = df_tratado.toDF(
    * [padronizar_nome(c) for c in df_tratado.columns]
)

# Trata valores nulos de um coluna da tabela específico
# df_tratado = df_tratado.dropna(subset=["valor_parcela"]) 

# Trata valores nulos de tabela
df_tratado = df_tratado.dropna()

# Troca virgula por ponto
df_tratado = df_tratado.withColumn(
    "valor_parcela",
    regexp_replace(col("valor_parcela"), "," , ".")
)

df_tratado = df_tratado.withColumn(
    "valor_parcela",
    col("valor_parcela").cast("decimal(10,2)")
)

##################################################
#  TRATAMENTO AUTOMÁTICO - SIMPLIFICADO
##################################################

df_tratado = (
    df_tratado
    .dropna()
    .withColumn(
     "valor_parcela",
     regexp_replace(col("valor_parcela"), "," , "."))
     .withColumn(
    "valor_parcela",
    col("valor_parcela").cast("decimal(10,2)"))
)
df_tratado.show(5)
df_tratado.printSchema()

##################################################
#  SOMA E MÉDIA 
##################################################

df_tratado.agg(
    round(sum("valor_parcela"), 2).alias("total_pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento")
).show()

##################################################
#  AGRUPANDO POR UF EM ORDEM DECRESCENTE
##################################################

df_tratado.groupBy("uf")\
    .agg(sum("valor_parcela").alias("total_pago"))\
    .orderBy("total_pago" , ascending=False)\
    .show(26)

##################################################
#  RANKING TOP 10
##################################################

ranking_favorecido = df_tratado.groupBy("cpf_favorecido" , "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

print("Rankink dos 10 primeiros com valores acumulados")
ranking_favorecido.show(5, truncate=False)

##################################################
#  MEDIA GERAL POR PARCELA
##################################################

media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print(f"Para fins de comparação, media geral de cada parcela e R$ {media_geral:.2f}")

df_uf = df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .limit(10) \
    .toPandas()

##################################################
#  GRAFICOS
##################################################

plt.figure()
plt.bar(df_uf["uf"], df_uf["total_pago"])
plt.title("Top 10 UFs - Total Pago Bolsa Família")
plt.xlabel("UF")
plt.ylabel("Total Pago")
plt.show()




