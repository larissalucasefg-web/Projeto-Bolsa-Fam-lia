
#=========================================================
# IMPORTANDO O SPARKSESSION
#=========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum, avg, count, desc, round, substring 
import matplotlib.pyplot as plt
import unicodedata
import re
#=========================================================
# CONSTRUINDO O AMBIENTE DE BIGDATA
#=========================================================

spark = SparkSession.builder\
.appName("Bolsa Familia")\
.config("spark.driver.memory", "8g") \
.config("spark.executor.memory", "8g") \
.config("spark.sql.shuffle.partitions", "50")\
.getOrCreate()

#criando o DataFrame
caminho_csv ="dados/pagamentos.csv"

df = spark.read\
.option ("header", True)\
.option ("inferSchema", True)\
.option ("sep", ";")\
.option ("encoding","ISO-8859-1")\
.csv(caminho_csv)

#lendo as cinco primeiras linha do data Frame
df.show(5)
df.printSchema()


#=========================================================
# PADRONIZANDO COLUNAS
#=========================================================

# Tabela com poucas colunas
# df_tratado = df_tratado.withColumnRenamed(\
#     "VALOR PARCELA", "valor_parcela")\
#     .withColumnRenamed(\
#         "MÊS COMPETÊNCIA", "mes_competencia")\
#     .withColumnRenamed(\
#         "MÊS REFERÊNCIA", "mes_referencia")\
#     .withColumnRenamed(\
#         "UF", "uf")\
#     .withColumnRenamed(\
#         "CÓDIGO MUNICÍPIO SIAFI", "codigo_municipio")


#=========================================================
# TABELA COM MUITAS COLUNAS COLUNAS ATRAVÉS DE DICÍONARIO
#=========================================================


df_tratado = df

colunas_padrao = {
    "MÊS COMPETÊNCIA": "data_competencia",
    "MÊS REFERÊNCIA": "data_referencia",
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


df_tratado.show(5)

#=========================================================
# TRATAMENTO AUTOMÁTICO
#=========================================================

# def padronizar_nome(col):
#     col = col.lower()
#     col = unicodedata.normalize("NFD", col)
#     col = col.encode("ascii", "ignore").decode("utf-8")
#     col = re.sub(r"[^a-z0-9]+", "_", col)
#     col = col.strip("_")
#     return col

# df_tratado = df_tratado.toDF(
#     *[padronizar_nome(c) for c in df_tratado.columns]
# )

# #df_tratado = df.dropna(subset=["VALOR PARCELA"])
# df_tratado = df.dropna()
# df_tratado = df_tratado.withColumnRenamed(
#     "VALOR PARCELA", "valor_parcela"
# )

#=========================================================
# TRATANDO OS DADOS UNIFICADO
#=========================================================


df_tratado = (
    df_tratado
    .dropna()
    .withColumn(
        "valor_parcela", 
        regexp_replace(col("valor_parcela"), ",", ".").cast("decimal(10,2)")
    )
    .withColumn(
        "ano_competencia", 
        substring(col("data_competencia"), 1, 4)
    )
    .withColumn(
        "mes_competencia", 
        substring(col("data_competencia"), 5, 2)
    )
)
df_tratado.show(2)

#=========================================================
# ANÁLISE DE DADOS FATURAMENTO E MÉDIA
#=========================================================


df_tratado.agg(
    round(sum("valor_parcela"), 2).alias("total_pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento")
).show()

#=========================================================
# ANÁLISE DOS DADOS DE PAGAMENTO POR ESTADO
#=========================================================


df_tratado.groupBy("uf")\
    .agg(sum("valor_parcela").alias("total_pago"))\
    .orderBy("total_pago" , ascending=False)\
    .show(5)

# 1. Agrupando por CPF e Nome para evitar nomes duplicados de pessoas diferentes
# 2. Somando o valor total e contando quantas parcelas foram recebidas

ranking_favorecido = df_tratado.groupBy("cpf_favorecido" , "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

# Exibindo os 10 maiores favorecidos na análise
print("Rankink dos 10 primeiros com valores acumulados")
ranking_favorecido.show(5, truncate=False)

# Comparação: Média do ranking vs Média Geral
media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print(f"Para fins de comparação, media geral de cada parcela e R$ {media_geral:.2f}")


df_tratado.show(5)
df_tratado.printSchema()

df_uf = df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .limit(10) \
    .toPandas()

# Gráfico
plt.figure()
plt.bar(df_uf["uf"], df_uf["total_pago"])
plt.title("Top 10 UFs - Total Pago Bolsa Família")
plt.xlabel("UF")
plt.ylabel("Total Pago")
plt.show()

