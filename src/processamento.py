
#=========================================================
# IMPORTANDO O SPARKSESSION
#=========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, avg, sum, desc, count
import unicodedata
import re

#=========================================================
# CONSTRUINDO O AMBIENTE DE BIGDATA
#=========================================================

spark = SparkSession.builder\
.appName("Bolsa familia")\
.config("spark.driver.memory", "8g")\
.config("spark.executor.memory", "8g")\
.config("spark.sql.shuffle.partitions", "50")\
.getOrCreate()

#criando o DataFrame
df = spark.read \
    .option("header", "true") \
    .option("sep", ";")\
    .option("encoding", "ISO-8859-1") \
    .option("inferSchema", "true") \
    .csv("dados/pagamentos.csv")

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


colunas_padrao = {
    "MÊS COMPETÊNCIA": "mes_competencia",
    "MÊS REFERÊNCIA": "mes_referencia",
    "UF": "uf",
    "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio",
    "NOME MUNICÍPIO": "nome_municipio",
    "CPF FAVORECIDO": "cpf_favorecido",
    "NIS FAVORECIDO": "nis_favorecido",
    "NOME FAVORECIDO": "nome_beneficiario",
    "VALOR PARCELA": "valor_parcela"
}
df_tratado = df

for antiga, nova in colunas_padrao.items():
    df_tratado = df_tratado.withColumnRenamed(antiga , nova)

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
    # remove valores nulos
    .dropna()
    
    # #RENOMEIA A COLUNA
    # .withColumnRenamed(
    #     "VALOR PARCELA" , "VALOR"
    # )
    # troca vírgula por ponto
    .withColumn(
        "valor_parcela",
        regexp_replace(col("valor_parcela"), ",", ".")
    )
    
    # converte para DECIMAL
    .withColumn(
        "valor_parcela",
        col("valor_parcela").cast("decimal(10,2)")
    )
)

df_tratado.show(2)

#=========================================================
# ANÁLISE DE DADOS FATURAMENTO E MÉDIA
#=========================================================


df_tratado.agg(
    sum("valor_parcela").alias("total_pago"),
    avg("valor_parcela").alias("media_pagamento")
).show(5)

#=========================================================
# ANÁLISE DOS DADOS DE PAGAMENTO POR ESTADO
#=========================================================


df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .show(5)


# 1. Agrupando por CPF e Nome para evitar nomes duplicados de pessoas diferentes
# 2. Somando o valor total e contando quantas parcelas foram recebidas
ranking_favorecidos = df_tratado.groupBy("cpf_favorecido", "nome_beneficiario") \
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    ) \
    .orderBy(desc("valor_total_acumulado"))

# Exibindo os 10 maiores favorecidos na análise
print("Ranking dos 10 Beneficiários com maiores valores acumulados:")
ranking_favorecidos.show(10, truncate=False)

# Comparação: Média do ranking vs Média Geral
media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print(f"Para fins de comparação, a média geral de cada parcela é: R$ {media_geral:.2f}")



df_tratado.show(5)
df_tratado.printSchema()

import matplotlib.pyplot as plt
from pyspark.sql.functions import sum
from leitura_dados import ler_dados
from tratamento_dados import tratar_dados

CAMINHO_CSV = "dados/pagamentos.csv"

# 1. Leitura
df = ler_dados(CAMINHO_CSV)

# 2. Tratamento
df_tratado = tratar_dados(df)

# 3. Agregação (Spark)
df_uf = (
    df_tratado\
        .groupBy("UF")\
        .agg(sum("valor").alias("total_pago"))\
        .orderBy("total_pago", ascending=False)\
        .limit(10)
)

df_uf.show()

df_uf = df_uf.toPandas()


# 4. Gráfico (Matplotlib)
plt.figure(figsize=(10, 6))
plt.bar(df_uf["UF"], df_uf["total_pago"])
plt.title("Top 10 UFs - Total Pago Bolsa Família")
plt.xlabel("UF")
plt.ylabel("Total Pago (R$)")
plt.show()