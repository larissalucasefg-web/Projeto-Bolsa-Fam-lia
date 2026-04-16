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