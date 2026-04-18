from leitura_dados import ler_dados
from tratamento_dados import tratar_dados
from pyspark.sql.functions import col, sum, avg, count, desc, round

caminho_csv = "dados/pagamentos.csv"

df = ler_dados(caminho_csv)
df_tratado = tratar_dados(df)

df_tratado.show(5)
df_tratado.printSchema()

print("Resumo Geral de Pagamentos:")
df_tratado.agg(
    round(sum(col("valor_parcela")), 2).alias("total_pago"),
    round(avg(col("valor_parcela")), 2).alias("media_pagamento")
).show()

ranking_favorecido = df_tratado.groupBy("cpf_favorecido", "nome_favorecido") \
    .agg(
        round(sum(col("valor_parcela")), 2).alias("valor_total_acumulado"),
        count(col("valor_parcela")).alias("quantidade_parcelas")
    ) \
    .orderBy(desc("valor_total_acumulado"))

print("Ranking dos 5 primeiros com valores acumulados:")
ranking_favorecido.show(5, truncate=False)


def analise_parcelas_por_pessoa(df, cpf, nome):
    
    df_pessoa = df.filter((col("cpf_favorecido") == cpf) & (col("nome_favorecido") == nome)) \
                  .orderBy("ano_competencia", "mes_competencia")
    
    
    resumo = df_pessoa.agg(
        count("*").alias("quantidade_parcelas"),
        round(sum("valor_parcela"), 2).alias("total_recebido"),
        round(avg("valor_parcela"), 2).alias("valor_medio_parcela")
    )
    
    return df_pessoa, resumo

cpf_exemplo = "***.600.238-**"
nome_exemplo = "CRISTIANE FERNANDES DA SILVA"

df_pessoa, resumo_pessoa = analise_parcelas_por_pessoa(df_tratado, cpf_exemplo, nome_exemplo)

print(f"Detalhamento para: {nome_exemplo}")
df_pessoa.show()

print("Resumo estatístico da pessoa:")
resumo_pessoa.show()