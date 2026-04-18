from pyspark.sql.functions import col, regexp_replace, substring

def tratar_dados(df):

    df_tratado = df.dropna()

    for coluna in df_tratado.columns:
       
        novo_nome = coluna.lower().replace(" ", "_").replace("ê", "e").replace("â", "a")
        df_tratado = df_tratado.withColumnRenamed(coluna, novo_nome)
    
    df_tratado = df_tratado.withColumn(
        "valor_parcela", 
        regexp_replace(col("valor_parcela"), ",", ".").cast("decimal(10,2)")
    ).filter(col("valor_parcela") > 0)

    df_tratado = df_tratado.withColumn(
        "ano_competencia", 
        substring(col("mes_competencia").cast("string"), 1, 4)
    ).withColumn(
        "mes_competencia_num", # Mudando o nome para não dar conflito com a coluna original
        substring(col("mes_competencia").cast("string"), 5, 2)
    )

    return df_tratado