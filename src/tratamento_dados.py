from pyspark.sql.functions import col
from pyspark.sql.functions import col, regexp_replace, substring

def tratar_dados(df):
    df = ( df
        # remove valores nulos
        .dropna(subset=["VALOR PARCELA"])
        
        #RENOMEIA A COLUNA
        .withColumnRenamed(
            "VALOR PARCELA" , "valor"
        )
        
        #RENOMEIA MUNICIPIOS
        .withColumnRenamed(
            "NOME MUNICÍPIO" , "municipio"
        )

        # troca vírgula por ponto
        .withColumn(
            "valor",
            regexp_replace(col("valor"), ",", ".")
        )
        
        # converte para DECIMAL
        .withColumn(
            "valor",
            col("valor").cast("decimal(10,2)")
        )

        #filtra valores inválidos
        .filter(col("valor") > 0)
        
        #cria uma coluna ano de competencia
        .withColumn(
            "ano_competência",
            substring(col("MÊS COMPETÊNCIA"), 1, 4)
        )
        .withColumn(
            "mes_competencia_num",
            substring(col("MÊS COMPETÊNCIA"), 5, 2)
    )
    )

    return df
