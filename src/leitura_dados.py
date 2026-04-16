from pyspark.sql import SparkSession

def ler_dados(caminho_csv):
    spark = SparkSession.builder \
        .appName("Bolsa Familia - Leitura") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "ISO-8859-1") \
        .option("inferSchema", "true") \
        .csv(caminho_csv)

    return df