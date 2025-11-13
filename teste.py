from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# No Databricks já existe 'spark', mas esse código também funciona fora
# try:
#     spark
# except NameError:
# spark = SparkSession.builder.appName("ExemploJoinSpark").getOrCreate()

def main():
    # Criar DataFrame 1: Clientes
    df_clientes = spark.createDataFrame(
        [
            (1, "João"),
            (2, "Maria"),
            (3, "Carlos"),
        ],
        ["cliente_id", "nome"]
    )

    # Criar DataFrame 2: Pedidos
    df_pedidos = spark.createDataFrame(
        [
            (1, "Notebook"),
            (2, "Cadeira"),
            (2, "Mesa"),
            (4, "TV"),   # cliente que não existe no df_clientes
        ],
        ["cliente_id", "produto"]
    )

    print("=== DataFrame Clientes ===")
    df_clientes.show()

    print("=== DataFrame Pedidos ===")
    df_pedidos.show()

    # Fazer o join
    df_join = df_clientes.join(
        df_pedidos,
        on="cliente_id",
        how="inner"       # pode ser: left, right, full, outer, left_anti, etc.
    )

    print("=== Resultado do JOIN ===")
    df_join.show()

if __name__ == "__main__":
    main()
