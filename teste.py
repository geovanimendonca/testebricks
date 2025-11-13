import argparse
from pyspark.sql import SparkSession

def main():
    # Criar parser de argumentos
    parser = argparse.ArgumentParser(description="Exemplo com parâmetros no Databricks Job")

    parser.add_argument("--nome", type=str, required=True, help="Nome da pessoa")
    parser.add_argument("--idade", type=int, required=True, help="Idade da pessoa")
    parser.add_argument("--limite", type=int, default=5, help="Limite de números")

    args = parser.parse_args()


    print("=== Parâmetros recebidos ===")
    print(f"Nome: {args.nome}")
    print(f"Idade: {args.idade}")
    print(f"Limite: {args.limite}")
    # Exemplo usando os parâmetros em um Spark DataFrame
    df = spark.range(0, args.limite).withColumnRenamed("id", "numero")

    print("=== DataFrame ===")
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()