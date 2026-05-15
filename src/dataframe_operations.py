from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, round


def save_with_pandas(spark_df, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pandas_df = spark_df.toPandas()

    if output_path.suffix == ".csv":
        pandas_df.to_csv(output_path, index=False)
    elif output_path.suffix == ".parquet":
        pandas_df.to_parquet(output_path, index=False)
    else:
        raise ValueError("Obsługiwane formaty zapisu: .csv oraz .parquet")


def main():
    spark = SparkSession.builder \
        .appName("MicrosoftSalesDataFrameExample") \
        .master("local[1]") \
        .getOrCreate()

    input_path = "data/sales.csv"

    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    print("\n=== Podgląd danych ===")
    df.show(10, truncate=False)

    print("\n=== Schemat danych ===")
    df.printSchema()

    print("\n=== Nazwy kolumn ===")
    print(df.columns)

    print("\n=== Selekcja wybranych kolumn ===")
    selected_df = df.select(
        "SalesOrderNumber",
        "OrderDate",
        "CustomerName",
        "Item",
        "Quantity",
        "UnitPrice",
        "TaxAmount"
    )
    selected_df.show(10, truncate=False)

    print("\n=== Dodanie kolumny LineTotal ===")
    df_with_total = df.withColumn(
        "LineTotal",
        col("Quantity") * col("UnitPrice") + col("TaxAmount")
    )

    df_with_total.select(
        "SalesOrderNumber",
        "CustomerName",
        "Item",
        "Quantity",
        "UnitPrice",
        "TaxAmount",
        "LineTotal"
    ).show(10, truncate=False)

    print("\n=== Filtrowanie: pozycje zamówień o wartości powyżej 1000 ===")
    high_value_orders = df_with_total.filter(col("LineTotal") > 1000)
    high_value_orders.select(
        "SalesOrderNumber",
        "CustomerName",
        "Item",
        "Quantity",
        "UnitPrice",
        "TaxAmount",
        "LineTotal"
    ).show(10, truncate=False)

    print("\n=== Grupowanie po produkcie ===")
    product_summary = df_with_total.groupBy("Item").agg(
        count("*").alias("rows_count"),
        spark_sum("Quantity").alias("total_quantity"),
        round(spark_sum("LineTotal"), 2).alias("total_sales_value"),
        round(avg("LineTotal"), 2).alias("avg_line_value")
    ).orderBy(col("total_sales_value").desc())

    product_summary.show(20, truncate=False)

    print("\n=== Grupowanie po kliencie ===")
    customer_summary = df_with_total.groupBy("CustomerName").agg(
        count("*").alias("orders_lines_count"),
        spark_sum("Quantity").alias("total_quantity"),
        round(spark_sum("LineTotal"), 2).alias("total_sales_value")
    ).orderBy(col("total_sales_value").desc())

    customer_summary.show(20, truncate=False)

    print("\n=== Grupowanie po dacie zamówienia ===")
    date_summary = df_with_total.groupBy("OrderDate").agg(
        count("*").alias("rows_count"),
        spark_sum("Quantity").alias("total_quantity"),
        round(spark_sum("LineTotal"), 2).alias("total_sales_value")
    ).orderBy("OrderDate")

    date_summary.show(20, truncate=False)

    print("\n=== Zapis wyników do CSV i Parquet ===")

    save_with_pandas(product_summary, "output/product_summary.csv")
    save_with_pandas(customer_summary, "output/customer_summary.parquet")
    save_with_pandas(date_summary, "output/date_summary.csv")

    print("\nZapisano wyniki:")
    print("output/product_summary.csv")
    print("output/customer_summary.parquet")
    print("output/date_summary.csv")

    spark.stop()


if __name__ == "__main__":
    main()