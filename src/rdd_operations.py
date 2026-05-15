from pyspark.sql import SparkSession
import csv
from io import StringIO


def parse_csv_line(line):
    reader = csv.reader(StringIO(line))
    return next(reader)


def main():
    spark = SparkSession.builder \
        .appName("MicrosoftSalesRDDExample") \
        .master("local[1]") \
        .getOrCreate()

    sc = spark.sparkContext

    input_path = "data/sales.csv"

    raw_rdd = sc.textFile(input_path)

    header = raw_rdd.first()
    columns = parse_csv_line(header)

    print("\n=== Kolumny w pliku CSV ===")
    print(columns)

    sales_rdd = raw_rdd \
        .filter(lambda line: line != header) \
        .map(parse_csv_line)

    print("\n=== Liczba wierszy w RDD ===")
    rows_count = sales_rdd.count()
    print(rows_count)

    print("\n=== Pierwsze 5 rekordów ===")
    for row in sales_rdd.take(5):
        print(row)

    print("\n=== Suma sprzedanych sztuk ===")
    total_quantity = sales_rdd \
        .map(lambda row: int(row[6])) \
        .reduce(lambda a, b: a + b)
    print(total_quantity)

    print("\n=== Całkowita wartość sprzedaży z podatkiem ===")
    total_sales_value = sales_rdd \
        .map(lambda row: int(row[6]) * float(row[7]) + float(row[8])) \
        .reduce(lambda a, b: a + b)
    print(round(total_sales_value, 2))

    print("\n=== Pozycje zamówień o wartości powyżej 1000 ===")
    high_value_orders = sales_rdd \
        .filter(lambda row: int(row[6]) * float(row[7]) + float(row[8]) > 1000) \
        .take(10)

    for order in high_value_orders:
        print(order)

    print("\n=== Sprzedaż według produktu ===")
    sales_by_product = sales_rdd \
        .map(lambda row: (row[5], int(row[6]) * float(row[7]) + float(row[8]))) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda row: row[1], ascending=False) \
        .take(20)

    for item, value in sales_by_product:
        print(item, round(value, 2))

    print("\n=== Liczba pozycji zamówień według klienta ===")
    rows_by_customer = sales_rdd \
        .map(lambda row: (row[3], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda row: row[1], ascending=False) \
        .take(20)

    for customer, count in rows_by_customer:
        print(customer, count)

    spark.stop()


if __name__ == "__main__":
    main()