Celem ćwiczenia było uruchomienie lokalnego środowiska Apache Spark oraz wykonanie podstawowych operacji na danych z użyciem PySpark. W projekcie wykorzystano dwa podejścia:

- DataFrame - wygodny sposób pracy z danymi tabelarycznymi,
- RDD - niższy poziom abstrakcji, pozwalający ręcznie wykonywać transformacje i akcje.

## Dataset

W projekcie wykorzystano istniejący publiczny dataset CSV udostępniony przez Microsoft Learning:
https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv

Dataset zawiera dane sprzedażowe, m.in. numer zamówienia, datę, klienta, produkt, ilość, cenę jednostkową oraz kwotę podatku. Plik zawiera 32718 wierszy danych.

## Wymagania

Do uruchomienia projektu wymagane są:

- Python 3.11,
- Java JDK 17,
- Apache Spark 4.1.1,
- pakiety: `pyspark`, `pandas`, `pyarrow`, `psutil`.

## Konfiguracja środowiska

Przed uruchomieniem skryptów w terminalu CMD należy ustawić zmienne środowiskowe:

```cmd
set SPARK_HOME=C:\spark-4.1.1-bin-hadoop3
set PATH=%SPARK_HOME%\bin;%PATH%
set PYSPARK_PYTHON=%CD%\.venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%CD%\.venv\Scripts\python.exe
```

W projekcie użyto Pythona 3.11, ponieważ na lokalnej konfiguracji Windows Python 3.12 powodował błąd procesu PySpark worker.

## DataFrame w PySpark

Kod znajduje się w pliku src/dataframe_operations.py

W skrypcie wykonano:

- wczytanie pliku `data/sales.csv` do DataFrame,
- wyświetlenie danych przez `show()`,
- wyświetlenie schematu przez `printSchema()`,
- selekcję wybranych kolumn,
- dodanie kolumny `LineTotal`,
- filtrowanie pozycji zamówień o wartości powyżej 1000,
- grupowanie i agregacje według produktu, klienta oraz daty zamówienia,
- zapis wyników do CSV i Parquet.

Wygenerowane pliki wynikowe:

```text
output/product_summary.csv
output/customer_summary.parquet
output/date_summary.csv
```

## RDD w PySpark

Kod znajduje się w pliku src/rdd_operations.py

W skrypcie wykonano:

- wczytanie CSV jako RDD przez `sc.textFile()`,
- ręczne parsowanie wierszy z użyciem modułu `csv`,
- zliczenie liczby wierszy,
- pobranie pierwszych rekordów,
- obliczenie sumy sprzedanych sztuk,
- obliczenie całkowitej wartości sprzedaży z podatkiem,
- filtrowanie pozycji o wartości powyżej 1000,
- agregację sprzedaży według produktu,
- zliczenie pozycji zamówień według klienta.

```text
Liczba wierszy: 32718
Suma sprzedanych sztuk: 32718
Całkowita wartość sprzedaży z podatkiem: 22602264.28
```

Do parsowania użyto modułu csv, ponieważ w danych występują nazwy produktów zawierające przecinki, np. Mountain-100 Silver, 44.

## Podsumowanie

W projekcie wykonano wszystkie wymagane elementy laboratorium: skonfigurowano lokalne środowisko Apache Spark, sprawdzono spark-shell i pyspark, pobrano istniejący dataset CSV, wykonano operacje DataFrame oraz RDD, a wyniki zapisano do CSV i Parquet.