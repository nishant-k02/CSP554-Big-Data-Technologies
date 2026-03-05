# CSP554 Assignment #7 - Ready-to-run PySpark script
# Usage inside pyspark REPL:
#   exec(open("/home/hadoop/assignment7_solution.py").read())
# Or via spark-submit:
#   spark-submit /home/hadoop/assignment7_solution.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, sum as _sum
import os
import hashlib

# --------------- Spark Session (works in pyspark or spark-submit) ---------------
try:
    spark  # type: ignore
except NameError:
    spark = SparkSession.builder.appName("CSP554-Assignment7").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------- Config ---------------
INPUT_DIR = os.getenv("INPUT_DIR", "hdfs:///user/hadoop")
FOODRATINGS_PATH = os.getenv("FOODRATINGS_PATH", f"{INPUT_DIR}/foodratings115952.txt")
FOODPLACES_PATH = os.getenv("FOODPLACES_PATH", f"{INPUT_DIR}/foodplaces115952.txt")

print("\n=== Using paths ===")
print("FOODRATINGS_PATH:", FOODRATINGS_PATH)
print("FOODPLACES_PATH :", FOODPLACES_PATH)

# --------------- Exercise 1: Load foodratings as CSV with schema ---------------
schema_ratings = StructType([
    StructField("name", StringType(), True),
    StructField("food1", IntegerType(), True),
    StructField("food2", IntegerType(), True),
    StructField("food3", IntegerType(), True),
    StructField("food4", IntegerType(), True),
    StructField("placeid", IntegerType(), True)
])

foodratings = (
    spark.read
         .option("header", "false")
         .option("mode", "PERMISSIVE")
         .option("inferSchema", "false")
         .schema(schema_ratings)
         .csv(FOODRATINGS_PATH)
)

print("\n--- Exercise 1: foodratings.printSchema() ---")
foodratings.printSchema()

print("\n--- Exercise 1: foodratings.show(5) ---")
foodratings.show(5, truncate=False)

# "Magic number" helpers (row count and sum of numeric columns).
row_count = foodratings.count()
sums_row = (foodratings
            .select(_sum(col("food1")).alias("sum_food1"),
                    _sum(col("food2")).alias("sum_food2"),
                    _sum(col("food3")).alias("sum_food3"),
                    _sum(col("food4")).alias("sum_food4"))
            .collect()[0])

checksum_src = f"{row_count}-{sums_row['sum_food1']}-{sums_row['sum_food2']}-{sums_row['sum_food3']}-{sums_row['sum_food4']}"
magic_checksum = hashlib.md5(checksum_src.encode("utf-8")).hexdigest()

print("\n--- Exercise 1: MAGIC NUMBERS ---")
print(f"Row Count Magic Number: {row_count}")
print(f"Column Sums: {dict(sums_row.asDict())}")
print(f"MD5 Checksum: {magic_checksum} (from '{checksum_src}')")

# --------------- Exercise 2: Load foodplaces as CSV with schema ---------------
schema_places = StructType([
    StructField("placeid", IntegerType(), True),
    StructField("placename", StringType(), True)
])

foodplaces = (
    spark.read
         .option("header", "false")
         .option("mode", "PERMISSIVE")
         .option("inferSchema", "false")
         .schema(schema_places)
         .csv(FOODPLACES_PATH)
)

print("\n--- Exercise 2: foodplaces.printSchema() ---")
foodplaces.printSchema()

print("\n--- Exercise 2: foodplaces.show(5) ---")
foodplaces.show(5, truncate=False)

# --------------- Exercise 3: Register as tables + SQL queries ---------------
foodratings.createOrReplaceTempView("foodratingsT")
foodplaces.createOrReplaceTempView("foodplacesT")

# Step B: food2 < 25 and food4 > 40
foodratings_ex3a = spark.sql("""
    SELECT *
    FROM foodratingsT
    WHERE (food2 < 25) AND (food4 > 40)
""")

print("\n--- Exercise 3B: foodratings_ex3a.printSchema() ---")
foodratings_ex3a.printSchema()

print("\n--- Exercise 3B: foodratings_ex3a.show(5) ---")
foodratings_ex3a.show(5, truncate=False)

# Step C: placeid > 3
foodplaces_ex3b = spark.sql("""
    SELECT *
    FROM foodplacesT
    WHERE (placeid > 3)
""")

print("\n--- Exercise 3C: foodplaces_ex3b.printSchema() ---")
foodplaces_ex3b.printSchema()

print("\n--- Exercise 3C: foodplaces_ex3b.show(5) ---")
foodplaces_ex3b.show(5, truncate=False)

# --------------- Exercise 4: Transformation filter (not SQL) ---------------
foodratings_ex4 = foodratings.filter(
    (col("name") == "Mel") & (col("food3") < 25)
)

print("\n--- Exercise 4: foodratings_ex4.printSchema() ---")
foodratings_ex4.printSchema()

print("\n--- Exercise 4: foodratings_ex4.show(5) ---")
foodratings_ex4.show(5, truncate=False)

# --------------- Exercise 5: Select only name and placeid (not SQL) ---------------
foodratings_ex5 = foodratings.select("name", "placeid")

print("\n--- Exercise 5: foodratings_ex5.printSchema() ---")
foodratings_ex5.printSchema()

print("\n--- Exercise 5: foodratings_ex5.show(5) ---")
foodratings_ex5.show(5, truncate=False)

# --------------- Exercise 6: Inner join on placeid (not SQL) ---------------
ex6 = foodratings.join(foodplaces, on="placeid", how="inner")

print("\n--- Exercise 6: ex6.printSchema() ---")
ex6.printSchema()

print("\n--- Exercise 6: ex6.show(5) ---")
ex6.show(5, truncate=False)


