from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count

# ---------------------------------
# 1️⃣ Create Spark Session
# ---------------------------------
spark = SparkSession.builder \
    .appName("paysim_etl_pipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

print("\n🚀 Spark Session Started")

# ---------------------------------
# 2️⃣ Read CSV
# ---------------------------------
csv_path = "data/PS_20174392719_1491204439457_log.csv"

df = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=True
)

print("\n✅ Rows loaded (Raw):", df.count())

# ---------------------------------
# 3️⃣ Data Understanding
# ---------------------------------
print("\n📌 Schema:")
df.printSchema()

print("\n📊 Summary Statistics:")
df.describe().show()

print("\n🏷 Transaction Types:")
df.select("type").distinct().show()

print("\n👀 Sample Raw Data:")
df.show(5)

# ---------------------------------
# 4️⃣ Data Cleaning
# ---------------------------------
print("\n🧹 Starting Data Cleaning...")

df_clean = df.dropna()
df_clean = df_clean.dropDuplicates()
df_clean = df_clean.filter(col("amount") > 0)

# Standardize column names
for column in df_clean.columns:
    df_clean = df_clean.withColumnRenamed(column, column.lower())

print("✅ Rows after Cleaning:", df_clean.count())

print("\n📌 Cleaned Schema:")
df_clean.printSchema()

print("\n👀 Cleaned Sample Data:")
df_clean.show(5)

# ---------------------------------
# 5️⃣ Feature Engineering
# ---------------------------------
print("\n🧠 Starting Feature Engineering...")

df_features = df_clean.withColumn(
    "is_large_transaction",
    when(col("amount") > 200000, 1).otherwise(0)
)

df_features = df_features.withColumn(
    "is_balance_error",
    when(
        (col("oldbalanceorg") - col("amount") != col("newbalanceorig")),
        1
    ).otherwise(0)
)

print("\n📊 Fraud Distribution:")
df_features.groupBy("isfraud").count().show()

print("\n📊 Large Transaction Distribution:")
df_features.groupBy("is_large_transaction").count().show()

print("\n📊 Balance Error Distribution:")
df_features.groupBy("is_balance_error").count().show()

print("\n👀 Feature Engineered Sample:")
df_features.show(5)

# ---------------------------------
# 6️⃣ Aggregation Layer (Analytics)
# ---------------------------------

print("\n📊 Transaction Summary by Type")

txn_summary = df_features.groupBy("type").agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
)

txn_summary.show()


print("\n🚨 Fraud Summary")

fraud_summary = df_features.groupBy("isfraud").agg(
    count("*").alias("fraud_transactions")
)

fraud_summary.show()


print("\n📅 Daily Transaction Summary")

daily_summary = df_features.groupBy("step").agg(
    sum("amount").alias("daily_total_amount"),
    count("*").alias("daily_transaction_count")
)

daily_summary.show()

# ---------------------------------
# 7️⃣ Save Processed Data to Parquet
# ---------------------------------

print("\n💾 Writing Clean Layer to Parquet")

df_features.write \
    .mode("overwrite") \
    .parquet("output/clean_transactions")

print("✅ Clean transactions saved")


print("\n💾 Writing Aggregation Tables")

txn_summary.write.mode("overwrite").parquet("output/txn_summary")

fraud_summary.write.mode("overwrite").parquet("output/fraud_summary")

daily_summary.write.mode("overwrite").parquet("output/daily_summary")

print("✅ Aggregation tables saved")

# ---------------------------------
# 8️⃣ Load Data into PostgreSQL
# ---------------------------------

print("\n🐘 Writing data to PostgreSQL warehouse")

postgres_url = "jdbc:postgresql://localhost:5433/bankingdb"

postgres_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Transactions table
df_features.write.jdbc(
    url=postgres_url,
    table="transactions",
    mode="overwrite",
    properties=postgres_properties
)

print("✅ transactions table written")

# Transaction summary
txn_summary.write.jdbc(
    url=postgres_url,
    table="txn_summary",
    mode="overwrite",
    properties=postgres_properties
)

print("✅ txn_summary table written")

# Fraud summary
fraud_summary.write.jdbc(
    url=postgres_url,
    table="fraud_summary",
    mode="overwrite",
    properties=postgres_properties
)

print("✅ fraud_summary table written")

# Daily summary
daily_summary.write.jdbc(
    url=postgres_url,
    table="daily_summary",
    mode="overwrite",
    properties=postgres_properties
)

print("✅ daily_summary table written")

# ---------------------------------
# 9️⃣ Stop Spark
# ---------------------------------

spark.stop()

print("\n🏁 ETL Pipeline Finished Successfully")