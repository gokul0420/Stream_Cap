from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder \
    .appName("CustomerTransactionStreaming") \
    .getOrCreate()

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("income", DoubleType(), True)
])


transaction_schema = StructType([
    StructField("txn_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True)
])

customers_static_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(customer_schema) \
    .load("/user/cloudera/capstone/customer/customer.csv")


transactions_stream_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(transaction_schema) \
    .load("/user/cloudera/capstone/transactions/")


enriched_df = transactions_stream_df.join(customers_static_df,"customer_id","inner"
)

total_spend = enriched_df.groupBy("customer_id", "name") \
    .agg(sum("amount").alias("Total_Spent"))

high_value_tx = enriched_df.filter(col("amount") > 3000) \
    .select("txn_id", "name", "amount")

live_count = enriched_df.agg(count("txn_id").alias("Total_Transactions_Processed"))

city_spending = enriched_df.groupBy("city") \
    .agg(sum("amount").alias("Total_Spending"))


def start_query(df, mode, query_name):
    return df.writeStream \
        .outputMode(mode) \
        .format("console") \
        .queryName(query_name) \
        .start()

q1 = start_query(total_spend, "complete", "TotalSpending")
q2 = start_query(high_value_tx, "append", "HighValue")
q3 = start_query(live_count, "complete", "LiveCount")
q4 = start_query(city_spending, "complete", "CityAnalysis")

q1.awaitTermination()
