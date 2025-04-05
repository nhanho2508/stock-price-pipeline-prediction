from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import psycopg2

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaDataViewer") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "venkat_stream") \
    .load()

df1 = df.selectExpr("CAST(value AS STRING)")

df2 = df1.withColumn("Body", regexp_replace("value", "\"", ""))

df3 = df2.withColumn("actual", split(df2.Body, ",")) \
    .withColumn("Timestamp", to_timestamp(col("actual").getItem(0), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Bank_Name", col("actual").getItem(1)) \
    .withColumn("Open", col("actual").getItem(2).cast("decimal(38, 0)")) \
    .withColumn("High", col("actual").getItem(3).cast("decimal(38, 0)")) \
    .withColumn("Low", col("actual").getItem(4).cast("decimal(38, 0)")) \
    .withColumn("Close", col("actual").getItem(5).cast("decimal(38, 0)")) \
    .withColumn("Volume", col("actual").getItem(6).cast("decimal(38, 0)"))

df4 = df3.select("Timestamp", "Bank_Name", "Open", "High", "Low", "Close", "Volume")

dfWindowed = df4.groupBy(window(df4.Timestamp, "5 seconds", "2 seconds"), df4.Bank_Name).mean().orderBy('window') \
    .select(col("window.start").alias("StartTime"), col("window.end").alias("EndTime"), "Bank_Name", 
            col("avg(Open)").alias("Avg_OpenPrice"),
            col("avg(High)").alias("Avg_HighPrice"),
            col("avg(Low)").alias("Avg_LowPrice"),
            col("avg(Close)").alias("Avg_ClosePrice"),
            col("avg(Volume)").alias("Avg_Volume"))

# Hàm insert dữ liệu vào PostgreSQL
def write_to_postgres(df, epoch_id):
    try:
        # Kết nối PostgreSQL
        connection = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",  # Nếu chạy Docker, có thể cần dùng 'Timescale_DB'
            port="5433",  # Cổng theo docker-compose
            database="demo_streaming"
        )
        cursor = connection.cursor()
        
        for row in df.collect():  # Lặp qua từng dòng
            sql_insert_query = """INSERT INTO venkat_demo (StartTime, EndTime, Bank_Name, Avg_OpenPrice, Avg_HighPrice, Avg_LowPrice, Avg_ClosePrice, Avg_Volume)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql_insert_query, (row.StartTime, row.EndTime, row.Bank_Name, row.Avg_OpenPrice, row.Avg_HighPrice, row.Avg_LowPrice, row.Avg_ClosePrice, row.Avg_Volume))
        
        connection.commit()
        print(f"Batch {epoch_id} inserted successfully!")

    except Exception as e:
        print(f"Error inserting batch {epoch_id}: {e}")

    finally:
        cursor.close()
        connection.close()

# Ghi dữ liệu vào PostgreSQL mỗi batch
dfWindowed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("complete") \
    .start().awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 spark_submit_1.py
