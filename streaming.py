from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, IntegerType
from config.kafka_config import KAFKA_BROKER, TOPIC_RAW_DATA, TOPIC_RESULT_DATA
import json
from data_processing import preprocessing
import findspark
findspark.init()
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType


import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.functions import col

scala_version = '2.12'
spark_version = '3.5.3'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.9.0'
]

spark = SparkSession.builder.master('local').appName('streaming_data').config('spark.jars.packages', ",".join(packages)).getOrCreate()

# Hàm tiền xử lý comment
# def preprocessing(comment):
#     # Áp dụng tiền xử lý dữ liệu như loại bỏ dấu câu, stopwords, v.v.
#     # Bạn có thể thay đổi nội dung này để phù hợp với nhu cầu của bạn
#     return comment.lower()  # Ví dụ chuyển thành chữ thường

# Hàm phân loại cảm xúc
def sentiment_predict(comment):
    # Viết lại nội dung hàm này dựa trên mô hình bạn đang sử dụng
    # Ví dụ, dự đoán cảm xúc từ comment
    # Trả về 1 cho "positive", 0 cho "negative"
    return 1 if "tốt" in comment else 0  # Đây là ví dụ đơn giản

# Đăng ký UDF (User Defined Function) cho hàm phân loại cảm xúc
preprocess_udf = udf(preprocessing, StringType())
sentiment_udf = udf(sentiment_predict, IntegerType())

# Đọc dữ liệu stream từ Kafka topic rawData
feedback_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_RAW_DATA) \
    .option("failOnDataLoss", "false") \
    .load()

print("Đọc dữ liệu thành công!")

schema_value = StructType(
    [StructField("rating",StringType(),True),
    StructField("comment",StringType(),True),
    StructField("orderID",StringType(),True),
    StructField("time",StringType(),True),])
# -------------------------------***-----------------------------
df_json = (feedback_stream
           .selectExpr("CAST(value AS STRING)")
           .withColumn("value",f.from_json("value",schema_value)))

df_json.printSchema()

# Tiền xử lý dữ liệu
processed_stream = df_json.withColumn("processed_comment", preprocess_udf(col("value.comment")))

# Dự đoán cảm xúc
predictions = processed_stream.withColumn("sentiment", sentiment_udf(col("processed_comment")))

print("Đã xử lí xong comment")

# Chuyển đổi dữ liệu thành định dạng JSON để gửi lại vào Kafka
result_data = predictions.select(
    col("processed_comment"), col("sentiment")
).withColumn(
    "value", 
    f.to_json(f.struct("processed_comment", "sentiment"))
)

# Gửi kết quả vào Kafka topic result_data
query = result_data.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_RESULT_DATA) \
    .option("checkpointLocation", "checkpoints/processed_feedback") \
    .start()

print("Đã gửi cho kafka")
# Chờ cho đến khi có dữ liệu và tiếp tục xử lý
query.awaitTermination()
