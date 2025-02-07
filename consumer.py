from kafka import KafkaConsumer
import json
import pandas as pd

import findspark
findspark.init()
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

from pyspark.sql.functions import col

scala_version = '2.12'
spark_version = '3.5.3'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.9.0'
]

spark = SparkSession.builder.master('local').appName('streaming_data').config('spark.jars.packages', ",".join(packages)).getOrCreate()
# Tạo KafkaConsumer để nhận dữ liệu từ một Kafka topic
def create_consumer(topic):
    """
    Tạo KafkaConsumer để nhận dữ liệu từ một Kafka topic.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],  # Kafka broker
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Chuyển đổi dữ liệu JSON
        auto_offset_reset='latest'  # Đảm bảo chỉ lấy dữ liệu mới

    )
    return consumer

# Hàm tiêu thụ dữ liệu từ Kafka và xử lý
def consume_data(consumer):
    """
    Tiêu thụ dữ liệu từ Kafka và xử lý.
    """
    print("đang chờ, chưa có dữ liệu")
    
    for message in consumer:
        print(f"Received message: {message.value}")
        
        # Thêm vào danh sách các dữ liệu đã nhận
        message_data = message.value
        
        # Chuyển dictionary thành Row để tạo Spark DataFrame
        row = Row(**message_data)
        
        # Tạo DataFrame từ Row
        df = spark.createDataFrame([row])

        # Hiển thị DataFrame
        df.show()
if __name__ == "__main__":
    topic = "resultData"  # Thay đổi theo tên topic mà bạn muốn tiêu th
    consumer = create_consumer(topic)
    print("Đang chờ dữ liệu từ Kafka...")
    consume_data(consumer)
