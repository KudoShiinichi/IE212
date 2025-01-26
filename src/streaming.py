from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from src.utils.data_processing import create_pipeline
from src.utils.model_utils import load_model
from config.kafka_config import KAFKA_BROKER, TOPIC_FEEDBACK
from config.settings import MODEL_FILE



# Tạo SparkSession
spark = SparkSession.builder.appName("ShopeeFeedbackStreaming").getOrCreate()

# Tải mô hình đã huấn luyện
model = load_model(MODEL_FILE)

# Hàm phân loại cảm xúc
def predict_sentiment(comment):
    return int(model.predict([comment])[0])

predict_udf = udf(predict_sentiment, IntegerType())

# Đọc dữ liệu từ Kafka
feedback_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_FEEDBACK) \
    .load()

feedback_stream = feedback_stream.selectExpr("CAST(value AS STRING) as comment")

# Tiền xử lý dữ liệu
pipeline = create_pipeline()
processed_stream = pipeline.fit(feedback_stream).transform(feedback_stream)

# Dự đoán cảm xúc
predictions = processed_stream.withColumn("sentiment", predict_udf("comment"))

# Hiển thị kết quả
query = predictions.select("comment", "sentiment").writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
