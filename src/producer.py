from kafka import KafkaProducer
import json
import random
import sys
import os

# Thêm thư mục gốc của dự án vào sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.kafka_config import KAFKA_BROKER, TOPIC_FEEDBACK
from src.utils.logging import get_logger

logger = get_logger("KafkaProducer")

def generate_feedback():
    """Sinh dữ liệu feedback mẫu."""
    comments = [
        {"id": 1, "comment": "Sản phẩm rất tốt, tôi rất hài lòng"},
        {"id": 2, "comment": "Chất lượng kém, không đáng tiền"},
        {"id": 3, "comment": "Bình thường, không quá đặc biệt"},
    ]
    return random.choice(comments)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logger.info("Producer bắt đầu gửi feedback...")
for _ in range(10):
    feedback = generate_feedback()
    producer.send(TOPIC_FEEDBACK, feedback)
    logger.info(f"Gửi feedback: {feedback}")
producer.flush()
