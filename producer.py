import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Kafka Producer configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_RAW_DATA = "rawData"

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Hàm lấy dữ liệu bình luận từ API Shopee
def fetch_shopee_comments(item_id, shop_id, limit=10, offset=0):
    url = "https://shopee.vn/api/v4/item/get_ratings"
    params = {
        "itemid": item_id,
        "shopid": shop_id,
        "limit": limit,
        "offset": offset,  # Dùng offset để lấy tiếp bình luận mới
        "filter": 0,  # 0: tất cả, 1: có hình ảnh, 2: 5 sao, 3: 4 sao, ...
        "type": 0     # 0: bình thường
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        comments = data.get("data", {}).get("ratings", [])
        print(f"Đã lấy {len(comments)} bình luận.")
        return comments
    else:
        print(f"Error: {response.status_code}")
        return []

# Hàm gửi dữ liệu vào Kafka
def send_to_kafka(data):
    producer.send(TOPIC_RAW_DATA, data)
    producer.flush()
    print("Dữ liệu đã được gửi đến Kafka")

# Hàm lấy và xử lý dữ liệu bình luận từ Shopee
def get_and_send_comments(item_id, shop_id, offset=0):
    limit = 10
    new_data_received = False

    comments = fetch_shopee_comments(item_id, shop_id, limit, offset)

    if comments:
        new_data_received = True
        for comment in comments:
            data = {
                "rating": comment.get("rating_star", "N/A"),
                "comment": comment.get("comment", "N/A"),
                "orderid": comment.get("orderid", "N/A"),
                "time": datetime.fromtimestamp(comment.get("ctime", 0)).strftime('%Y-%m-%d %H:%M:%S')  # Chuyển timestamp thành thời gian
            }

            # Gửi bình luận vào Kafka
            send_to_kafka(data)

        # Lấy offset của bình luận cuối cùng để tiếp tục lấy dữ liệu mới trong lần sau
        offset += 10# Sử dụng orderid hoặc giá trị khác làm offset mới
        print("Đã có comment mới.")
    else:
        print("Không có comment mới.")

    return new_data_received, offset

# Main function: gửi request mỗi 5 phút
def main():
    item_id = 22088583698  # ID sản phẩm (thay bằng ID sản phẩm của bạn)
    shop_id = 196261835    # ID shop (thay bằng ID shop của bạn)

    offset = 0  # Biến lưu trữ offset của bình luận cuối cùng đã lấy

    for x in range(0, 1000):
        try:
            print("Gửi request lấy dữ liệu bình luận...")
            new_data_received, offset = get_and_send_comments(item_id, shop_id, offset)
            if not new_data_received:
                print("Không có comment mới, sẽ thử lại sau...")
            # Chờ 5 phút trước khi gửi request tiếp theo
            time.sleep(5)  # 5 phút = 300 giây
        except KeyboardInterrupt:
            print("Ngắt kết nối. Dừng chương trình.")
            break
    print("Kết thúc quá trình crawl dữ liệu.")

if __name__ == "__main__":
    main()
