import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Cấu hình ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'invoices'
CONSUMER_GROUP_ID = 'invoice-processing-group'

def create_consumer():
    """Tạo một Kafka Consumer để đọc tin nhắn."""
    try:
        consumer = KafkaConsumer(
            # Tên topic để lắng nghe
            TOPIC_NAME,
            
            # Địa chỉ của Kafka broker
            bootstrap_servers=[KAFKA_BROKER],
            
            # Bắt đầu đọc từ tin nhắn cũ nhất nếu là consumer mới
            auto_offset_reset='earliest',
            
            # Định danh cho consumer group
            group_id=CONSUMER_GROUP_ID,
            
            # Tự động giải mã tin nhắn từ bytes về dictionary
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print("✅ Kết nối đến Kafka và lắng nghe topic thành công!")
        return consumer
    except NoBrokersAvailable:
        print(f"❌ Không thể kết nối đến broker tại {KAFKA_BROKER}. Vui lòng kiểm tra lại Kafka server.")
        return None

def listen_for_messages(consumer):
    """Lắng nghe và xử lý tin nhắn từ Kafka."""
    print("\n--- Đang lắng nghe tin nhắn từ topic '{}' ---".format(TOPIC_NAME))
    print("(Nhấn Ctrl+C để dừng)")
    try:
        # Vòng lặp này sẽ chạy vô hạn, chờ tin nhắn mới
        for message in consumer:
            print("\n-----")
            print(f"✅ Nhận được tin nhắn mới:")
            print(f"  - Topic: {message.topic}")
            print(f"  - Partition: {message.partition}")
            print(f"  - Offset: {message.offset}")
            print(f"  - Dữ liệu: {message.value}")
            print("-----")
            
    except KeyboardInterrupt:
        print("\n🛑 Dừng lắng nghe.")
    finally:
        # Đóng kết nối consumer
        print("... Đóng kết nối consumer.")
        consumer.close()

if __name__ == "__main__":
    kafka_consumer = create_consumer()
    
    if kafka_consumer:
        listen_for_messages(kafka_consumer)