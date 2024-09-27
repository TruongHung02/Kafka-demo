import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Cấu hình ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'invoices'

def create_producer():
    """Tạo một Kafka Producer với cơ chế thử lại."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # Mã hóa tin nhắn dưới dạng JSON bytes
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Số lần thử lại nếu gửi thất bại
            retries=5
        )
        print("✅ Kết nối đến Kafka thành công!")
        return producer
    except NoBrokersAvailable:
        print(f"❌ Không thể kết nối đến broker tại {KAFKA_BROKER}. Vui lòng kiểm tra lại Kafka server.")
        return None

def send_messages(producer: KafkaProducer):
    """Gửi các tin nhắn mẫu đến Kafka."""
    for i in range(1, 11):
        message = {
            'invoice_id': f'INV-{i:04d}',
            'amount': 100.0 + (i * 10),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"-> Đang gửi tin nhắn: {message}")
        
        # Gửi tin nhắn đến topic
        producer.send(TOPIC_NAME, value=message)
        
        # Đợi 1 giây giữa các lần gửi
        time.sleep(1)

if __name__ == "__main__":
    kafka_producer = create_producer()
    
    if kafka_producer:
        try:
            send_messages(kafka_producer)
        finally:
            # Đảm bảo tất cả tin nhắn đang chờ được gửi đi
            print("... Đang đợi gửi hết tin nhắn.")
            kafka_producer.flush()
            
            # Đóng kết nối
            print("... Đóng kết nối producer.")
            kafka_producer.close()
            print("✅ Gửi tin nhắn hoàn tất.")