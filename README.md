
## Cài đặt


### Chạy kafka server

```bash
    docker compose up -d

```

* Tạo topic

```bash
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic my-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

```

### Cài đặt python ubuntu
```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

```

* Chạy producer

```bash
    python code/producer.py

```

* Chạy consumer

```bash
    python code/consumer.py

```

