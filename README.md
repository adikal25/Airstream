

```markdown
# Airstream

A real-time data streaming pipeline that simulates a production-grade architecture using Apache Kafka, Apache Spark, Apache Cassandra, and orchestrated by Airflow. It ingests data from a test API, processes it in real time, and stores it in a distributed NoSQL database.

---

## 🚀 Overview

This project demonstrates how to build an end-to-end real-time data pipeline using a modern open-source stack. Although the data source is a test API, the pipeline architecture is built to reflect real-world scenarios involving data ingestion, streaming, and storage across distributed systems.

---

## 🛠️ Technologies Used

- **Airflow** – Orchestrates the ingestion of data from an API
- **Kafka** – Acts as a real-time message broker
- **Spark Structured Streaming** – Processes Kafka messages in real time
- **Cassandra** – Stores processed data in a scalable, distributed manner
- **PostgreSQL** – Optional staging layer before Kafka
- **Python** – Used across ingestion, processing, and orchestration logic
- **docker** - for orchestrating and running instances of all the tools used

---

## 🔄 Data Flow

```

\[Test API]
↓
\[Airflow DAG → PostgreSQL (optional)]
↓
\[Kafka Producer → Kafka Broker (Zookeeper)]
↓
\[Spark Consumer (Structured Streaming)]
↓
\[Cassandra]

```

- Data is fetched from a test API and optionally stored in PostgreSQL.
- Kafka producer (triggered by Airflow) sends formatted data to a Kafka topic.
- Spark Structured Streaming job subscribes to the topic, parses the data, and writes it into Cassandra.

---


## 🧪 Sample Message Format (Kafka Payload)

```json
{
  "id": "uuid",
  "first_name": "John",
  "last_name": "Doe",
  "gender": "male",
  "address": "123 Test St",
  "post_code": "12345",
  "email": "john.doe@example.com",
  "username": "johndoe",
  "registered_date": "2025-06-30",
  "phone": "+123456789",
  "picture": "https://randomuser.me/api/portraits/men/1.jpg"
}
````

---

## 🔧 Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/your-username/streampulse.git
cd streampulse
```

### 2. Start services using Docker

```bash
docker-compose up -d
```

Ensure Kafka is available at `localhost:9092` and Cassandra at `localhost:9042`.

### 3. Set up Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Start the Kafka producer

```bash
python kafka_producer.py
```

### 5. Run the Spark consumer

```bash
python spark_consumer.py
```

---

## 🎯 Goals of the Project

* Practice integrating major components of a real-time data pipeline
* Learn how to consume and process Kafka messages using Spark
* Persist streaming data to a high-throughput NoSQL database (Cassandra)
* Use Airflow to manage end-to-end workflow automation
```
