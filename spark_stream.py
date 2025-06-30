import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)


def create_keyspace(cassandra_session):
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info(
        "Cassandra keyspace 'spark_streams' created or already exists.")


def create_table(cassandra_session):
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    logging.info("Cassandra table 'created_users' created or already exists.")


def insert_user_data(cassandra_session, **user):
    logging.info("Inserting user data into Cassandra...")

    try:
        cassandra_session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user.get('id'), user.get('first_name'), user.get(
                'last_name'), user.get('gender'),
            user.get('address'), user.get('post_code'), user.get(
                'email'), user.get('username'),
            user.get('dob'), user.get('registered_date'), user.get(
                'phone'), user.get('picture')
        ))
        logging.info(
            f"Successfully inserted data for user {user.get('first_name')} {user.get('last_name')}")
    except Exception as e:
        logging.error(f"Failed to insert data into Cassandra: {e}")


def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName('RealTimeUserStreaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark session: {e}")
        return None


def read_kafka_stream(spark):
    try:
        kafka_stream_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka stream loaded successfully.")
        return kafka_stream_df
    except Exception as e:
        logging.error(f"Error reading Kafka stream: {e}")
        return None


def connect_to_cassandra():
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
        logging.info("Connected to Cassandra.")
        return cassandra_session
    except Exception as e:
        logging.error(f"Error connecting to Cassandra: {e}")
        return None


def parse_kafka_json_stream(kafka_df):
    # Define schema based on the expected Kafka message format
    json_schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Extract the JSON payload from Kafka and apply schema
    structured_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), json_schema).alias('user')) \
        .select("user.*")

    logging.info("Kafka stream successfully parsed into structured DataFrame.")
    return structured_df


if __name__ == "__main__":
    # Step 1: Initialising the Spark session
    spark_session = create_spark_session()

    if spark_session:
        # Step 2: Reading data stream from Kafka
        kafka_df = read_kafka_stream(spark_session)

        if kafka_df:
            # Step 3: Parse and extract fields from Kafka JSON messages
            user_df = parse_kafka_json_stream(kafka_df)

            # Step 4: Connect to Cassandra and prepare DB
            cassandra_session = connect_to_cassandra()
            if cassandra_session:
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                logging.info("Starting stream to Cassandra...")

                # Step 5: Start writing the structured stream to Cassandra
                query = user_df.writeStream \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("checkpointLocation", "/tmp/checkpoint") \
                    .option("keyspace", "spark_streams") \
                    .option("table", "created_users") \
                    .start()

                query.awaitTermination()
