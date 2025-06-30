import unittest
from unittest.mock import MagicMock, patch
from spark_consumer import (
    create_keyspace, create_table, insert_user_data,
    create_spark_session, parse_kafka_json_stream
)
from airflow_dag import get_data, format_data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


class TestStreamingPipeline(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("UnitTest") \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_parse_kafka_json_stream(self):
        data = [('{"id": "123", "first_name": "Ada", "last_name": "Lovelace", "gender": "female", "address": "1 AI Street", "post_code": "10001", "email": "ada@tech.com", "username": "ada_l", "registered_date": "2025-06-30", "phone": "1234567890", "picture": "url.jpg"}',)]
        schema = StructType([StructField("value", StringType(), True)])
        df = self.spark.createDataFrame(data, schema)
        result = parse_kafka_json_stream(df)
        self.assertIn("first_name", result.columns)
        self.assertEqual(result.columns[0], "id")

    def test_create_keyspace_executes(self):
        mock_session = MagicMock()
        create_keyspace(mock_session)
        mock_session.execute.assert_called_once()

    def test_create_table_executes(self):
        mock_session = MagicMock()
        create_table(mock_session)
        mock_session.execute.assert_called_once()

    def test_format_data_structure(self):
        raw = {
            "gender": "male",
            "name": {"first": "Alan", "last": "Turing"},
            "location": {
                "street": {"number": 221, "name": "B Baker Street"},
                "city": "London",
                "state": "Greater London",
                "country": "UK",
                "postcode": "NW16XE"
            },
            "email": "alan@maths.uk",
            "login": {"username": "atur"},
            "dob": {"date": "1912-06-23"},
            "phone": "+44 12345678",
            "picture": {"medium": "img.jpg"}
        }
        formatted = format_data(raw)
        self.assertEqual(formatted["first_name"], "Alan")
        self.assertIn("address", formatted)
        self.assertIn("picture", formatted)

    @patch('airflow_dag.KafkaProducer')
    def test_kafka_producer_send(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        from airflow_dag import stream_data
        stream_data()
        mock_producer.send.assert_called_once()
    