import pytest
from pyspark.sql import SparkSession
from src.jobs.spark.word_count import word_count # Assuming src is in PYTHONPATH or project root

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("PySparkWordCountTests") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_word_count_empty_string(spark_session):
    input_text = ""
    result_df = word_count(spark_session, input_text)
    assert result_df.count() == 0

def test_word_count_simple_text(spark_session):
    input_text = "hello world hello"
    result_df = word_count(spark_session, input_text)
    
    expected_data = [("hello", 2), ("world", 1)]
    expected_df = spark_session.createDataFrame(expected_data, ["word", "count"])
    
    # Collect and sort for comparison as order might differ
    result_list = sorted([(row['word'], row['count']) for row in result_df.collect()])
    expected_list = sorted([(row['word'], row['count']) for row in expected_df.collect()])
    
    assert result_list == expected_list

def test_word_count_with_punctuation_and_case(spark_session):
    # Note: Current word_count is basic and doesn't handle punctuation or case normalization.
    # This test reflects its current behavior.
    input_text = "Hello world! HELLO Python."
    result_df = word_count(spark_session, input_text)
    
    # Expected based on simple split by space
    expected_data = [("Hello", 1), ("world!", 1), ("HELLO", 1), ("Python.", 1)]
    expected_df = spark_session.createDataFrame(expected_data, ["word", "count"])

    result_list = sorted([(row['word'], row['count']) for row in result_df.collect()])
    expected_list = sorted([(row['word'], row['count']) for row in expected_df.collect()])
    
    assert result_list == expected_list

def test_word_count_multiple_spaces(spark_session):
    input_text = "word  another   word"
    result_df = word_count(spark_session, input_text)
    
    expected_data = [("word", 2), ("another", 1)]
    expected_df = spark_session.createDataFrame(expected_data, ["word", "count"])
    
    result_list = sorted([(row['word'], row['count']) for row in result_df.collect()])
    expected_list = sorted([(row['word'], row['count']) for row in expected_df.collect()])
    
    assert result_list == expected_list
