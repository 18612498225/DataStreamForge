import pytest
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from src.jobs.flink.word_count import word_count_job # Assuming src is in PYTHONPATH

@pytest.fixture(scope="session")
def flink_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Configure for local testing if needed, e.g., parallelism
    env.set_parallelism(1)
    return env

def test_pyflink_word_count_empty_string(flink_env):
    input_text = ""
    result_table = word_count_job(flink_env, input_text)
    
    results_list = list(result_table.execute().collect())
    assert len(results_list) == 0

def test_pyflink_word_count_simple_text(flink_env):
    input_text = "hello world hello flink"
    result_table = word_count_job(flink_env, input_text)
    
    # Collect results and convert to a comparable format (e.g., set of tuples)
    # Order is not guaranteed by default in distributed processing.
    results_set = set((row['word'], row['count']) for row in result_table.execute().collect())
    
    expected_set = {("hello", 2), ("world", 1), ("flink", 1)}
    assert results_set == expected_set

def test_pyflink_word_count_with_punctuation_and_case(flink_env):
    input_text = "Hello Flink! HELLO Python."
    result_table = word_count_job(flink_env, input_text)
    
    results_set = set((row['word'], row['count']) for row in result_table.execute().collect())
    
    # UDF normalizes to lowercase and removes common punctuation
    expected_set = {("hello", 2), ("flink", 1), ("python", 1)}
    assert results_set == expected_set

def test_pyflink_word_count_multiple_lines_and_spaces(flink_env):
    input_text = "word  another   word\nnew  line word"
    result_table = word_count_job(flink_env, input_text)
    
    results_set = set((row['word'], row['count']) for row in result_table.execute().collect())
    
    expected_set = {("word", 3), ("another", 1), ("new", 1), ("line", 1)}
    assert results_set == expected_set
