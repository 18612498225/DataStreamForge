from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udf

@udf(result_type=DataTypes.STRING())
def normalize_word(word):
    return word.lower().strip(".,!?")

def word_count_job(env, input_text):
    """
    Counts words in a given string using PyFlink (Table API).
    
    :param env: StreamExecutionEnvironment
    :param input_text: A string containing text to process.
    :return: A PyFlink Table with words and their counts.
    """
    t_env = StreamTableEnvironment.create(env)

    # Create a Table from a list containing the input string
    # In a real application, you would use a source connector
    elements = [(1, sentence) for sentence in input_text.splitlines() if sentence.strip()]
    if not elements: # Handle empty or whitespace-only input
         elements = [(1, "")]

    source_table = t_env.from_elements(elements, schema=['id', 'text'])

    # Split sentences into words, normalize, and count
    # Using UDF for normalization to showcase its usage
    word_counts_table = source_table \
        .flat_map(lambda x: x.text.split()) \
        .map(normalize_word).alias("word") \
        .filter(col('word') != '') \
        .group_by(col('word')) \
        .select(col('word'), col('word').count.alias('count'))

    return word_counts_table

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    # PyFlink Table API in batch mode by default if using from_elements without a streaming source.
    # For streaming, you'd configure sources and sinks differently.
    # env.set_parallelism(1) # Optional: set parallelism

    sample_text = "Hello Flink hello world\nPyFlink is great Flink"
    
    print(f"Processing sample text: '{sample_text}'")
    counts_table = word_count_job(env, sample_text)

    print("Word counts (execute and print):")
    # For local execution and display, you can convert to pandas or iterate
    # Note: .execute().print() is more for CLI, here we iterate for programmatic access
    with counts_table.execute().collect() as results:
        for row in results:
            print(row)
