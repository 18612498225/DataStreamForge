from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def word_count(spark, input_text):
    """
    Counts words in a given string using PySpark.
    
    :param spark: SparkSession object.
    :param input_text: A string containing text to process.
    :return: A Spark DataFrame with words and their counts.
    """
    # Create a DataFrame from a list containing the input string
    # In a real application, you would read from a file or other source
    df = spark.createDataFrame([(1, input_text)], ["id", "text"])

    # Split the text into words, then explode into rows
    words_df = df.select(explode(split(col("text"), "\s+")).alias("word"))

    # Filter out empty strings that might result from multiple spaces
    non_empty_words_df = words_df.filter(col("word") != "")

    # Count the occurrences of each word
    word_counts_df = non_empty_words_df.groupBy("word").count()

    return word_counts_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PySparkWordCount").getOrCreate()

    sample_text = "hello world hello spark hello python"
    
    print(f"Processing sample text: '{sample_text}'")
    counts = word_count(spark, sample_text)

    print("Word counts:")
    counts.show()

    spark.stop()
