from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, first, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

def define_schemas():
    """Defines schemas for the input CSV files."""
    user_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", FloatType(), True)
    ])

    order_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_date", DateType(), True) # Assuming YYYY-MM-DD format from generator
    ])
    return user_schema, product_schema, order_schema

def run_user_profiling(spark: SparkSession, data_path: str):
    """
    Runs the user profiling job.
    Reads mock data, processes it, and generates user profiles.
    """
    user_schema, product_schema, order_schema = define_schemas()

    # Load data
    users_df = spark.read.csv(f"{data_path}/users.csv", header=True, schema=user_schema)
    products_df = spark.read.csv(f"{data_path}/products.csv", header=True, schema=product_schema)
    orders_df = spark.read.csv(f"{data_path}/orders.csv", header=True, schema=order_schema)

    print("Sample data loaded:")
    users_df.show(5, truncate=False)
    products_df.show(5, truncate=False)
    orders_df.show(5, truncate=False)

    # Calculate total spent per order item
    order_item_spend_df = orders_df.join(products_df, "product_id") \
                                   .withColumn("item_total_spent", col("quantity") * col("price"))

    # --- Calculate profile attributes ---

    # 1. Basic user info + last order date
    user_base_profile_df = users_df.join(
        order_item_spend_df.groupBy("user_id").agg(max("order_date").alias("last_order_date")),
        "user_id",
        "left" # Keep all users, even if they have no orders
    )

    # 2. Aggregations from orders: total_orders, total_spent
    user_order_aggregates_df = order_item_spend_df.groupBy("user_id").agg(
        count("order_id").alias("total_orders"),
        sum("item_total_spent").alias("total_spent")
    )

    # 3. Join aggregates back to user base profile
    user_profile_df = user_base_profile_df.join(user_order_aggregates_df, "user_id", "left")

    # 4. Calculate avg_order_value (handle division by zero if total_orders is 0)
    user_profile_df = user_profile_df.withColumn(
        "avg_order_value",
        col("total_spent") / col("total_orders") # This will be null if total_orders is null or zero
    )
    
    # Fill NA for users with no orders
    user_profile_df = user_profile_df.fillna(0, subset=["total_orders", "total_spent", "avg_order_value"])


    # 5. Determine favorite category
    # Count occurrences of each category per user
    user_category_counts_df = order_item_spend_df.groupBy("user_id", "category") \
                                                 .agg(count("*").alias("category_count"))

    # Find the category with the max count for each user
    # Using window function might be an alternative, but this approach is also common:
    # Create a unique rank for categories per user
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("user_id").orderBy(desc("category_count"))
    
    user_favorite_category_df = user_category_counts_df \
        .withColumn("rank", col("category_count") / sum("category_count").over(Window.partitionBy("user_id"))) \
        .filter(col("rank") >= 0.0) \
        .orderBy("user_id", desc("rank")) \
        .groupBy("user_id") \
        .agg(first("category").alias("favorite_category")) # Takes the first one in case of ties after ordering


    # Join favorite category to profile
    user_profile_df = user_profile_df.join(user_favorite_category_df, "user_id", "left")
    
    # Select final columns and show results
    final_profile_df = user_profile_df.select(
        "user_id", "name", "age", "city",
        "total_orders", "total_spent", "avg_order_value",
        "favorite_category", "last_order_date"
    ).orderBy("user_id")

    print("--- Generated User Profiles ---")
    final_profile_df.show(truncate=False)
    
    # For testing or further processing, you might want to return this DataFrame
    return final_profile_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("UserProfilingSparkJob") \
        .master("local[*]") \
        .getOrCreate()

    # Assuming the script is run from the project root, so data/mock is accessible
    # If running from src/jobs/spark/, path would be ../../../data/mock
    # For simplicity, this example assumes it's run in a context where 'data/mock' is valid.
    # A more robust solution would use command-line args or config for data paths.
    mock_data_path = "data/mock" 
    
    print(f"Starting user profiling job. Reading data from: {mock_data_path}")
    run_user_profiling(spark, mock_data_path)

    spark.stop()
