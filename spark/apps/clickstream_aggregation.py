"""
PySpark Structured Streaming Application for Feature Store
Reads clickstream events from Kafka, performs windowed aggregations,
and sinks to Parquet for the Offline Feature Store.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # Internal Docker network
KAFKA_TOPIC = "user_clicks"
CHECKPOINT_LOCATION = "/home/jovyan/data/checkpoints/user_clicks_agg"
OUTPUT_PATH = "/home/jovyan/data/offline_store/user_click_features"

# Window configuration
WINDOW_DURATION = "1 hour"
SLIDE_DURATION = "10 minutes"
WATERMARK_DELAY = "15 minutes"


def get_spark_session() -> SparkSession:
    """Create SparkSession with Kafka and Parquet support."""
    return (
        SparkSession.builder
        .appName("FeatureStore-ClickstreamAggregation")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def get_event_schema() -> StructType:
    """Define schema for clickstream events."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),  # ISO format string
        StructField("session_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_tier", StringType(), True),
        StructField("view_duration_sec", IntegerType(), True),
        StructField("click_position", IntegerType(), True),
        StructField("referrer", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
    ])


def read_from_kafka(spark: SparkSession):
    """Read streaming data from Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(kafka_df, schema: StructType):
    """Parse JSON events from Kafka value column."""
    return (
        kafka_df
        # Extract value as string
        .selectExpr("CAST(value AS STRING) as json_value")
        # Parse JSON with schema
        .select(F.from_json(F.col("json_value"), schema).alias("data"))
        # Flatten the struct
        .select("data.*")
        # Convert timestamp string to proper timestamp type
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
        )
        # Handle alternative timestamp formats
        .withColumn(
            "event_timestamp",
            F.coalesce(
                F.col("event_timestamp"),
                F.to_timestamp(F.col("timestamp"))
            )
        )
        # Add processing timestamp
        .withColumn("processing_time", F.current_timestamp())
        # Filter out null user_ids
        .filter(F.col("user_id").isNotNull())
    )


def apply_windowed_aggregation(events_df):
    """
    Apply windowed aggregation with watermarking for late data handling.
    
    Calculates per user_id over a 1-hour sliding window (sliding every 10 min):
    - Total event count
    - Click count
    - View count
    - Cart count
    - Purchase count
    - Unique products viewed
    - Unique categories
    - Total revenue (from purchases)
    """
    return (
        events_df
        # Apply watermark for late data handling
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        # Group by user_id and time window
        .groupBy(
            F.col("user_id"),
            F.col("user_country"),
            F.col("user_tier"),
            F.window(
                F.col("event_timestamp"),
                WINDOW_DURATION,
                SLIDE_DURATION
            )
        )
        .agg(
            # Event counts by type
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            
            # Engagement metrics
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("product_category").alias("unique_categories"),
            F.countDistinct("session_id").alias("session_count"),
            
            # Device distribution
            F.sum(F.when(F.col("device_type") == "mobile", 1).otherwise(0)).alias("mobile_events"),
            F.sum(F.when(F.col("device_type") == "desktop", 1).otherwise(0)).alias("desktop_events"),
            
            # Revenue (from purchases)
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("price") * F.col("quantity"))
                .otherwise(0)
            ).alias("total_revenue"),
            
            # Average view duration
            F.avg(
                F.when(F.col("event_type") == "view", F.col("view_duration_sec"))
            ).alias("avg_view_duration_sec"),
            
            # Top referrer (mode approximation using first)
            F.first("referrer", ignorenulls=True).alias("primary_referrer"),
        )
        # Extract window boundaries
        .select(
            F.col("user_id"),
            F.col("user_country"),
            F.col("user_tier"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("total_events"),
            F.col("click_count"),
            F.col("view_count"),
            F.col("cart_count"),
            F.col("purchase_count"),
            F.col("unique_products"),
            F.col("unique_categories"),
            F.col("session_count"),
            F.col("mobile_events"),
            F.col("desktop_events"),
            F.col("total_revenue"),
            F.col("avg_view_duration_sec"),
            F.col("primary_referrer"),
            # Derived features
            (F.col("click_count") / F.col("view_count")).alias("click_through_rate"),
            (F.col("cart_count") / F.col("click_count")).alias("cart_rate"),
            (F.col("purchase_count") / F.col("cart_count")).alias("conversion_rate"),
        )
        # Add partition column for date-based partitioning
        .withColumn("event_date", F.to_date(F.col("window_start")))
        # Add feature timestamp (for Feast)
        .withColumn("feature_timestamp", F.col("window_end"))
    )


def write_to_parquet(aggregated_df, output_path: str, checkpoint_location: str):
    """
    Write aggregated features to Parquet, partitioned by date.
    Uses append mode to continuously add new windowed results.
    """
    return (
        aggregated_df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_location)
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="1 minute")  # Process micro-batches every minute
        .start()
    )


def write_to_console(aggregated_df):
    """Write to console for debugging (use in development)."""
    return (
        aggregated_df.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    """Main entry point for the streaming application."""
    print("=" * 70)
    print("🚀 Feature Store - Clickstream Aggregation Pipeline")
    print("=" * 70)
    
    # Create Spark session
    print("\n📦 Initializing Spark Session...")
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"   Spark Version: {spark.version}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Window: {WINDOW_DURATION} sliding every {SLIDE_DURATION}")
    print(f"   Watermark: {WATERMARK_DELAY}")
    print(f"   Output: {OUTPUT_PATH}")
    
    # Define schema
    event_schema = get_event_schema()
    
    # Read from Kafka
    print("\n📡 Connecting to Kafka...")
    kafka_df = read_from_kafka(spark)
    
    # Parse events
    print("🔄 Parsing JSON events...")
    events_df = parse_events(kafka_df, event_schema)
    
    # Apply windowed aggregation
    print("📊 Applying windowed aggregation...")
    aggregated_df = apply_windowed_aggregation(events_df)
    
    # Start streaming queries
    print(f"\n✅ Starting streaming to Parquet at {OUTPUT_PATH}")
    parquet_query = write_to_parquet(
        aggregated_df, 
        OUTPUT_PATH, 
        CHECKPOINT_LOCATION
    )
    
    # Optionally also write to console for monitoring
    # console_query = write_to_console(aggregated_df)
    
    print("\n🎯 Streaming pipeline running. Press Ctrl+C to stop.")
    print("-" * 70)
    
    # Wait for termination
    try:
        parquet_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down streaming pipeline...")
        parquet_query.stop()
        spark.stop()
        print("✨ Pipeline stopped gracefully.")


if __name__ == "__main__":
    main()
