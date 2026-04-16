from pyspark.sql.functions import col, from_json

from core.config import load_config
from streaming.runtime import create_spark_session
from streaming.schemas import get_weather_schema


def write_weather_batch(batch_df, batch_id, output_path: str):
    count = batch_df.count()
    print(f"weather_detail Batch {batch_id}, count={count}")

    if count == 0:
        return

    print("weather columns:", batch_df.columns)
    batch_df.show(5, truncate=False)

    (
        batch_df.write
        .mode("append")
        .option("header", False)
        .partitionBy("dt")
        .csv(output_path)
    )


def main():
    config = load_config()
    spark = create_spark_session("WeatherDetailStreamJob")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.weather_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str").select(
        from_json(col("json_str"), get_weather_schema()).alias("data")
    )

    weather_df = parsed_df.select("data.*")

    detail_df = weather_df.select(
        "collect_time",
        "country",
        "province",
        "city",
        "district",
        "district_id",
        "weather_text",
        "temperature",
        "feels_like",
        "humidity",
        "wind_class",
        "wind_dir",
        "precipitation_1h",
        "clouds",
        "visibility",
        "aqi",
        "pm25",
        "pm10",
        "pressure",
        "dt",
    ).filter(
        col("city").isNotNull() & col("collect_time").isNotNull() & col("dt").isNotNull()
    )

    query = (
        detail_df.writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: write_weather_batch(batch_df, batch_id, config.paths.weather_output_path))
        .option("checkpointLocation", config.paths.weather_checkpoint)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
