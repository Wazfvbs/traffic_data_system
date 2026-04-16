from pyspark.sql.functions import col, from_json

from core.config import load_config
from streaming.runtime import create_spark_session
from streaming.schemas import get_traffic_schema


def write_detail_batch(batch_df, batch_id, output_path: str):
    count = batch_df.count()
    print(f"traffic_detail Batch {batch_id}, count={count}")

    if count == 0:
        return

    batch_df.show(5, truncate=False)

    (
        batch_df.write
        .mode("append")
        .partitionBy("dt")
        .parquet(output_path)
    )


def main():
    config = load_config()
    spark = create_spark_session("TrafficDetailStreamJob")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.traffic_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str").select(
        from_json(col("json_str"), get_traffic_schema()).alias("data")
    )

    traffic_df = parsed_df.select("data.*")

    detail_df = traffic_df.select(
        "collect_time",
        "city",
        "district",
        "road_id",
        "road_name",
        "description",
        "overall_status",
        "overall_status_desc",
        "section_desc",
        "road_type",
        "congestion_distance",
        "speed",
        "section_status",
        "congestion_trend",
        "dt",
    ).filter(
        col("road_name").isNotNull() & col("collect_time").isNotNull() & col("dt").isNotNull()
    )

    query = (
        detail_df.writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: write_detail_batch(batch_df, batch_id, config.paths.traffic_detail_output_path))
        .option("checkpointLocation", config.paths.traffic_detail_checkpoint)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
