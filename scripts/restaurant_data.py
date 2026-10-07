import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    count,
    when,
    trim,
    isnan,
    lit,
    sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    ArrayType,
    DoubleType
)
from pyspark.sql.avro.functions import from_avro


# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("Restaurant Data Processing") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.shuffle.file.buffer", "64k") \
    .config("spark.reducer.maxSizeInFlight", "48m") \
    .config("spark.network.timeout", "120s") \
    .config("spark.io.compression.codec", "zstd") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "3") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10m") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()


# ============================================================
# FILES
# ============================================================

OFFSETS_FILE = "/opt/airflow/scripts/offsets.json"

SCHEMA_FILE = "/opt/airflow/schemas/restaurants.avsc"

TOPIC = "restaurant_topic"


# ============================================================
# AVRO SCHEMA
# ============================================================

restaurant_schema = StructType([
    StructField("camis", StringType(), True),
    StructField("dba", StringType(), True),
    StructField("boro", StringType(), True),
    StructField("building", StringType(), True),
    StructField("street", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("cuisine_description", StringType(), True),
    StructField("inspection_date", StringType(), True),
    StructField("action", StringType(), True),
    StructField("violation_code", StringType(), True),
    StructField("violation_description", StringType(), True),
    StructField("critical_flag", StringType(), True),
    StructField("score", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("grade_date", StringType(), True),
    StructField("record_date", StringType(), True),
    StructField("inspection_type", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("community_board", StringType(), True),
    StructField("council_district", StringType(), True),
    StructField("census_tract", StringType(), True),
    StructField("bin", StringType(), True),
    StructField("bbl", StringType(), True),
    StructField("nta", StringType(), True),
    StructField(
        "location",
        StructType([
            StructField("type", StringType(), True),
            StructField(
                "coordinates",
                ArrayType(DoubleType()),
                True
            )
        ]),
        True
    )
])


# ============================================================
# READ STARTING OFFSETS
# ============================================================

with open(OFFSETS_FILE, "r") as offset_file:
    starting_offsets = json.load(offset_file)


# ============================================================
# READ AVRO SCHEMA
# ============================================================

with open(SCHEMA_FILE, "r") as schema_file:
    avro_schema = schema_file.read()


# ============================================================
# READ FROM KAFKA
# ============================================================

read_df = (
    spark.read
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "broker:9093,broker2:9093,broker3:9093"
    )
    .option("subscribe", TOPIC)
    .option(
        "startingOffsets",
        json.dumps(starting_offsets)
    )
    .load()
)


# ============================================================
# DECODE AVRO
# ============================================================

decoded_df = read_df.select(
    "partition",
    "offset",
    from_avro(
        col("value").substr(6, 1000000),
        avro_schema
    ).alias("restaurant_data")
)


# ============================================================
# FLATTEN AVRO RECORD
# ============================================================

restaurant_df = decoded_df.select(
    "partition",
    "offset",
    "restaurant_data.*"
)


# ============================================================
# TRANSFORM DATA
# ============================================================

transformed_df = (
    decoded_df
    .select(
        lit(TOPIC).alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("restaurant_data.camis").alias("camis"),
        col("restaurant_data.dba").alias("dba"),
        col("restaurant_data.boro").alias("boro"),
        col("restaurant_data.building").alias("building"),
        col("restaurant_data.street").alias("street"),
        col("restaurant_data.zipcode").alias("zipcode"),
        col("restaurant_data.phone").alias("phone"),
        col("restaurant_data.cuisine_description").alias("cuisine_description"),
        col("restaurant_data.inspection_date").alias("inspection_date"),
        col("restaurant_data.action").alias("action"),
        col("restaurant_data.violation_code").alias("violation_code"),
        col("restaurant_data.violation_description").alias("violation_description"),
        col("restaurant_data.critical_flag").alias("critical_flag"),
        col("restaurant_data.score").alias("score"),
        col("restaurant_data.grade").alias("grade"),
        col("restaurant_data.grade_date").alias("grade_date"),
        col("restaurant_data.record_date").alias("record_date"),
        col("restaurant_data.inspection_type").alias("inspection_type"),
        col("restaurant_data.community_board").alias("community_board"),
        col("restaurant_data.council_district").alias("council_district"),
        col("restaurant_data.census_tract").alias("census_tract"),
        col("restaurant_data.bin").alias("bin"),
        col("restaurant_data.bbl").alias("bbl"),
        col("restaurant_data.nta").alias("nta")
    )
    .withColumn(
        "inspection_date",
        to_timestamp(col("inspection_date"))
    )
    .withColumn(
        "grade_date",
        to_timestamp(col("grade_date"))
    )
    .withColumn(
        "record_date",
        to_timestamp(col("record_date"))
    )
    .withColumn(
        "score",
        col("score").cast(IntegerType())
    )
    .withColumn(
        "community_board",
        col("community_board").cast(IntegerType())
    )
    .withColumn(
        "council_district",
        col("council_district").cast(IntegerType())
    )
    .withColumn(
        "census_tract",
        col("census_tract").cast(IntegerType())
    )
    .withColumn(
        "bin",
        col("bin").cast(LongType())
    )
    .withColumn(
        "bbl",
        col("bbl").cast(LongType())
    )
)


# ============================================================
# POSTGRESQL
# ============================================================

POSTGRES_URL = "jdbc:postgresql://postgres:5432/restaurant"

POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

POSTGRES_TABLE = "staging.restaurant_inspections"


# ============================================================
# CLEANED DATA
# ============================================================

cleaned_df = transformed_df


# ============================================================
# REMOVE DUPLICATES INSIDE CURRENT BATCH
# ============================================================

batch_df = cleaned_df.dropDuplicates(
    ["kafka_topic", "kafka_partition", "kafka_offset"]
)


# ============================================================
# READ ALREADY PROCESSED KAFKA RECORDS
# ============================================================

existing_keys_df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        POSTGRES_URL
    )
    .option(
        "dbtable",
        """
        (
            SELECT
                kafka_topic,
                kafka_partition,
                kafka_offset
            FROM staging.restaurant_inspections
        ) AS existing_keys
        """
    )
    .option(
        "user",
        POSTGRES_PROPERTIES["user"]
    )
    .option(
        "password",
        POSTGRES_PROPERTIES["password"]
    )
    .option(
        "driver",
        POSTGRES_PROPERTIES["driver"]
    )
    .load()
)


# ============================================================
# KEEP ONLY NEW KAFKA RECORDS
# ============================================================

new_rows_df = batch_df.join(
    existing_keys_df,
    ["kafka_topic", "kafka_partition", "kafka_offset"],
    "left_anti"
)


# ============================================================
# WRITE NEW RECORDS TO POSTGRESQL
# ============================================================

(
    new_rows_df
    .write
    .format("jdbc")
    .option("url", POSTGRES_URL)
    .option("dbtable", POSTGRES_TABLE)
    .option("user", POSTGRES_PROPERTIES["user"])
    .option("password", POSTGRES_PROPERTIES["password"])
    .option("driver", POSTGRES_PROPERTIES["driver"])
    .mode("append")
    .save()
)


# ============================================================
# UPDATE OFFSETS ONLY AFTER SUCCESSFUL POSTGRESQL WRITE
# ============================================================

batch_max_offsets = (
    read_df
    .groupBy("partition")
    .max("offset")
    .collect()
)


updated_offsets = json.loads(
    json.dumps(starting_offsets)
)


for row in batch_max_offsets:

    partition = str(row["partition"])
    max_offset = int(row["max(offset)"])

    updated_offsets[TOPIC][partition] = max_offset + 1


# ============================================================
# ATOMIC OFFSET FILE UPDATE
# ============================================================

temporary_offsets_file = OFFSETS_FILE + ".tmp"


with open(temporary_offsets_file, "w") as offset_file:

    json.dump(
        updated_offsets,
        offset_file,
        indent=2
    )


os.replace(
    temporary_offsets_file,
    OFFSETS_FILE
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()