from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, when
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, DoubleType, IntegerType
)

spark = SparkSession.builder \
    .appName('SmartManuTech-IoT-Pipeline') \
    .config('spark.sql.shuffle.partitions', '8') \
    .config('spark.streaming.stopGracefullyOnShutdown', 'true') \
    .config('spark.cassandra.connection.host', 'cassandra-node') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField('machine_id', StringType()),
    StructField('timestamp', TimestampType()),
    StructField('temperature', DoubleType()),
    StructField('vibration', DoubleType()),
    StructField('production_speed', DoubleType()),
    StructField('energy_kwh', DoubleType()),
    StructField('error_code', IntegerType())
])

raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka-broker:9092') \
    .option('subscribe', 'iot-sensors') \
    .option('startingOffsets', 'latest') \
    .option('maxOffsetsPerTrigger', '10000') \
    .load()

parsed = raw_stream \
    .select(from_json(col('value').cast('string'), schema).alias('data')) \
    .select('data.*')

with_alerts = parsed.withColumn('alert_level',
    when(col('temperature') > 85, 'CRITICAL')
    .when(col('temperature') > 75, 'WARNING')
    .when(col('vibration') > 12, 'CRITICAL')
    .when(col('vibration') > 9, 'WARNING')
    .when(col('error_code') > 0, 'ERROR')
    .otherwise('OK')
)

agg_stream = with_alerts \
    .withWatermark('timestamp', '10 seconds') \
    .groupBy(
        window(col('timestamp'), '1 minute', '30 seconds'),
        col('machine_id')
    ) \
    .agg(
        avg('temperature').alias('avg_temp'),
        max('temperature').alias('max_temp'),
        avg('vibration').alias('avg_vibration'),
        avg('production_speed').alias('avg_speed'),
        avg('energy_kwh').alias('avg_energy')
    )

query_cassandra = with_alerts.writeStream \
    .outputMode('append') \
    .format('org.apache.spark.sql.cassandra') \
    .option('keyspace', 'smartmanutech') \
    .option('table', 'sensor_readings') \
    .option('checkpointLocation', '/tmp/spark-checkpoint/cassandra') \
    .trigger(processingTime='1 second') \
    .start()

query_console = with_alerts \
    .filter(col('alert_level') != 'OK') \
    .writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .start()

print("Pipeline iniciado. Esperando datos de sensores...")
query_cassandra.awaitTermination()