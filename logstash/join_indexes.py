from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

appName = 'join data'

def extract_json(stream_df, schema):
    # parse json
    stream_json_df = stream_df.withColumn("jsonData", from_json(col("value"), schema)).drop("value")
    
    # explode json lines
    exploded_json_df = stream_json_df.select(explode(stream_json_df.jsonData).alias("jsonData"))
    
    # get json fields
    return exploded_json_df.select(col("jsonData.*"))
    
def join():
    spark = (SparkSession
               .builder
               .appName(appName)
               .enableHiveSupport()
               .master("local[*]")
               .getOrCreate())
               
    spark.sparkContext.setLogLevel("ERROR")

    postfix_stream_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 8888) \
        .load()
    
    mailet_stream_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 8898) \
        .load() \
    
    
    
    
    postfix_stream_schema = ArrayType(
                StructType([
            StructField('postfix_message-id', StringType(), True),
            StructField('postfix_delay_transmission', FloatType(), True),
            StructField('@timestamp', TimestampType(), True),
                ])
            )
            
    postfix_strings_df = extract_json(postfix_stream_df, postfix_stream_schema)
    
    # convert types
    postfix_df = postfix_strings_df.select(
                              col("postfix_message-id").alias("id_p"),
                              col("postfix_delay_transmission"),
                              col("@timestamp").alias("@timestamp_p"),
                         ).withWatermark("@timestamp_p", "15 seconds")  # maximal delay
    
    
    
    
    mailet_stream_schema = ArrayType(
                StructType([
            StructField('message-id', StringType(), True),
            StructField('james_typeSujetMessage', StringType(), True),
            StructField('@timestamp', TimestampType(), True),
                ])
            )
    
    mailet_strings_df = extract_json(mailet_stream_df, mailet_stream_schema)
    
    # convert types
    mailet_df = mailet_strings_df.select(
                              col("message-id").alias("id_m"),
                              col("@timestamp").alias("@timestamp_m"),
                              col("james_typeSujetMessage"),
                         ).withWatermark("@timestamp_m", "15 seconds")  # maximal delay
    
    
    # join
    data_df = postfix_df.join(mailet_df, postfix_df.id_p == mailet_df.id_m, "inner")
    
    data_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start() \
        .awaitTermination()

join()