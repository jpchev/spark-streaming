import sys
import json
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql.types import *
from pyspark.sql.functions import *

balises_url = "https://data.ffvl.fr/json/balises.json"

db_target_url = "jdbc:mysql://mysqldb:3306/meteo"
db_target_properties = {"user":"pat", "password":"metheny", "driver": "org.mariadb.jdbc.Driver"}

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise IOError("Invalid usage; the correct format is:\nmeteo_analyse.py <hostname> <port>")

    spark = (SparkSession
               .builder
               .appName("meteoAnalyse")
               .enableHiveSupport()
               .getOrCreate())

    spc = spark.sparkContext

    spc.addFile(balises_url)

    # read data
    balises_df = spark.read.option("multiline","true") \
            .json("file://" + SparkFiles.get("balises.json"))
    balises_df = balises_df.select(
            col("idBalise"),
            col("nom"),
            concat(col("longitude"), lit(' '), col("latitude")).alias("coords")
            )

    # Create DataFrame representing the stream of input lines from connection to localhost:20000
    releves_meteo_stream_df = spark \
        .readStream \
        .format("socket") \
        .option("host", sys.argv[1]) \
        .option("port", sys.argv[2]) \
        .load()

    releves_meteo_schema = ArrayType(
            StructType([
        StructField('idbalise', StringType(), True),
        StructField('date', StringType(), True),
        StructField('vitesseVentMoy', StringType(), True),
        StructField('vitesseVentMax', StringType(), True),
        StructField('vitesseVentMin', StringType(), True),
        StructField('directVentMoy', StringType(), True),
        StructField('directVentInst', StringType(), True),
        StructField('temperature', StringType(), True),
        StructField('hydrometrie', StringType(), True),
        StructField('pression', StringType(), True),
        StructField('luminosite', StringType(), True),
        StructField('LUM', StringType(), True)
            ])
        )


    # parse json
    releves_meteo_json_df = releves_meteo_stream_df.withColumn("jsonData", from_json(col("value"), releves_meteo_schema)).drop("value")

    # explode json lines
    releves_meteo_exploded_json_df = releves_meteo_json_df.select(explode(releves_meteo_json_df.jsonData).alias("jsonData"))

    # drop duplicates
    releves_meteo_strings_df = releves_meteo_exploded_json_df.select(col("jsonData.*")).dropDuplicates(["idbalise", "date"])

    # convert types
    releves_meteo_df = releves_meteo_strings_df.select(
             col("idbalise"),
             to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss").alias("date"),
             col("temperature").cast(FloatType()),
             col("vitesseVentMoy").cast(FloatType()),
             col("vitesseVentMax").cast(FloatType()),
             col("vitesseVentMin").cast(FloatType()),
             col("directVentMoy").cast(FloatType()),
             col("directVentInst").cast(FloatType()),
             col("hydrometrie").cast(FloatType()),
            ).withWatermark("date", "5 minutes")  # maximal delay

    # join
    data_df = releves_meteo_df \
                .join(balises_df, releves_meteo_df.idbalise == balises_df.idBalise, "inner") \
                .drop(balises_df.idBalise)

    data_df = data_df.filter("temperature is not null").groupBy(
                     window("date", "30 minutes", "15 minutes"),
                     "idbalise",
                     "nom",
                     "coords",
                     ).agg(
                            avg("temperature").alias("avg_temperature"),
                            avg("vitesseVentMoy").alias("avg_vitesseVentMoy"),
                            avg("vitesseVentMax").alias("avg_vitesseVentMax"),
                            avg("vitesseVentMin").alias("avg_vitesseVentMin"),
                            avg("directVentMoy").alias("avg_directVentMoy"),
                            avg("directVentInst").alias("avg_directVentInst"),
                            avg("hydrometrie").alias("avg_hydrometrie")
                             )

    data_df = data_df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("idbalise"),
                col("nom"),
                col("coords"),
                col("avg_temperature"),
                col("avg_vitesseVentMoy"),
                col("avg_vitesseVentMax"),
                col("avg_vitesseVentMin"),
                col("avg_directVentMoy"),
                col("avg_directVentInst"),
                col("avg_hydrometrie")
            )

    #data_df \
    #   .writeStream\
    #   .outputMode("update") \
    #   .format("console") \
    #   .option("truncate", False) \
    #   .start() \
    #  .awaitTermination()

    def process_row(row, batch_id):
        # Process row
        row.write.jdbc(url=db_target_url, table="meteo_temperature_avg", mode="append", properties=db_target_properties)
        pass

    # reduce (coalesce) the number of partitions to the number of available CPU cores (4)
    # to avoid database overload (too many connections)
    data_df.coalesce(4) \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(process_row) \
        .option("checkpointLocation", "checkpoint_meteo/") \
        .start() \
        .awaitTermination()

                                                   