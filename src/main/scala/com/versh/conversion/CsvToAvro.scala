package com.versh.conversion

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToAvro extends App {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("csv_to_avro_file")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR");

  //read csv with options
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("C:\\work\\repo\\data\\files\\5m Sales Records.csv")
  df.show()
  df.printSchema()

  //parquet does not support space in column name.
  val new_cols =  df.columns.map(x => x.replaceAll(" ", "_"))
  val df2 = df.toDF(new_cols : _*)

  //convert to avro
  df2.repartition(1).write.format("avro").mode(SaveMode.Overwrite)
    .save("C:\\out")

 /* df.write.format("avro").mode(SaveMode.Append)
    .save("src/main/resources/data/out/taxi_zones")*/

  //convert to avro by partition
/* df.write.partitionBy("service_zone")
    .format("avro")
    .mode(SaveMode.Overwrite)
    .save("C:/out")*/

}
