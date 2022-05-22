package com.versh.conversion

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToOrc  extends App {

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


  //repartitioning to get one file for comparison (this is not good practice)
  df.repartition(1).write.format("orc").mode(SaveMode.Overwrite).option("compression","snappy")
    .save("C:\\out")

}
