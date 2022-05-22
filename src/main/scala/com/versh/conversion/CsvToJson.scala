package com.versh.conversion

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToJson extends App {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("csv_to_json_file")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR");

  //read csv with options
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("C:\\work\\repo\\data\\files\\5m Sales Records.csv")
  df.show()
  df.printSchema()

  //convert to csv
  df.repartition(1).write.mode(SaveMode.Overwrite).json("c:\\out")
}
