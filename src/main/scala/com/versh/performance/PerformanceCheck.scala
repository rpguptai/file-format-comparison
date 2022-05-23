package com.versh.performance

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object PerformanceCheck {

  def groupByCheck(df: DataFrame): Long = {
    df.groupBy("Region").count().count()
  }

  def randomBatch(df: DataFrame):Array[Row] ={
    df.sample(0.5).collect()

  }

  def distinctCheck(df: DataFrame):Long={
    df.distinct().count()
  }

  def getDataFrame(spark:SparkSession,storageType:String,path:String):DataFrame={
  val df = storageType match {
    case "csv"  => spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(path+".csv")
    case "avro"  => spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .format("avro").load(path+".avro")
    case "json"  => spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .format("json").load(path+".json")
    case "orc"  => spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .format("orc").load(path+".orc")
    case "parquet"  => spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .format("parquet").load(path+".parquet")
  }
 df
  }

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("SchemaEvolutionCheckParquet")
      .getOrCreate()

  val df = getDataFrame(spark,"orc","C:\\work\\repo\\data\\files\\5m Sales Records");
    groupByCheck(df)
    val time = spark.time(groupByCheck(df))
  }
}
