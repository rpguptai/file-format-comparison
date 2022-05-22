package com.versh.schema

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SchemaEvolutionCheckOrc extends App {

  val spark: SparkSession = SparkSession.builder()
  .config("spark.master", "local")
  .appName("SchemaEvolutionCheckOrc")
  .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR");
  val structureData = Seq(
  Row("36636","Finance",Row(3000,"USA")),
  Row("40288","Finance",Row(5000,"IND")),
  Row("42114","Sales",Row(3900,"USA")),
  Row("39192","Marketing",Row(2500,"CAN")),
  Row("34534","Sales",Row(6500,"USA"))
  )

  val structureSchema = new StructType()
  .add("id",StringType)
  .add("dept",StringType)
  .add("properties",new StructType()
  .add("salary",IntegerType)
  .add("location",StringType)
  )

  var df = spark.createDataFrame(
  spark.sparkContext.parallelize(structureData),structureSchema)

  df.show()
  df.printSchema()

  df.write.format("orc").mode(SaveMode.Overwrite)
  .save("c:\\out\\orc\\partition-date=2020-01-02")

  val structureData1 = Seq(
  Row("36636",Row(3000,"USA","345")),
  Row("40288",Row(5000,"IND","123")),
  Row("42114",Row(3900,"USA","0990")),
  Row("39192",Row(2500,"CAN","7687")),
  Row("34534",Row(6500,"USA","8788"))
  )

  val structureSchema1 = new StructType()
  .add("id",StringType)
  .add("properties",new StructType()
  .add("salary",IntegerType)
  .add("location",StringType)
  .add("pin",StringType)
  )

  var df1 = spark.createDataFrame(
  spark.sparkContext.parallelize(structureData1),structureSchema1)
  df1.show()
  df1.printSchema()

  df1.write.format("orc").mode(SaveMode.Overwrite)
  .save("c:\\out\\orc\\partition-date=2020-01-03")


  val df2 = spark.read.options(Map("mergeSchema"->"true")).orc("c:\\out\\orc\\")
  df2.printSchema()
  df2.show()
}

