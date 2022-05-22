package com.versh.schema
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType,IntegerType}

object NestedSchemaCheck extends App {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("NestedSchemaCheck")
    .getOrCreate()

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
  df.printSchema()
  df.show(false)
  // this will give error , csv does not support nested
  //df.write.format("csv").mode(SaveMode.Overwrite).save("C:\\out")

  df.write.format("orc").mode(SaveMode.Overwrite)
    .save("C:\\out\\orc")

  df.write.format("avro").mode(SaveMode.Overwrite)
    .save("C:\\out\\avro")

  df.write.format("json").mode(SaveMode.Overwrite)
    .save("C:\\out\\json")

  df.write.format("parquet").mode(SaveMode.Overwrite)
    .save("C:\\out\\parquet")
}
