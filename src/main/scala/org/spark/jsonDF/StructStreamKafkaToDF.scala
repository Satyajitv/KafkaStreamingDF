package org.spark.jsonDF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by svegesna on 12/11/17.
  */
object StructStreamKafkaToDF {

  //Retrieves the schema of the kafka value column.
  def getValueSchema(options: scala.collection.mutable.Map[String, String], spark: SparkSession): StructType = {
    import spark.implicits._

    val Records = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", options.get("kafka.bootstrap.servers").get.toString)
      .option("subscribe", options.get("subscribe").get.toString)
      .option("maxOffsetsPerTrigger", options.get("maxOffsetsPerTrigger").get.toString).load()
      .select($"value".cast("string")).limit(20).as[String]

    val json = spark.read.json(Records).select($"payload.after")

    val schema = json.schema

    json.unpersist()

    schema

  }


  def fields(struct: String): Array[String] = {
    val fieldsAndTypes: Array[String] = struct.replace("struct<", "").replace(">", "").split(",")
    val data: Array[String] = fieldsAndTypes.map(x => x.split(":")(0))
    data
  }
}
