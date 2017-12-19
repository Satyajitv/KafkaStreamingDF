package org.spark.jsonDF

import debezium.Debezium
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.StringType

/**
  * Created by svegesna on 12/12/17.
  */
object StructStreaming {

  def DebeziumKafkaInstance(spark: SparkSession, options: scala.collection.mutable.Map[String, String]): DataFrame = {
    val optionsUpdte=options.filterKeys(x => !x.equals("maxOffsetsPerTrigger"))

    spark.readStream.format("kafka").options(optionsUpdte)
        .option("maxOffsetsPerTrigger",options.get("maxOffsetsPerTrigger").get.toLong)
      .load().transform(Debezium.getJsonValue)
  }

  def createRowDF(spark: SparkSession, options: scala.collection.mutable.Map[String, String]):DataFrame  ={
    val fieldNames = StructStreamKafkaToDF.fields(StructStreamKafkaToDF.getValueSchema(options,spark).fields(0).dataType.catalogString)
    var fetchValue = DebeziumKafkaInstance(spark,options)
    import spark.implicits._
    for( col <- fieldNames) {
      fetchValue = fetchValue.withColumn(col.toString , functions.get_json_object($"payloadAfterValue".cast(StringType), "$."+col.toString))
    }
    fetchValue.drop("payloadAfterValue")
  }
}
