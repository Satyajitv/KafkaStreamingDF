Structured Streaming(Kafka) JSON to DF (Specifically for Debezium payload.after Data)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The same should also work with other JSON formats apart from Debezium, with minute changes.

Approach:
+++++++++


*InferSchema from kafka batch process version.
*Use the schema to create JSON row values to Streaming DF with columns and their values.

Steps to create DF from structured streaming JSON values:
1.Create an object like SparkEntry.
2.Create SparkSession instance.
3.Create a Map with the options that are required to connect to Kafka, example

          val options = new scala.collection.mutable.HashMap[String, String]
          options("kafka.bootstrap.servers") = "**.**.**.**:9092"
          options("subscribe") = "***************"
          options("maxOffsetsPerTrigger") = "10"
          options("startingOffsets") = "earliest"
          options("checkpointLocation") = "*****************************"

4.Pass SparkSession and options to StructStreaming.createRowDF(spark,options) method,which 
  return's DataFrame with all fields in JSON as Columns in DF.
  
   StructStreaming.createRowDF(spark,options).writeStream.format("console").start().awaitTermination()
