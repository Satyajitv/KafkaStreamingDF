import org.apache.spark.sql.SparkSession
import org.spark.jsonDF.StructStreaming

/**
  * Created by svegesna on 12/11/17.
  */
object SparkEntry extends App{

  val spark = SparkSession
    .builder().master("local[*]")
    .appName("StreamProtocomCRR")
    .getOrCreate()

  val options = new scala.collection.mutable.HashMap[String, String]

  options("kafka.bootstrap.servers") = "**.**.**:9092"
  options("subscribe") = "***************"
  options("maxOffsetsPerTrigger") = "10"
  options("startingOffsets") = "earliest"
  options("checkpointLocation") = "f**********************"

  StructStreaming.createRowDF(spark,options).writeStream.format("console").start().awaitTermination()

}
