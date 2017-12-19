package debezium

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{split, substring_index}

/**
  * Created by svegesna on 12/12/17.
  */
object Debezium {
  // This method is going to fetch the payload.after(actual row) json string from the value column.
  def getJsonValue(df:DataFrame):DataFrame= {
    df.selectExpr("value").withColumn("payloadAfterValue", split(substring_index(df("value"), "\"after\":", -1), ",\"source\"").getItem(0)).drop("value")
  }
}
