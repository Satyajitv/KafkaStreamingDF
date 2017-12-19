package org.spark.jsonDF

/**
  * Created by svegesna on 12/12/17.
  */
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

object FindKeysJson { // add tests for result
  implicit val formats = DefaultFormats

  def getJsonKeys(jsonString:String):Map[String, String] =  {
    parse(jsonString).mapField(k => {
      val v: String = k._2 match {
        case s: JString => k._2.extract[String]
        case _ => write(k._2)
      }
      (k._1, JString(v))
    }).extract[Map[String, String]]
  }
  //val data = "{\"id\":24013211933,\"facility_id\":1755,\"group_id\":192023,\"station_id\":169565,\"inmate_id\":0,\"destination_id\":0,\"DestinationANI\":null,\"CallRecordWav\":null,\"CallTimeLimit\":60,\"CallDollarLimit\":null,\"EndTime\":\"2017-12-07 03:33:32\",\"EndCompCode\":0,\"EndTermCode\":50,\"duration\":30,\"rate\":0,\"price\":0.0,\"Alarmed\":0,\"alarm_id\":0,\"ContactCode\":\"SIP/openser5-00076e68~1512617581.541035\",\"CdrSignature\":null,\"CreationDate\":\"2017-12-07 03:33:02\",\"CreationUser\":null,\"UpdateDate\":null,\"prepaid_code_id\":0,\"prepaid\":0,\"bill_seconds\":0,\"active_call_id\":1603272255,\"free_call\":0,\"lang\":null,\"macro\":\"tdNCICcoll\",\"accept_time\":null,\"ast_host\":23,\"threeway_rating\":0,\"threeway_risk\":0,\"threeway_dtmf\":null,\"threeway_silence_length\":null,\"threeway_silence_start\":null,\"threeway_ringing_start\":null,\"vv_score\":0,\"amd_status\":null,\"amd_cause\":null,\"call_type\":0,\"sub_progress\":0,\"merchant_tx\":null,\"destination_type\":0,\"req_data\":null,\"tax\":0.0,\"bill_price\":0.0,\"merchant_id\":0,\"carrier_uri\":null,\"data_year\":2017,\"data_quarter\":4}"
  //print(getJsonKeys(data))

}
