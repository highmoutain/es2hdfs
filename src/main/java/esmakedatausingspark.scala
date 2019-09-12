/**
 * Created by 长春 on 2018/3/29.
 */

import java.io.File

import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

import org.elasticsearch.spark.sql._
object esmakedatausingspark {
  def main(args: Array[String]): Unit = {
    //执行命令方式：spark-submit  --class esmakedatausingspark --num-executors 1 --driver-cores 2 --driver-memory 2G
    // --executor-cores 2 --executor-memory 5G ~/es2hdfs.jar 1 "ae_event_es_608_201906" 2000 2 "ae_event_es_608_201905/analytics"
    val starttime_day = args(0)
    val index = args(1)

    val scrollsize = args(2)
    val offset = args(3)
    val newIndexName = args(4)
//    val spark = SparkSession
//      .builder()
//      .appName("es2hdfs")
//      .getOrCreate()
    val warehouse = new File("./warehouse").getCanonicalPath
    val metastore = new File("./metastore").getCanonicalPath
    val spark = SparkSession
      .builder()
      .appName("StreamExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(warehouse, metastore)

    //val sqlContext = new SQLContext(sc)
    val esOptions = Map("es.nodes"->"172.23.4.156,172.23.4.157,172.23.4.158", "es.scroll.size"->scrollsize,
      "es.input.max.docs.per.partition"->"200000","es.nodes.wan.only"->"true")
    val e = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(index)
    //e.saveToEs("spark/people")
    e.createOrReplaceTempView("temp")
    val newDF = spark.sql("select _td_current_appversion, _td_current_city,_td_current_country," +
      "_td_current_network,_td_current_operator,_td_current_province+"+ offset +" as _td_current_province,_td_event_count,_td_event_duration," +
      "_td_event_label,_td_miniprogram_type,_td_qrcode_name,_td_reffer,_td_scene,_td_sdk_source,_td_utm_all,_td_utm_campaign,_td_utm_content," +
      "_td_utm_medium,_td_utm_source,_td_utm_term,accountoffset,distinctid,event,eventid,eventtype,isvistor," +
      "offset,ownerid,ownertype, productid,starttime,"+starttime_day+" as starttime_day,tenantid,updatetime from temp")
    //spark.conf.set("es.nodes", "172.23.4.156")
    val destesOptions = Map("es.nodes"->"172.23.4.156,172.23.4.157,172.23.4.158")
    newDF.write.format("org.elasticsearch.spark.sql").options(destesOptions).mode("append").save(newIndexName)
    //newDF.saveToEs(newIndexName)
    //newDF.saveToEs(newIndexName)
    //newDF.rdd.save
    //newDF.write.parquet("/test/ok2")
    //newDF.write.format()







    //e.write.parquet(path)
    //e.createOrReplaceTempView("people")
    //val aDF = spark.sql("select cur_appversioncode, cur_appversionname from people")
    //aDF.write.parquet("/test/ok2")


  }

}
