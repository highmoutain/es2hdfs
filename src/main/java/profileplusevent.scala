/**
 * Created by 长春 on 2018/3/29.
 */

import java.io.File

import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession

object profileplusevent {
  def main(args: Array[String]): Unit = {
    //执行命令方式：spark-submit  --class esmakedatausingspark --num-executors 1 --driver-cores 2 --driver-memory 2G
    // --executor-cores 2 --executor-memory 5G ~/es2hdfs.jar 1 "ae_event_es_608_201906" 2000 2 "ae_event_es_608_201905/analytics"
    val index_p = args(0)
    val index_e= args(1)

    val scrollsize = args(2)
    val newIndexName = args(3)
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
    val e = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(index_e)
    //e.saveToEs("spark/people")
    e.createOrReplaceTempView("event")
    val profile = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(index_p)
    profile.createOrReplaceTempView("profile")
    val newDF = spark.sql("select e._td_current_appversion,e._td_current_city,e._td_current_country," +
      "e._td_current_network,e._td_current_operator,e._td_current_province,e._td_event_count,e._td_event_duration," +
      "e._td_event_label,e._td_miniprogram_type,e._td_qrcode_name,e._td_reffer,e._td_scene,e._td_sdk_source," +
      "e._td_utm_all,e._td_utm_campaign,e._td_utm_content,e._td_utm_medium,e._td_utm_source,e._td_utm_term," +
      "e.accountoffset,e.distinctid,e.event,e.eventid,e.eventtype,e.isvistor,e.offset,e.ownerid,e.ownertype," +
      "e.productid,e.starttime,e.starttime_day,e.tenantid,e.updatetime,p._td_brand,p._td_browser,p._td_channel," +
      "p._td_email,p._td_getui_id,p._td_huawei_id,p._td_ios_id,p._td_jiguang_id,p._td_mobile,p._td_os,p._td_phonenum," +
      "p._td_pixel,p._td_platform,p._td_xiaomi_id,p.accounttype from event e left join profile p" +
      " on e.offset = p.offset")
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
