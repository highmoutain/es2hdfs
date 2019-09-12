/**
 * Created by 长春 on 2018/3/29.
 */

import java.io.File
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

import org.elasticsearch.spark.sql._

import org.apache.spark.sql.SparkSession
object es2hdfs {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val index = args(1)
    val actionOrProfile = args(2)
    val scrollsize = args(3)
    val tableName = args(4)
    val newIndexName = args(5)
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
    val esOptions = Map("es.nodes"->"172.23.4.30,172.23.4.31,172.23.4.32", "es.scroll.size"->scrollsize,"es.input.max.docs.per.partition"->"200000")
    val e = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(index)
    //e.saveToEs("spark/people")


    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://172.23.5.162:3306/enterprise"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"meta")
    connectionProperties.put("password", s"meta2015")
    val employees_table = spark.read.jdbc(jdbcUrl, "DICTIONARY_ITEM", connectionProperties)
    e.createOrReplaceTempView("estable")

    employees_table.createOrReplaceTempView("mysqltable")

    val finaltable = spark.sql("select e._td_brand,m.id from estable e join mysqltable m on e._td_brand = m.id ")

    spark.range(5).cache()

    finaltable.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only","true")
      .option("es.nodes", "172.23.4.30,172.23.4.31,172.23.4.32")
      .mode("Overwrite")
      .save(newIndexName)

    if ( actionOrProfile == "profile" ) {
      /*
      e.write
           .format("carbondata")
           .option("tableName", "action_carbon_20180522_2")
           .option("compress", "true")
           .option("tempCSV", "false")
           .mode(SaveMode.Overwrite)
           .save()
           */
      e.registerTempTable("tempprofile")
      val sql = "insert into " + tableName + " select " +
                   "  _td_brand  ,  _td_browser  ,  _td_channel  ,  _td_mobile  ,  _td_pixel  ,  _td_platform  ,  _td_sdk_source  ,  accountid  ,  accountoffset  ,  accounttype  ,  age  ,  birthday  ,  distinctid  ,  educational  ,  gender  ,  islastaccount  ,  islastdevice  ,  name  ,  offset  ,  ownerid  ,  ownertype  ,  personcity  ,  personcountry  ,  personprovince  ,  productid  ,  profession  ,  tenantid  ,  updatetime " +
                 " from tempprofile"
      println("sql:" + sql)

      spark.sql(sql)
      //e.drop("params").registerTempTable("tempaction")
      //spark.sql("use default; insert into action_carbon_20180522 select cur_appversioncode  ,  cur_appversionname  ,  cur_browserid  ,  cur_carrierid  ,  cur_channelid  ,  cur_cityid  ,  cur_countryid  ,  cur_ip  ,  cur_networkid  ,  cur_osid  ,  cur_provinceid  ,  deviceproductoffset  ,  duration  ,  eventcount  ,   eventlabelid  ,  eventtypeid  ,  organizationid  ,  platformid  ,  productid  ,  relatedaccountproductoffset  ,  sessionduration  ,  sessionid  ,  sessionstarttime  ,  sessionstatus  ,  sourceid  ,  starttime  ,  starttimestr, eventid  from tempaction")
    } else if (actionOrProfile == "action" ) {
      e.registerTempTable("tempaction")
      val sql =  "insert into " + tableName + " select " +
                "  _td_current_appversion , _td_current_city , _td_current_country , _td_current_network , _td_current_operator , _td_current_province , _td_event_count , _td_event_duration , _td_reffer , _td_utm_all , _td_utm_campaign , _td_utm_medium , _td_utm_source , _td_utm_term , accountoffset , distinctid , event , eventtype , isvistor , offset , ownerid , ownertype , productid , starttime , starttime_day , tenantid , updatetime,eventid " +
                " from tempaction "
      println("sql:" + sql)

      spark.sql(sql)

    }
    //e.write.parquet(path)
    //e.createOrReplaceTempView("people")
    //val aDF = spark.sql("select cur_appversioncode, cur_appversionname from people")
    //aDF.write.parquet("/test/ok2")


  }

}
