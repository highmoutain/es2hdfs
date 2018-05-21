import java.io.File

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.sql.CarbonSession._
import org.apache.kudu.spark.kudu._

/**
 * Created by 长春 on 2018/5/21.
 */
object kudu2carbonSS {
  def main(args: Array[String]):Unit = {
   // val sc = new SparkContext("local[*]", "Constant Input DStream Demo", new SparkConf())
    val conf = new SparkConf().setAppName("App Name")
    val sc = new SparkContext(conf)
    import org.apache.spark.streaming.{ StreamingContext, Seconds }
    val ssc = new StreamingContext(sc, batchDuration = Seconds(60))

    // Create the RDD
    val rdd = sc.parallelize(0 to 9)

    // Create constant input dstream with the RDD
    import org.apache.spark.streaming.dstream.ConstantInputDStream
    val input  = new ConstantInputDStream(ssc, rdd)
    input.foreachRDD( x => {val kuduTableName = "behavior"
      val carbonTableName = "behavior"

      val warehouse = new File("./warehouse").getCanonicalPath
      val metastore = new File("./metastore").getCanonicalPath
      val spark = SparkSession
        .builder()
        .appName("StreamExample")
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreateCarbonSession(warehouse, metastore)

      val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051",
        "kudu.faultTolerantScan" -> "true", "kudu.table" -> kuduTableName)).kudu
      //改为注册为临时表，用sql语句试
      df2.registerTempTable("temp")
      //spark.sql("insert overwrite table profile_carbondata2  select productid , sourceid , deviceproductoffset , age , appversioncode , appversionname , birthday , brandid , browserid , carrierid , channelid , childstatus , childstatusid , cityid , countryid , cur_appversioncode , cur_appversionname , cur_carrierid , cur_channelid , cur_cityid , cur_countryid , cur_ip , cur_networkid , cur_osid , cur_provinceid , deviceid , educational , educationalid , email , firm , firstlogintime , firstvisittime , gender , ip , isaccountlastupdate , islastupdate , lastsessiontime , marriage , marriageid , mobileid , name , networkid , organizationid , osid , personcity , personcityid , personcountry , personcountryid , personprovince , personprovinceid , pixelid , platformid , profession , professionid , provinceid , relatedaccountid , relatedaccountproductoffset , telephone , test_firm  from temp")
      spark.sql("insert overwrite table behavior  select eventid ,_td_current_appversion ,_td_current_city ,_td_current_country  ,_td_current_network  ,_td_current_operator  ,_td_current_province  ,_td_event_count  ,_td_interval_duration  ,event  ,eventName  ,eventType  ,startTime   from temp")
    })
    //val output = new TestOutputStream(input)
    //output.register()
  //  input.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
