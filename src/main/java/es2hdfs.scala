/**
 * Created by 长春 on 2018/3/29.
 */

import java.io.File
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.SparkSession
object es2hdfs {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val index = args(1)
    val actionOrProfile = args(2)
    val scrollsize = args(3)
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
    val esOptions = Map("es.nodes"->"172.20.3.2,172.20.3.4,172.20.40.19", "es.scroll.size"->scrollsize,"es.input.max.docs.per.partition"->"200000")
    val e = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(index)


    if ( actionOrProfile == "action" ) {
      e.drop("params").write
           .format("carbondata")
           .option("tableName", "action_carbon_20180522_2")
           .option("compress", "true")
           .option("tempCSV", "false")
           .mode(SaveMode.Overwrite)
           .save()
      //e.drop("params").registerTempTable("tempaction")
      //spark.sql("use default; insert into action_carbon_20180522 select cur_appversioncode  ,  cur_appversionname  ,  cur_browserid  ,  cur_carrierid  ,  cur_channelid  ,  cur_cityid  ,  cur_countryid  ,  cur_ip  ,  cur_networkid  ,  cur_osid  ,  cur_provinceid  ,  deviceproductoffset  ,  duration  ,  eventcount  ,   eventlabelid  ,  eventtypeid  ,  organizationid  ,  platformid  ,  productid  ,  relatedaccountproductoffset  ,  sessionduration  ,  sessionid  ,  sessionstarttime  ,  sessionstatus  ,  sourceid  ,  starttime  ,  starttimestr, eventid  from tempaction")
    } else {
      e.write.parquet(path)
    }
    //e.write.parquet(path)
    //e.createOrReplaceTempView("people")
    //val aDF = spark.sql("select cur_appversioncode, cur_appversionname from people")
    //aDF.write.parquet("/test/ok2")


  }

}
