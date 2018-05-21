/**
 * Created by 长春 on 2018/4/27.
 */

import java.io.File

import com.typesafe.config.Config
import org.apache.spark.sql.{SaveMode, SparkSession, CarbonEnv}
import org.apache.spark.sql.CarbonSession._
import org.apache.kudu.spark.kudu._
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.scalactic._
import spark.jobserver.SparkSessionJob
import spark.jobserver.api.{SingleProblem, ValidationProblem, JobEnvironment}

import scala.util.Try

object kudu2carbonWithJobServer extends SparkSessionJob {
  /*
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName("kudu2carbon")
//      .getOrCreate()
    val kuduTableName = args(0)
    val carbonTableName = args(1)
    val warehouse = new File("./warehouse").getCanonicalPath
    val metastore = new File("./metastore").getCanonicalPath
    val spark = SparkSession
      .builder()
      .appName("StreamExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(warehouse, metastore)
    spark.sql("DROP TABLE IF EXISTS " + carbonTableName)

    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051",
      "kudu.faultTolerantScan" -> "true","kudu.table" -> kuduTableName)).kudu

//    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051",
//      "kudu.table" -> kuduTableName)).kudu
    //df2.registerTempTable("kudu_table")
    //spark.sql("insert into table profile_carbondata_fromkudu select * from kudu_table")
    df2.write
      .format("carbondata")
      .option("tableName", carbonTableName)
      .option("compress", "true")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
  }
  */

  type JobData = List[String]
  type JobOutput = collection.Map[String, Long]

  override def runJob(sparkSession: SparkSession, runtime: JobEnvironment, data: JobData): JobOutput = {
    //sparkSession.sparkContext.parallelize(data).countByValue
    val kuduTableName = data(0)
    val carbonTableName = data(1)

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
//    df2.write
//      .format("carbondata")
//      .option("tableName", carbonTableName)
//      .option("compress", "true")
//      .option("tempCSV", "false")
//      .mode(SaveMode.Overwrite)
//      .save()
    Map("result" -> 100000)
  }


  override def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  //JobData = { config.getString("kuduTableName")::config.getString("kuduTableName")::Nil }
  JobData Or Every[ValidationProblem] = {
    Try(List(config.getString("kuduTableName"), config.getString("carbonTableName")))
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("input param error"))))
  }

  //  JobData Or Every[ValidationProblem] = {
  //    Try(config.getString("input.string").split(" ").toSeq)
  //      .map(words => Good(words))
  //      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  //  }


}
