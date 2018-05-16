/**
 * Created by 长春 on 2018/4/27.
 */

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession, CarbonEnv}
import org.apache.spark.sql.CarbonSession._
import org.apache.kudu.spark.kudu._
import org.apache.carbondata.core.util.path.CarbonStorePath
import spark.jobserver.SparkSessionJob

object kudu2carbon2 extends SparkSessionJob {
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

}
