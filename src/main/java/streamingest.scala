/**
 * Created by 长春 on 2018/4/10.
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.streaming.parser._
object streamingest {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder
//      .appName("streamingest")
//      .getOrCreate()
    val tableName = args(0)
    val pqtpath = args(1)
    val warehouse = new File("./warehouse").getCanonicalPath
    val metastore = new File("./metastore").getCanonicalPath
    val spark = SparkSession
      .builder()
      .appName("StreamExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(warehouse, metastore)

    // drop table if exists previously
    //spark.sql(s"DROP TABLE IF EXISTS carbon_table")
    // Create target carbon table and populate with initial data
    /*
    spark.sql(
      s"""
         | CREATE TABLE carbon_table (
         | col1 INT,
         | col2 STRING
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('streaming'='true')""".stripMargin)
*/
    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), tableName)(spark)
    val tablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)

    var qry: StreamingQuery = null
    val userSchema = spark.read.parquet(pqtpath).schema
    val readSocketDF = spark.readStream.schema(userSchema).parquet(pqtpath)

    // Write data from socket stream to carbondata file
    qry = readSocketDF.writeStream
      .format("carbondata")
      .trigger(ProcessingTime("20 seconds"))
      .option("checkpointLocation", tablePath.getStreamingCheckpointDir)
      .option("dbName", "default")
      .option("tableName", tableName)
      .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_ROW_PARSER)
      .outputMode("append")
      .start()

    // start new thread to show data
//      new Thread() {
//        override def run(): Unit = {
//          do {
//            spark.sql("select * from carbon_table").show(false)
//            Thread.sleep(10000)
//          } while (true)
//        }
//      }.start()

    qry.awaitTermination()

  }
}
