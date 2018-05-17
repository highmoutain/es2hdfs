import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu._
import collection.JavaConversions._
import org.apache.kudu.client._

/**
 * Created by 长春 on 2018/4/25.
 */
object hdfs2kudu {
  def main(args: Array[String]): Unit = {
    //val pqtpath = args(0)
    val hashPartition = args(0)
    val spark = SparkSession
      .builder()
      .appName("hdfs2kudu")
      .getOrCreate()

    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051","kudu.table" -> "test_table8",
      "kudu.faultTolerantScan" -> "true")).kudu
    val kuduContext = new KuduContext("172.20.3.1:7051", spark.sqlContext.sparkContext)
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.addHashPartitions(List("productid","sourceid","deviceproductoffset"),hashPartition.toInt).setNumReplicas(1)
    //kuduTableOptions.setRangePartitionColumns(List("productid","sourceid","deviceproductoffset")).setNumReplicas(1)
    kuduContext.createTable("test_table9", df2.schema, Seq("productid","sourceid","deviceproductoffset"), kuduTableOptions)
    kuduContext.insertRows(df2, "test_table9")
    kuduContext.deleteTable("test_table9")
    //val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051","kudu.table" -> "test_table7")).kudu

    //val kuduContext = new KuduContext("172.20.3.1:7051", spark.sqlContext.sparkContext)
    /*
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.setRangePartitionColumns(List("productid","sourceid","deviceproductoffset")).setNumReplicas(1)
    val df = spark.read.parquet("/warehouse/spark/profile_pqt/part-00017-14343208-c05c-492c-aa4f-843ee51b029a-c000.snappy.parquet")
    kuduContext.createTable("test_table3", df.schema, Seq("productid","sourceid","deviceproductoffset"), kuduTableOptions)
    kuduContext.insertRows(df2, "test_table8")
    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051","kudu.table" -> "test_table7")).kudu
*/
//fuzhibiao
    /*
    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051","kudu.table" -> "test_table7")).kudu
    val kuduContext = new KuduContext("172.20.3.1:7051", spark.sqlContext.sparkContext)
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.setRangePartitionColumns(List("productid","sourceid","deviceproductoffset")).setNumReplicas(1)
    kuduContext.createTable("test_table8", df2.schema, Seq("productid","sourceid","deviceproductoffset"), kuduTableOptions)
    kuduContext.insertRows(df2, "test_table8")
    kuduContext.deleteTable("test_table8")
     */

  }

}