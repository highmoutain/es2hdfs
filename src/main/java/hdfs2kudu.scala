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
    val hashPartition2 = args(1)
    val spark = SparkSession
      .builder()
      .appName("hdfs2kudu")
      .getOrCreate()

    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.20.3.1:7051","kudu.table" -> "ae_profile_20e",
      "kudu.faultTolerantScan" -> "true")).kudu
    val kuduContext = new KuduContext("172.20.40.18:7051,172.20.40.20:7051,172.20.40.21:7051", spark.sqlContext.sparkContext)

    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.addHashPartitions(List("productid"), hashPartition.toInt).setNumReplicas(1)
      .addHashPartitions(List("offset"),hashPartition2.toInt)
    //kuduTableOptions.setRangePartitionColumns(List("productid","sourceid","deviceproductoffset")).setNumReplicas(1)
    kuduContext.createTable("ae_profile_30e", df2.schema, Seq("productid", "offset", "accountoffset"), kuduTableOptions)

    //kuduContext.insertRows(df2, "ae_profile_20e")
//    kuduContext.deleteTable("")
//
//    for (i <- 1 to 5) {
//      val df3 = df2.withColumn("productid1",df2("productid")+ i*10)
//      val df4 = df3.drop("productid").withColumnRenamed("productid1","productid")
//      kuduContext.insertRows(df4, "ae_profile_20e")
//    }


    //kuduContext.deleteTable("test_table10")

    /*make data
    val df3 = df2.withColumn("productid1",df2("productid")+1)
val df4 = df3.drop("productid").withColumnRenamed("productid1","productid")


val df5 = df4.withColumn("productid1",df4("productid")+1)
val df6 = df5.drop("productid").withColumnRenamed("productid1","productid")

val df7 = df6.withColumn("productid1",df6("productid")+1)
val df8 = df7.drop("productid").withColumnRenamed("productid1","productid")

val df9 = df8.withColumn("productid1",df8("productid")+1)
val df10 = df9.drop("productid").withColumnRenamed("productid1","productid")
     */

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
