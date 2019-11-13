
import java.io.File

import org.apache.spark.sql.{CarbonSession, SaveMode, SparkSession}

object mysql2Carbon {
  //spark-submit --class mysql2Carbon  /home/hadoop/es2hdfs-1.0-SNAPSHOT.jar $url $user $password $tableName $carbonTableName
  //example:spark-submit --class mysql2Carbon  /home/hadoop/es2hdfs-1.0-SNAPSHOT.jar
  //jdbc:mysql://172.23.6.141:3306/enterprise  meta meta2015 product product_mysql 60

  def main(args: Array[String]): Unit = {
    val url = args(0)
    val user = args(1)
    val password = args(2)
    val tableName = args(3)
    val carbonTableName = args(4)
    val repartition = args(5)
    println("---mysqlTableName: " + s"| $tableName |" )
    println("---carbonTableName: " + s"| $carbonTableName |" )
    println("--repartition:" + s"|$repartition|")


    val warehouse = new File("./warehouse").getCanonicalPath
    val metastore = new File("./metastore").getCanonicalPath
    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .appName("a")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(warehouse, metastore)


    val jdbcDF = spark.read.format("jdbc")
      .option("delimiter", ",")
      .option("heard", true)
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", user).option("password", password).load()


    jdbcDF.repartition(repartition.toInt).write
      .format("carbondata")
      .option("tableName", carbonTableName)
      //.option("partitionColumns", "c1")  // a list of column names
      .mode(SaveMode.Overwrite)
      .save()

  }
}


