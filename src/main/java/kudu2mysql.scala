import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}


import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.functions.lit



/**
 * Created by 长春 on 2018/4/25.
 */
object kudu2mysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("hdfs2kudu")
      .getOrCreate()

    val df2 = spark.sqlContext.read.options(Map("kudu.master" -> "172.26.125.2:7051","kudu.table" -> "ae_profile_carbon_one")).kudu
    val jdbcUrl = s"jdbc:mysql://172.26.125.4:3306/enterprise?user=meta&password=meta2015"

    df2.drop("p_id").withColumn("p_id",lit(1).cast("int")).write.mode(SaveMode.Append).jdbc(jdbcUrl,"ae_profile_mysql_five",new Properties)
    //spark.read.format("jdbc").options()
    //spark.read.jdbc(jdbcUrl,"ae_profile_mysql_two",new Properties)




  }

}
