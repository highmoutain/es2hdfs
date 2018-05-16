/**
 * Created by 长春 on 2018/5/10.
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object createIndex {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val initOffset = args(1).toInt + 1
    val spark = SparkSession
      .builder()
      .appName("createIndex")
      .getOrCreate()

    val df = spark.read.parquet(path)
    val rdd2 = df.rdd.zipWithUniqueId().map(x=>(x._2+initOffset,x._1))

//    val schema = StructType(List(
//      StructField("integer_column", IntegerType, nullable = false),
//      StructField("string_column", StringType, nullable = true),
//      StructField("date_column", DateType, nullable = true)
//    ))


  }

}
