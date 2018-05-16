/**
 * Created by 长春 on 2018/3/29.
 */
import org.apache.spark.sql.SparkSession
object es2hdfs {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val index = args(1)
    val actionOrProfile = args(2)
    val scrollsize = args(3)
    val spark = SparkSession
      .builder()
      .appName("es2hdfs")
      .getOrCreate()
    //val sqlContext = new SQLContext(sc)
    val esOptions = Map("es.nodes"->"172.20.3.2,172.20.3.4,172.20.40.19", "es.scroll.size"->scrollsize,"es.input.max.docs.per.partition"->"200000")
    val e = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(index)
    e.rdd

    if ( actionOrProfile == "action" ) {
      e.drop("params").write.parquet(path)
    } else {
      e.write.parquet(path)
    }
    //e.write.parquet(path)
    //e.createOrReplaceTempView("people")
    //val aDF = spark.sql("select cur_appversioncode, cur_appversionname from people")
    //aDF.write.parquet("/test/ok2")


  }

}
