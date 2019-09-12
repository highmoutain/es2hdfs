import java.io.File
import java.sql.{ResultSet, Statement, DriverManager}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by 长春 on 2018/7/26.
 */
object carbonjdbc {
  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  //private var logger: Logger = LoggerFactory.getLogger(classOf[carbonjdbc])


  def main(args: Array[String]) {
    var resultBrand = 0
    var resultAccountid = ""
    var resultPersoncity = 0
    try {
      Class.forName(driverName)
    }
    catch {
      case classNotFoundException: ClassNotFoundException =>
        classNotFoundException.printStackTrace()
    }
    val connection = DriverManager.getConnection(s"jdbc:hive2://172.20.40.18:10000/default", "", "")
    val statement: Statement = connection.createStatement
    println(s"============HIVE CLI IS STARTED ON PORT 10000 ==============")

    val sql = "SELECT * FROM ae_profile_es_543 limit 10"

    val resultSet: ResultSet = statement.executeQuery(sql)

    var rowsFetched = 0

    while (resultSet.next) {
      if (rowsFetched == 0) {
        println("+---+" + "+-------+" + "+--------------+")
        println("| _td_brand|" + "| accountid |" + "| personcity        |")

        println("+---+" + "+-------+" + "+--------------+")

        resultBrand = resultSet.getInt("_td_brand")
        resultAccountid = resultSet.getString("accountid")
        resultPersoncity = resultSet.getInt("personcity")

        println(resultBrand + s"| $resultAccountid |" + resultPersoncity)
        println("+---+" + "+-------+" + "+--------------+")
      }
      else {
        resultBrand = resultSet.getInt("_td_brand")
        resultAccountid = resultSet.getString("accountid")
        resultPersoncity = resultSet.getInt("personcity")

        println(resultBrand + s"| $resultAccountid |" + resultPersoncity)
        println("+---+" + "+-------+" + "+--------------+")
      }
      rowsFetched = rowsFetched + 1
    }
    System.exit(0)
  }

}
