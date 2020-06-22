package test
import java.sql
import java.sql.{Connection, DriverManager}

object DBUntils {
  val IP = "127.0.0.1"
  val Port = "3306"
  val DBType = "mysql"
  val DBName = "scala"
  val username = "root"
  val password = "123456"
  val url = "jdbc:" + DBType + "://" + IP + ":" + Port + "/" + DBName
  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed() || conn != null) {
        conn.close()
      }
    }catch {
      case ex:Exception =>{
        ex.printStackTrace()
      }
    }


  }
}
